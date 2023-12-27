/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.core.instance;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.db.Db;
import org.apache.inlong.agent.db.InstanceDb;
import org.apache.inlong.agent.db.TaskProfileDb;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Instance;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DateTransUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.enums.InstanceStateEnum;
import org.apache.inlong.common.enums.TaskStateEnum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * handle the instance created by task, including add, delete, update etc.
 * the instance info is store in both db and memory.
 */
public class InstanceManager extends AbstractDaemon {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceManager.class);
    private static final int ACTION_QUEUE_CAPACITY = 100;
    public static final int CLEAN_INSTANCE_ONCE_LIMIT = 10;
    public volatile int CORE_THREAD_SLEEP_TIME_MS = 1000;
    public static final int INSTANCE_DB_CLEAN_INTERVAL_MS = 10000;
    private long lastCleanTime = 0;
    public static final String DB_INSTANCE_EXPIRE_CYCLE_COUNT = "3";
    // instance in db
    private final InstanceDb instanceDb;
    private TaskProfileDb taskProfileDb;
    private TaskProfile taskFromDb;
    // task in memory
    private final ConcurrentHashMap<String, Instance> instanceMap;
    // instance profile queue.
    private final BlockingQueue<InstanceAction> actionQueue;
    // task thread pool;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("instance-manager"));

    private final int instanceLimit;
    private final AgentConfiguration agentConf;
    private final String taskId;
    private volatile boolean runAtLeastOneTime = false;
    private volatile boolean running = false;
    private final double reserveCoefficient = 0.8;

    private class InstancePrintStat {

        public int defaultCount = 0;
        public int finishedCount = 0;
        public int deleteCount = 0;
        public int otherCount = 0;

        private void stat(InstanceStateEnum state) {
            switch (state) {
                case DEFAULT: {
                    defaultCount++;
                    break;
                }
                case FINISHED: {
                    finishedCount++;
                    break;
                }
                case DELETE: {
                    deleteCount++;
                }
                default: {
                    otherCount++;
                }
            }
        }

        @Override
        public String toString() {
            return String.format("default %d finished %d delete %d other %d", defaultCount, finishedCount,
                    deleteCount, otherCount);
        }
    }

    /**
     * Init task manager.
     */
    public InstanceManager(String taskId, int instanceLimit, Db basicDb, TaskProfileDb taskProfileDb) {
        this.taskId = taskId;
        instanceDb = new InstanceDb(basicDb);
        this.taskProfileDb = taskProfileDb;
        this.agentConf = AgentConfiguration.getAgentConf();
        instanceMap = new ConcurrentHashMap<>();
        this.instanceLimit = instanceLimit;
        actionQueue = new LinkedBlockingQueue<>(ACTION_QUEUE_CAPACITY);
    }

    public String getTaskId() {
        return taskId;
    }

    public Instance getInstance(String instanceId) {
        return instanceMap.get(instanceId);
    }

    public InstanceProfile getInstanceProfile(String instanceId) {
        return instanceDb.getInstance(taskId, instanceId);
    }

    public boolean submitAction(InstanceAction action) {
        if (action == null) {
            return false;
        }
        return actionQueue.offer(action);
    }

    /**
     * thread for core thread.
     *
     * @return runnable profile.
     */
    private Runnable coreThread() {
        return () -> {
            Thread.currentThread().setName("instance-manager-core-" + taskId);
            running = true;
            while (isRunnable()) {
                try {
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME_MS);
                    cleanDbInstance();
                    dealWithActionQueue(actionQueue);
                    keepPaceWithDb();
                    String inlongGroupId = taskFromDb.getInlongGroupId();
                    String inlongStreamId = taskFromDb.getInlongStreamId();
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_INSTANCE_MGR_HEARTBEAT, inlongGroupId, inlongStreamId,
                            AgentUtils.getCurrentTime(), 1, 1);
                } catch (Throwable ex) {
                    LOGGER.error("coreThread {}", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
                runAtLeastOneTime = true;
            }
            running = false;
        };
    }

    private void cleanDbInstance() {
        if (AgentUtils.getCurrentTime() - lastCleanTime > INSTANCE_DB_CLEAN_INTERVAL_MS) {
            List<InstanceProfile> instances = instanceDb.getInstances(taskId);
            doCleanDbInstance(instances);
            InstancePrintStat stat = new InstancePrintStat();
            for (int i = 0; i < instances.size(); i++) {
                InstanceProfile instance = instances.get(i);
                stat.stat(instance.getState());
            }
            LOGGER.info(
                    "instanceManager running! taskId {} mem {} db total {} {} action count {}",
                    taskId, instanceMap.size(), instances.size(), stat, actionQueue.size());
            lastCleanTime = AgentUtils.getCurrentTime();
        }
    }

    private void doCleanDbInstance(List<InstanceProfile> instances) {
        AtomicInteger cleanCount = new AtomicInteger();
        Iterator<InstanceProfile> iterator = instances.iterator();
        while (iterator.hasNext()) {
            if (cleanCount.get() > CLEAN_INSTANCE_ONCE_LIMIT) {
                return;
            }
            InstanceProfile instanceFromDb = iterator.next();
            if (instanceFromDb.getState() != InstanceStateEnum.FINISHED) {
                return;
            }
            TaskProfile taskFromDb = taskProfileDb.getTask(taskId);
            if (taskFromDb != null) {
                if (taskFromDb.getCycleUnit().compareToIgnoreCase(CycleUnitType.REAL_TIME) == 0) {
                    return;
                }
                if (taskFromDb.isRetry()) {
                    if (taskFromDb.getState() != TaskStateEnum.RETRY_FINISH) {
                        return;
                    }
                } else {
                    if (instanceFromDb.getState() != InstanceStateEnum.FINISHED) {
                        return;
                    }
                }
            }
            long expireTime = DateTransUtils.calcOffset(DB_INSTANCE_EXPIRE_CYCLE_COUNT + taskFromDb.getCycleUnit());
            if (AgentUtils.getCurrentTime() - instanceFromDb.getModifyTime() > expireTime) {
                cleanCount.getAndIncrement();
                LOGGER.info("instance has expired, delete from db dataTime {} taskId {} instanceId {}",
                        instanceFromDb.getSourceDataTime(), instanceFromDb.getTaskId(),
                        instanceFromDb.getInstanceId());
                deleteFromDb(instanceFromDb.getInstanceId());
                iterator.remove();
            }
        }
    }

    private void keepPaceWithDb() {
        traverseDbTasksToMemory();
        traverseMemoryTasksToDb();
    }

    private void traverseDbTasksToMemory() {
        instanceDb.getInstances(taskId).forEach((profileFromDb) -> {
            InstanceStateEnum dbState = profileFromDb.getState();
            Instance instance = instanceMap.get(profileFromDb.getInstanceId());
            switch (dbState) {
                case DEFAULT: {
                    if (instance == null) {
                        LOGGER.info("traverseDbTasksToMemory add instance to mem taskId {} instanceId {}",
                                profileFromDb.getTaskId(), profileFromDb.getInstanceId());
                        addToMemory(profileFromDb);
                    }
                    break;
                }
                case FINISHED:
                    DELETE: {
                        if (instance != null) {
                            LOGGER.info("traverseDbTasksToMemory delete instance from mem taskId {} instanceId {}",
                                    profileFromDb.getTaskId(), profileFromDb.getInstanceId());
                            deleteFromMemory(profileFromDb.getInstanceId());
                        }
                        break;
                    }
                default: {
                    LOGGER.error("instance invalid state {} taskId {} instanceId {}", dbState,
                            profileFromDb.getTaskId(),
                            profileFromDb.getInstanceId());
                }
            }
        });
    }

    private void traverseMemoryTasksToDb() {
        instanceMap.values().forEach((instance) -> {
            InstanceProfile profileFromDb = instanceDb.getInstance(instance.getTaskId(), instance.getInstanceId());
            if (profileFromDb == null) {
                deleteFromMemory(instance.getInstanceId());
                return;
            }
            InstanceStateEnum stateFromDb = profileFromDb.getState();
            if (stateFromDb != InstanceStateEnum.DEFAULT) {
                deleteFromMemory(instance.getInstanceId());
            }
        });
    }

    private void dealWithActionQueue(BlockingQueue<InstanceAction> queue) {
        while (isRunnable()) {
            try {
                InstanceAction action = queue.poll();
                if (action == null) {
                    break;
                }
                switch (action.getActionType()) {
                    case ADD:
                        addInstance(action.getProfile());
                        break;
                    case FINISH:
                        finishInstance(action.getProfile());
                        break;
                    case DELETE:
                        deleteInstance(action.getProfile().getInstanceId());
                        break;
                    default:
                        LOGGER.error("invalid action type for instance manager: taskId {} type {}", taskId,
                                action.getActionType());
                }
            } catch (Throwable ex) {
                LOGGER.error("dealWithActionQueue", ex);
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
            }
        }
    }

    @Override
    public void start() {
        restoreFromDb();
        submitWorker(coreThread());
    }

    @Override
    public void stop() {
        waitForTerminate();
        stopAllInstances();
    }

    public void waitForTerminate() {
        super.waitForTerminate();
        while (running) {
            AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME_MS);
        }
    }

    private void restoreFromDb() {
        taskFromDb = taskProfileDb.getTask(taskId);
        List<InstanceProfile> profileList = instanceDb.getInstances(taskId);
        profileList.forEach((profile) -> {
            InstanceStateEnum state = profile.getState();
            if (state == InstanceStateEnum.DEFAULT) {
                LOGGER.info("instance restoreFromDb addToMem state {} taskId {} instanceId {}", state, taskId,
                        profile.getInstanceId());
                addToMemory(profile);
            } else {
                LOGGER.info("instance restoreFromDb ignore state {} taskId {} instanceId {}", state, taskId,
                        profile.getInstanceId());
            }
        });
    }

    private void addInstance(InstanceProfile profile) {
        if (instanceMap.size() >= instanceLimit) {
            LOGGER.error("instanceMap size {} over limit {}", instanceMap.size(), instanceLimit);
            return;
        }
        LOGGER.info("add instance taskId {} instanceId {}", taskId, profile.getInstanceId());
        if (!shouldAddAgain(profile.getInstanceId(), profile.getFileUpdateTime())) {
            LOGGER.info("addInstance shouldAddAgain returns false skip taskId {} instanceId {}", taskId,
                    profile.getInstanceId());
            return;
        }
        addToDb(profile, true);
        addToMemory(profile);
    }

    private void finishInstance(InstanceProfile profile) {
        profile.setState(InstanceStateEnum.FINISHED);
        profile.setModifyTime(AgentUtils.getCurrentTime());
        addToDb(profile, false);
        deleteFromMemory(profile.getInstanceId());
        LOGGER.info("finished instance state {} taskId {} instanceId {}", profile.getState(),
                profile.getTaskId(), profile.getInstanceId());
    }

    private void deleteInstance(String instanceId) {
        deleteFromDb(instanceId);
        deleteFromMemory(instanceId);
    }

    private void deleteFromDb(String instanceId) {
        InstanceProfile profile = instanceDb.getInstance(taskId, instanceId);
        String inlongGroupId = profile.getInlongGroupId();
        String inlongStreamId = profile.getInlongStreamId();
        instanceDb.deleteInstance(taskId, instanceId);
        LOGGER.info("delete instance from db: taskId {} instanceId {} result {}", taskId,
                instanceId, instanceDb.getInstance(taskId, instanceId));
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_DB, inlongGroupId, inlongStreamId,
                profile.getSinkDataTime(), 1, 1);
    }

    private void deleteFromMemory(String instanceId) {
        Instance instance = instanceMap.get(instanceId);
        if (instance == null) {
            LOGGER.error("try to delete instance from memory but not found: taskId {} instanceId {}", taskId,
                    instanceId);
            return;
        }
        String inlongGroupId = instance.getProfile().getInlongGroupId();
        String inlongStreamId = instance.getProfile().getInlongStreamId();
        instance.destroy();
        instanceMap.remove(instanceId);
        LOGGER.info("delete instance from memory: taskId {} instanceId {}", taskId, instance.getInstanceId());
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_MEM, inlongGroupId, inlongStreamId,
                instance.getProfile().getSinkDataTime(), 1, 1);
    }

    private void addToDb(InstanceProfile profile, boolean addNew) {
        LOGGER.info("add instance to db state {} instanceId {}", profile.getState(), profile.getInstanceId());
        instanceDb.storeInstance(profile);
        if (addNew) {
            String inlongGroupId = profile.getInlongGroupId();
            String inlongStreamId = profile.getInlongStreamId();
            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_DB, inlongGroupId, inlongStreamId,
                    profile.getSinkDataTime(), 1, 1);
        }
    }

    /**
     * add instance to memory, if there is a record refer to the instance id exist we need to destroy it first.
     */
    private void addToMemory(InstanceProfile instanceProfile) {
        String inlongGroupId = instanceProfile.getInlongGroupId();
        String inlongStreamId = instanceProfile.getInlongStreamId();
        Instance oldInstance = instanceMap.get(instanceProfile.getInstanceId());
        if (oldInstance != null) {
            oldInstance.destroy();
            instanceMap.remove(instanceProfile.getInstanceId());
            LOGGER.error("old instance {} should not exist, try stop it first",
                    instanceProfile.getInstanceId());
            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_DEL_INSTANCE_MEM_UNUSUAL, inlongGroupId, inlongStreamId,
                    instanceProfile.getSinkDataTime(), 1, 1);
        }
        LOGGER.info("instanceProfile {}", instanceProfile.toJsonStr());
        try {
            Class<?> taskClass = Class.forName(instanceProfile.getInstanceClass());
            Instance instance = (Instance) taskClass.newInstance();
            boolean initSuc = instance.init(this, instanceProfile);
            if (initSuc) {
                instanceMap.put(instanceProfile.getInstanceId(), instance);
                EXECUTOR_SERVICE.submit(instance);
                LOGGER.info(
                        "add instance to memory instanceId {} instanceMap size {}, runningPool instance total {}, runningPool instance active {}",
                        instance.getInstanceId(), instanceMap.size(), EXECUTOR_SERVICE.getTaskCount(),
                        EXECUTOR_SERVICE.getActiveCount());
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_MEM, inlongGroupId, inlongStreamId,
                        instanceProfile.getSinkDataTime(), 1, 1);
            } else {
                LOGGER.error(
                        "add instance to memory init failed instanceId {}", instance.getInstanceId());
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_ADD_INSTANCE_MEM_FAILED, inlongGroupId, inlongStreamId,
                        instanceProfile.getSinkDataTime(), 1, 1);
            }
        } catch (Throwable t) {
            LOGGER.error("add instance error {}", t.getMessage());
        }
    }

    private void stopAllInstances() {
        instanceMap.values().forEach((instance) -> {
            deleteFromMemory(instance.getInstanceId());
        });
        instanceMap.clear();
    }

    public boolean shouldAddAgain(String fileName, long lastModifyTime) {
        InstanceProfile profileFromDb = instanceDb.getInstance(taskId, fileName);
        if (profileFromDb == null) {
            LOGGER.debug("not in db should add {}", fileName);
            return true;
        } else {
            InstanceStateEnum state = profileFromDb.getState();
            if (state == InstanceStateEnum.FINISHED && lastModifyTime > profileFromDb.getModifyTime()) {
                LOGGER.debug("finished but file update again {}", fileName);
                return true;
            }
            if (state == InstanceStateEnum.DELETE) {
                LOGGER.debug("delete and add again {}", fileName);
                return true;
            }
            return false;
        }
    }

    public boolean isFull() {
        return (instanceMap.size() + actionQueue.size()) >= instanceLimit * reserveCoefficient;
    }

    public boolean allInstanceFinished() {
        if (!runAtLeastOneTime) {
            return false;
        }
        if (!instanceMap.isEmpty()) {
            return false;
        }
        if (!actionQueue.isEmpty()) {
            return false;
        }
        List<InstanceProfile> instances = instanceDb.getInstances(taskId);
        for (int i = 0; i < instances.size(); i++) {
            InstanceProfile profile = instances.get(i);
            if (profile.getState() != InstanceStateEnum.FINISHED) {
                return false;
            }
        }
        return true;
    }
}