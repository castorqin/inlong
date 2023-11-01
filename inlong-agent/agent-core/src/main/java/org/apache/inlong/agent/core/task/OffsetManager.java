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

package org.apache.inlong.agent.core.task;

import org.apache.inlong.agent.conf.OffsetProfile;
import org.apache.inlong.agent.db.OffsetDb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * used to store instance offset to db
 * where key is task id + read file name and value is instance offset
 */
public class OffsetManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetManager.class);
    private static volatile OffsetManager offsetManager = null;
    private final OffsetDb offsetDb;

    private OffsetManager() {
        this.offsetDb = new OffsetDb();
    }

    /**
     * task position manager singleton, can only generated by agent manager
     */
    public static OffsetManager init() {
        if (offsetManager == null) {
            synchronized (OffsetManager.class) {
                if (offsetManager == null) {
                    offsetManager = new OffsetManager();
                }
            }
        }
        return offsetManager;
    }

    /**
     * get taskPositionManager singleton
     */
    public static OffsetManager getInstance() {
        if (offsetManager == null) {
            throw new RuntimeException("task position manager has not been initialized by agentManager");
        }
        return offsetManager;
    }

    public void setOffset(OffsetProfile profile) {
        offsetDb.setOffset(profile);
    }

    public void deleteOffset(String taskId, String instanceId) {
        offsetDb.deleteOffset(taskId, instanceId);
    }

    public OffsetProfile getOffset(String taskId, String instanceId) {
        return offsetDb.getOffset(taskId, instanceId);
    }
}
