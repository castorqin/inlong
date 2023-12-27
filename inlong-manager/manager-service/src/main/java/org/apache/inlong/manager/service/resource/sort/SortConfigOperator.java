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

package org.apache.inlong.manager.service.resource.sort;

import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;

import java.util.List;

/**
 * Interface of the Sort config operator
 */
public interface SortConfigOperator {

    /**
     * Determines whether the current instance matches the specified type.
     *
     * @param sinkTypeList sink type list
     */
    Boolean accept(List<String> sinkTypeList);

    /**
     * Build Sort config.
     *
     * @param groupInfo inlong group info
     * @param streamInfo inlong stream info
     * @param isStream is the config built for inlong stream
     */
    void buildConfig(InlongGroupInfo groupInfo, InlongStreamInfo streamInfo, boolean isStream) throws Exception;

}
