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

package org.apache.inlong.manager.pojo.node.tube;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.node.DataNodeRequest;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * TubeMQ data node request
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeDefine(value = DataNodeType.TUBEMQ)
@ApiModel("TubeMQ data node request")
public class TubeMQDataNodeRequest extends DataNodeRequest {

    private String masterHostList;
    private String linkMaxDelayMsgCount;
    private String sessionWarnDelayedMsgCount;
    private String sessionMaxDelayMsgCount;
    private String nettyWriteBufferHighWaterMark;
    private String rpcTimeOutMs;
    private String tubeHbPeriodMs;

    public TubeMQDataNodeRequest() {
        super.setType(DataNodeType.TUBEMQ);
    }
}
