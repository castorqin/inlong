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

package org.apache.inlong.manager.pojo.sink.tube;

import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.util.JsonTypeDefine;
import org.apache.inlong.manager.pojo.sink.StreamSink;

import io.swagger.annotations.ApiModel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Tube mq sink info
 */
@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@ApiModel(value = "Tube mq sink info")
@JsonTypeDefine(value = SinkType.TUBEMQ)
public class TubeSink extends StreamSink {

    private String tid;
    private String bid;
    private String topic;
    private String delimiter;
    private String dataType;
    private String masterHost;
    private String linkMaxDelayedMsgCount;
    private String sessionWarnDelayedMsgCount;
    private String sessionMaxDelayedMsgCount;
    private String rpcTimeOutMs;
    private String tubeHbPeriodMs;
}
