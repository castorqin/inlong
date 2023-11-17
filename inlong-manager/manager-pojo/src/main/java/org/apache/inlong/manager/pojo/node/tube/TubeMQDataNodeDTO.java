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

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;

@Data
public class TubeMQDataNodeDTO {

    private String masterHostList;
    private String linkMaxDelayMsgCount;
    private String sessionWarnDelayedMsgCount;
    private String sessionMaxDelayMsgCount;
    private String nettyWriteBufferHighWaterMark;
    private String rpcTimeOutMs;
    private String sortChannelType;

    /**
     * Get the dto instance from the request
     */
    public static TubeMQDataNodeDTO getFromRequest(TubeMQDataNodeRequest request, String extParams) {
        TubeMQDataNodeDTO dto = StringUtils.isNotBlank(extParams)
                ? TubeMQDataNodeDTO.getFromJson(extParams)
                : new TubeMQDataNodeDTO();
        return CommonBeanUtils.copyProperties(request, dto, true);
    }

    /**
     * Get the dto instance from the JSON string.
     */
    public static TubeMQDataNodeDTO getFromJson(@NotNull String extParams) {
        try {
            return JsonUtils.parseObject(extParams, TubeMQDataNodeDTO.class);
        } catch (Exception e) {
            throw new BusinessException(ErrorCodeEnum.GROUP_INFO_INCORRECT,
                    String.format("Failed to parse extParams for Cloud log service node: %s", e.getMessage()));
        }
    }
}
