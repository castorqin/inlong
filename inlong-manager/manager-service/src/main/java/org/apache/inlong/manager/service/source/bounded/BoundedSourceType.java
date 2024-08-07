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

package org.apache.inlong.manager.service.source.bounded;

import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import lombok.Getter;

@Getter
public enum BoundedSourceType {

    PULSAR("pulsar");

    private final String sourceType;

    BoundedSourceType(String name) {
        this.sourceType = name;
    }

    public static BoundedSourceType getInstance(String name) {
        for (BoundedSourceType source : values()) {
            if (source.getSourceType().equalsIgnoreCase(name)) {
                return source;
            }
        }
        throw new BusinessException(ErrorCodeEnum.BOUNDED_SOURCE_TYPE_NOT_SUPPORTED,
                String.format(ErrorCodeEnum.BOUNDED_SOURCE_TYPE_NOT_SUPPORTED.getMessage(), name));
    }

    public static boolean isBoundedSource(String name) {
        for (BoundedSourceType source : values()) {
            if (source.getSourceType().equalsIgnoreCase(name)) {
                return true;
            }
        }
        return false;
    }
}
