/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.kubernetes;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum KubernetesErrorCode
        implements ErrorCodeSupplier
{
    KUBERNETES_CLIENT_ERROR(0, EXTERNAL),
    KUBERNETES_AUTHENTICATION_ERROR(1, EXTERNAL),
    KUBERNETES_SCHEMA_ERROR(2, EXTERNAL),
    KUBERNETES_WRITE_CONFLICT(3, EXTERNAL),
    KUBERNETES_INVALID_WRITE(4, USER_ERROR),
    KUBERNETES_RESOURCE_NOT_FOUND(5, EXTERNAL);

    private final ErrorCode errorCode;

    KubernetesErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0521_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
