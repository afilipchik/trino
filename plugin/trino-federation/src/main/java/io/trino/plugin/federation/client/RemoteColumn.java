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
package io.trino.plugin.federation.client;

import io.trino.spi.type.Type;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A column of a remote table as reported by {@code information_schema.columns}. The Trino
 * type is empty when the remote type is not supported by the federation connector, so
 * metadata listing can skip the column instead of failing.
 */
public record RemoteColumn(String name, String remoteType, Optional<Type> type)
{
    public RemoteColumn
    {
        requireNonNull(name, "name is null");
        requireNonNull(remoteType, "remoteType is null");
        requireNonNull(type, "type is null");
    }
}
