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
package io.trino.plugin.federation;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * @param name remote column name, or {@code _region} for the synthetic region column
 * @param type Trino type mapped from the remote type
 * @param regionColumn whether this is the synthetic {@code _region} column materialized by
 *         the connector instead of being read from the regions
 */
public record FederationColumnHandle(String name, Type type, boolean regionColumn)
        implements ColumnHandle
{
    public FederationColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(type, "type is null");
    }

    public ColumnMetadata columnMetadata()
    {
        return new ColumnMetadata(name, type);
    }
}
