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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

/**
 * @param name lowercase Trino column name
 * @param jsonName original JSON field name of the object's top-level property, empty for synthetic columns
 * @param type Trino type mapped from the resource OpenAPI schema
 * @param hidden whether the column is hidden from {@code SELECT *} and {@code DESCRIBE}
 */
public record KubernetesColumnHandle(String name, String jsonName, Type type, boolean hidden)
        implements ColumnHandle
{
    public KubernetesColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(jsonName, "jsonName is null");
        requireNonNull(type, "type is null");
    }

    public ColumnMetadata columnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setHidden(hidden)
                .build();
    }
}
