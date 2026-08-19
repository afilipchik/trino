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

import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * @param namespaceFilter namespace equality constraint pushed down into API list calls
 * @param nameFilter object name equality constraint pushed down as a field selector
 * @param clusterFilter cluster equality constraint used to prune the split fan-out
 *         in multi-cluster catalogs
 */
public record KubernetesTableHandle(
        String schemaName,
        String tableName,
        String group,
        String version,
        String kind,
        boolean namespaced,
        Optional<String> namespaceFilter,
        Optional<String> nameFilter,
        Optional<String> clusterFilter,
        OptionalLong limit)
        implements ConnectorTableHandle
{
    public KubernetesTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(group, "group is null");
        requireNonNull(version, "version is null");
        requireNonNull(kind, "kind is null");
        requireNonNull(namespaceFilter, "namespaceFilter is null");
        requireNonNull(nameFilter, "nameFilter is null");
        requireNonNull(clusterFilter, "clusterFilter is null");
        requireNonNull(limit, "limit is null");
    }

    public static KubernetesTableHandle of(ResourceDescriptor resource)
    {
        return new KubernetesTableHandle(
                resource.schemaName(),
                resource.tableName(),
                resource.group(),
                resource.version(),
                resource.kind(),
                resource.namespaced(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
    }

    public ResourceDescriptor descriptor()
    {
        return new ResourceDescriptor(schemaName, tableName, group, version, kind, namespaced);
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }
}
