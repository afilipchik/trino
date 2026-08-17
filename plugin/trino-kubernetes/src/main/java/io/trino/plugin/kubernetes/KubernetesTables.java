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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.kubernetes.client.KubernetesClient;
import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.plugin.kubernetes.schema.KubernetesTypeMapper;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

/**
 * Table metadata layer: resolves discovered resources to their typed column lists,
 * with caching.
 */
public class KubernetesTables
{
    private final KubernetesClient client;
    private final KubernetesTypeMapper typeMapper;
    private final Cache<SchemaTableName, List<KubernetesColumnHandle>> columnsCache;

    @Inject
    public KubernetesTables(KubernetesClient client, KubernetesTypeMapper typeMapper, KubernetesConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeMapper = requireNonNull(typeMapper, "typeMapper is null");
        this.columnsCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(config.getMetadataCacheTtl().toMillis(), MILLISECONDS)
                .maximumSize(10_000)
                .build();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(client.resourcesBySchema().keySet());
    }

    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        client.resourcesBySchema().forEach((schema, resources) -> {
            if (schemaName.isPresent() && !schemaName.get().equals(schema)) {
                return;
            }
            for (ResourceDescriptor resource : resources) {
                tables.add(new SchemaTableName(schema, resource.tableName()));
            }
        });
        return tables.build();
    }

    public Optional<ResourceDescriptor> resource(SchemaTableName tableName)
    {
        return client.resource(tableName.getSchemaName(), tableName.getTableName());
    }

    /**
     * All columns of the table: the typed columns from the OpenAPI schema followed by
     * the synthetic hidden columns (name, namespace, merge row id).
     */
    public List<KubernetesColumnHandle> columns(ResourceDescriptor resource)
    {
        return uncheckedCacheGet(
                columnsCache,
                new SchemaTableName(resource.schemaName(), resource.tableName()),
                () -> loadColumns(resource));
    }

    public List<KubernetesColumnHandle> visibleColumns(ResourceDescriptor resource)
    {
        return columns(resource).stream()
                .filter(column -> !column.hidden())
                .collect(ImmutableList.toImmutableList());
    }

    private List<KubernetesColumnHandle> loadColumns(ResourceDescriptor resource)
    {
        List<KubernetesColumnHandle> schemaColumns = typeMapper.columns(client.openApiDocument(resource), resource);
        Set<String> names = schemaColumns.stream().map(KubernetesColumnHandle::name).collect(toSet());
        ImmutableList.Builder<KubernetesColumnHandle> columns = ImmutableList.builder();
        if (!names.contains(KubernetesColumns.NAME_COLUMN)) {
            columns.add(KubernetesColumns.NAME_HANDLE);
        }
        if (!names.contains(KubernetesColumns.NAMESPACE_COLUMN) && resource.namespaced()) {
            columns.add(KubernetesColumns.NAMESPACE_HANDLE);
        }
        columns.addAll(schemaColumns);
        if (!names.contains(KubernetesColumns.MERGE_ROW_ID_COLUMN)) {
            columns.add(KubernetesColumns.MERGE_ROW_ID_HANDLE);
        }
        return columns.build();
    }
}
