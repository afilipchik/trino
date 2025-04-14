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
package io.trino.plugin.k8s;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class K8SMetadata
        implements ConnectorMetadata
{
    private final K8SClient k8sClient;

    @Inject
    public K8SMetadata(K8SClient k8sClient)
    {
        this.k8sClient = requireNonNull(k8sClient, "k8sClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(k8sClient.getSchemaNames());
    }

    @Override
    public K8STableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        K8STable table = k8sClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new K8STableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(((K8STableHandle) table).toSchemaTableName());
    }

    public ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        if (!listSchemaNames().contains(tableName.getSchemaName())) {
            return null;
        }

        K8STable table = k8sClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(
                tableName,
                table.getColumnsMetadata());
    }

    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(k8sClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : k8sClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        K8STableHandle k8sTableHandle = (K8STableHandle) tableHandle;

        K8STable table = k8sClient.getTable(k8sTableHandle.getSchemaName(), k8sTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(k8sTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int ordinalPosition = 0;
        for (K8SColumn column : table.getColumns()) {
            columnHandles.put(column.getName(), new K8SColumnHandle(column.getName(), column.getType(), ordinalPosition));
            ordinalPosition++;
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnHandle, "columnHandle is null");
        K8STableHandle k8sTableHandle = (K8STableHandle) tableHandle;
        K8SColumnHandle k8sColumnHandle = (K8SColumnHandle) columnHandle;

        K8STable table = k8sClient.getTable(k8sTableHandle.getSchemaName(), k8sTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(k8sTableHandle.toSchemaTableName());
        }

        if (k8sColumnHandle.getOrdinalPosition() >= table.getColumns().size()) {
            throw new IllegalArgumentException("Invalid column ordinal: " + k8sColumnHandle.getOrdinalPosition());
        }

        K8SColumn column = table.getColumns().get(k8sColumnHandle.getOrdinalPosition());
        checkArgument(column.getName().equals(k8sColumnHandle.getColumnName()), "Column handle %s is not for table %s", columnHandle, tableHandle);
        checkArgument(column.getType().equals(k8sColumnHandle.getColumnType()), "Column handle %s is not for table %s", columnHandle, tableHandle);

        return new ColumnMetadata(column.getName(), column.getType());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }
}
