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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static java.util.Objects.requireNonNull;

public class KubernetesMetadata
        implements ConnectorMetadata
{
    private final KubernetesTables tables;

    @Inject
    public KubernetesMetadata(KubernetesTables tables)
    {
        this.tables = requireNonNull(tables, "tables is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return tables.listSchemaNames();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return tables.listTables(schemaName);
    }

    @Override
    public KubernetesTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        return tables.resource(tableName)
                .map(KubernetesTableHandle::of)
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        KubernetesTableHandle handle = (KubernetesTableHandle) table;
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (KubernetesColumnHandle column : tables.columns(handle.descriptor())) {
            columns.add(column.columnMetadata());
        }
        return new ConnectorTableMetadata(handle.schemaTableName(), columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
    {
        KubernetesTableHandle handle = (KubernetesTableHandle) table;
        ImmutableMap.Builder<String, ColumnHandle> columns = ImmutableMap.builder();
        for (KubernetesColumnHandle column : tables.columns(handle.descriptor())) {
            columns.put(column.name(), column);
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((KubernetesColumnHandle) columnHandle).columnMetadata();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        KubernetesTableHandle table = (KubernetesTableHandle) handle;
        if (table.limit().isPresent() && table.limit().orElseThrow() <= limit) {
            return Optional.empty();
        }
        KubernetesTableHandle newHandle = new KubernetesTableHandle(
                table.schemaName(),
                table.tableName(),
                table.group(),
                table.version(),
                table.kind(),
                table.namespaced(),
                table.namespaceFilter(),
                table.nameFilter(),
                OptionalLong.of(limit));
        return Optional.of(new LimitApplicationResult<>(newHandle, false, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        KubernetesTableHandle table = (KubernetesTableHandle) handle;
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isNone() || summary.getDomains().isEmpty()) {
            return Optional.empty();
        }

        Optional<String> namespaceFilter = table.namespaceFilter();
        Optional<String> nameFilter = table.nameFilter();
        Map<ColumnHandle, Domain> remaining = new HashMap<>(summary.getDomains().get());
        boolean pushedDown = false;

        for (Map.Entry<ColumnHandle, Domain> entry : summary.getDomains().get().entrySet()) {
            KubernetesColumnHandle column = (KubernetesColumnHandle) entry.getKey();
            Domain domain = entry.getValue();
            if (!KubernetesColumns.isSynthetic(column) || !domain.isSingleValue()) {
                continue;
            }
            String value = ((Slice) domain.getSingleValue()).toStringUtf8();
            if (column.name().equals(KubernetesColumns.NAMESPACE_COLUMN) && table.namespaced() && namespaceFilter.isEmpty()) {
                namespaceFilter = Optional.of(value);
                remaining.remove(entry.getKey());
                pushedDown = true;
            }
            else if (column.name().equals(KubernetesColumns.NAME_COLUMN) && nameFilter.isEmpty()) {
                nameFilter = Optional.of(value);
                remaining.remove(entry.getKey());
                pushedDown = true;
            }
        }

        if (!pushedDown) {
            return Optional.empty();
        }

        KubernetesTableHandle newHandle = new KubernetesTableHandle(
                table.schemaName(),
                table.tableName(),
                table.group(),
                table.version(),
                table.kind(),
                table.namespaced(),
                namespaceFilter,
                nameFilter,
                table.limit());
        return Optional.of(new ConstraintApplicationResult<>(newHandle, TupleDomain.withColumnDomains(ImmutableMap.copyOf(remaining)), constraint.getExpression(), false));
    }

    @Override
    public boolean supportsMissingColumnsOnInsert()
    {
        return true;
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> columns, RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        KubernetesTableHandle table = (KubernetesTableHandle) tableHandle;
        ImmutableList.Builder<KubernetesColumnHandle> insertColumns = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            insertColumns.add((KubernetesColumnHandle) column);
        }
        return new KubernetesInsertTableHandle(table, insertColumns.build());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public ColumnHandle getMergeRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return KubernetesColumns.MERGE_ROW_ID_HANDLE;
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return CHANGE_ONLY_UPDATED_COLUMNS;
    }

    @Override
    public ConnectorMergeTableHandle beginMerge(ConnectorSession session, ConnectorTableHandle tableHandle, Map<Integer, Collection<ColumnHandle>> updateCaseColumns, RetryMode retryMode)
    {
        if (retryMode != RetryMode.NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }
        KubernetesTableHandle table = (KubernetesTableHandle) tableHandle;
        Set<String> updatedColumns = updateCaseColumns.values().stream()
                .flatMap(Collection::stream)
                .map(column -> ((KubernetesColumnHandle) column).name())
                .collect(toImmutableSet());
        return new KubernetesMergeTableHandle(table, tables.visibleColumns(table.descriptor()), updatedColumns);
    }

    @Override
    public void finishMerge(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            List<ConnectorTableHandle> sourceTableHandles,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {}
}
