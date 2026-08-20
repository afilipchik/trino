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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.federation.client.RegionClient;
import io.trino.plugin.federation.client.RegionClients;
import io.trino.plugin.federation.client.RemoteColumnMetadata;
import io.trino.plugin.federation.sql.RemoteSqlBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.plugin.federation.FederationColumns.REGION_COLUMN;
import static io.trino.plugin.federation.FederationColumns.REGION_COLUMN_NAME;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_REGION_UNREACHABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Lists schemas, tables, and columns of the federated catalog. Regions are assumed to hold
 * homogeneous shards of the same tables, so metadata is read from the first reachable region
 * in configured order and cached briefly.
 */
public class FederationMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(FederationMetadata.class);

    private static final Duration METADATA_CACHE_TTL = Duration.ofSeconds(10);
    private static final String INFORMATION_SCHEMA = "information_schema";
    private static final String SCHEMAS_CACHE_KEY = "";

    private final RegionClients regionClients;
    private final Cache<String, List<String>> schemasCache;
    private final Cache<Optional<String>, List<SchemaTableName>> tablesCache;
    private final Cache<SchemaTableName, List<FederationColumnHandle>> columnsCache;

    @Inject
    public FederationMetadata(RegionClients regionClients)
    {
        this.regionClients = requireNonNull(regionClients, "regionClients is null");
        this.schemasCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(METADATA_CACHE_TTL)
                .build();
        this.tablesCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(METADATA_CACHE_TTL)
                .build();
        this.columnsCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(METADATA_CACHE_TTL)
                .build();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return cacheGet(schemasCache, SCHEMAS_CACHE_KEY, () -> fromFirstReachableRegion(client ->
                client.listSchemas().stream()
                        .filter(schema -> !schema.equals(INFORMATION_SCHEMA))
                        .collect(toImmutableList())));
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.equals(Optional.of(INFORMATION_SCHEMA))) {
            return ImmutableList.of();
        }
        return cacheGet(tablesCache, schemaName, () -> fromFirstReachableRegion(client ->
                client.listTables(schemaName).stream()
                        .filter(table -> !table.getSchemaName().equals(INFORMATION_SCHEMA))
                        .collect(toImmutableList())));
    }

    @Override
    public FederationTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }
        List<FederationColumnHandle> columns = tableColumns(tableName);
        if (columns.isEmpty()) {
            return null;
        }
        return FederationTableHandle.of(tableName, columns, regionNames());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        FederationTableHandle handle = (FederationTableHandle) table;
        return new ConnectorTableMetadata(handle.schemaTableName(), columnMetadata(handle.columns()));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        FederationTableHandle handle = (FederationTableHandle) tableHandle;
        ImmutableMap.Builder<String, ColumnHandle> columns = ImmutableMap.builder();
        for (FederationColumnHandle column : handle.columns()) {
            columns.put(column.name(), column);
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((FederationColumnHandle) columnHandle).columnMetadata();
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        ImmutableMap.Builder<SchemaTableName, RelationColumnsMetadata> relationColumnsBuilder = ImmutableMap.builder();
        for (SchemaTableName table : listTables(session, schemaName)) {
            List<FederationColumnHandle> columns = tableColumns(table);
            if (columns.isEmpty()) {
                // table disappeared between listing and describing
                continue;
            }
            relationColumnsBuilder.put(table, RelationColumnsMetadata.forTable(table, columnMetadata(columns)));
        }
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = relationColumnsBuilder.buildOrThrow();
        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        FederationTableHandle handle = (FederationTableHandle) table;
        if (handle.limit().isPresent() || handle.topN().isPresent() || handle.aggregation().isPresent()) {
            // filtering below an already pushed LIMIT / ORDER BY / aggregation would change results
            return Optional.empty();
        }

        List<String> newActiveRegions = handle.activeRegions();
        TupleDomain<FederationColumnHandle> newConstraint = handle.constraint();
        TupleDomain<ColumnHandle> remainingFilter = TupleDomain.all();
        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isNone()) {
            newActiveRegions = ImmutableList.of();
        }
        else {
            ImmutableMap.Builder<FederationColumnHandle, Domain> pushableDomains = ImmutableMap.builder();
            ImmutableMap.Builder<ColumnHandle, Domain> remainingDomains = ImmutableMap.builder();
            for (Map.Entry<ColumnHandle, Domain> entry : summary.getDomains().orElseThrow().entrySet()) {
                FederationColumnHandle column = (FederationColumnHandle) entry.getKey();
                Domain domain = entry.getValue();
                if (column.regionColumn()) {
                    // consumed: a _region domain prunes the fan-out instead of being sent to
                    // the regions, where the column does not exist
                    newActiveRegions = newActiveRegions.stream()
                            .filter(region -> domain.includesNullableValue(utf8Slice(region)))
                            .collect(toImmutableList());
                }
                else if (RemoteSqlBuilder.isPushableDomain(column.type(), domain)) {
                    pushableDomains.put(column, domain);
                }
                else {
                    remainingDomains.put(column, domain);
                }
            }
            newConstraint = newConstraint.intersect(TupleDomain.withColumnDomains(pushableDomains.buildOrThrow()));
            remainingFilter = TupleDomain.withColumnDomains(remainingDomains.buildOrThrow());
        }

        if (newActiveRegions.equals(handle.activeRegions()) && newConstraint.equals(handle.constraint())) {
            return Optional.empty();
        }
        return Optional.of(new ConstraintApplicationResult<>(
                handle.withActiveRegions(newActiveRegions).withConstraint(newConstraint),
                remainingFilter,
                constraint.getExpression(),
                false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle table,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        FederationTableHandle handle = (FederationTableHandle) table;
        if (handle.aggregation().isPresent()) {
            return Optional.empty();
        }
        if (!projections.stream().allMatch(Variable.class::isInstance)) {
            // computed projections and dereferences are not supported
            return Optional.empty();
        }

        Set<FederationColumnHandle> referencedColumns = assignments.values().stream()
                .map(FederationColumnHandle.class::cast)
                .collect(toImmutableSet());
        List<FederationColumnHandle> newColumns = handle.columns().stream()
                .filter(referencedColumns::contains)
                .collect(toImmutableList());
        checkState(newColumns.size() == referencedColumns.size(), "projection references columns missing from the table handle: %s", referencedColumns);
        if (newColumns.equals(handle.columns())) {
            return Optional.empty();
        }
        return Optional.of(new ProjectionApplicationResult<>(
                handle.withColumns(newColumns),
                projections,
                assignments.entrySet().stream()
                        .map(entry -> new Assignment(entry.getKey(), entry.getValue(), ((FederationColumnHandle) entry.getValue()).type()))
                        .collect(toImmutableList()),
                false));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        FederationTableHandle handle = (FederationTableHandle) table;
        if (handle.topN().isPresent() || handle.aggregation().isPresent()) {
            // an already pushed ORDER BY ... LIMIT pre-reduces at least as much, and the
            // regional SQL cannot carry a plain LIMIT next to it
            return Optional.empty();
        }
        if (handle.limit().isPresent() && handle.limit().orElseThrow() <= limit) {
            return Optional.empty();
        }
        return Optional.of(new LimitApplicationResult<>(handle.withLimit(limit), false, false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle table,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        FederationTableHandle handle = (FederationTableHandle) table;
        if (handle.topN().isPresent() || handle.aggregation().isPresent()) {
            return Optional.empty();
        }

        ImmutableList.Builder<FederationSortColumn> ordering = ImmutableList.builder();
        for (SortItem sortItem : sortItems) {
            FederationColumnHandle column = (FederationColumnHandle) assignments.get(sortItem.getName());
            checkState(column != null, "sort item references an unknown column: %s", sortItem.getName());
            if (column.regionColumn()) {
                // constant within each region, so a per-region ORDER BY cannot honor it
                return Optional.empty();
            }
            if (!RemoteSqlBuilder.isPushableType(column.type())) {
                return Optional.empty();
            }
            ordering.add(new FederationSortColumn(column, sortItem.getSortOrder()));
        }
        return Optional.of(new TopNApplicationResult<>(handle.withTopN(new FederationTopN(ordering.build(), topNCount)), false, false));
    }

    /**
     * Ordered columns of a remote table with the synthetic {@code _region} column appended.
     * An empty result means the table does not exist: an existing table always lists at
     * least one remote column, so its result contains at least {@code _region}.
     */
    private List<FederationColumnHandle> tableColumns(SchemaTableName table)
    {
        if (table.getSchemaName().equals(INFORMATION_SCHEMA)) {
            return ImmutableList.of();
        }
        return cacheGet(columnsCache, table, () -> loadColumns(table));
    }

    private List<FederationColumnHandle> loadColumns(SchemaTableName table)
    {
        List<RemoteColumnMetadata> remoteColumns = fromFirstReachableRegion(client ->
                client.describeTable(table.getSchemaName(), table.getTableName()));
        if (remoteColumns.isEmpty()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<FederationColumnHandle> columns = ImmutableList.builder();
        for (RemoteColumnMetadata column : remoteColumns) {
            if (column.type().isEmpty()) {
                log.debug("Skipping column '%s' of table '%s' with unsupported type '%s'", column.name(), table, column.remoteType());
                continue;
            }
            if (column.name().equals(REGION_COLUMN_NAME)) {
                log.debug("Skipping column '%s' of table '%s' shadowed by the synthetic region column", column.name(), table);
                continue;
            }
            columns.add(new FederationColumnHandle(column.name(), column.type().get(), false));
        }
        columns.add(REGION_COLUMN);
        return columns.build();
    }

    private static List<ColumnMetadata> columnMetadata(List<FederationColumnHandle> columns)
    {
        return columns.stream()
                .map(FederationColumnHandle::columnMetadata)
                .collect(toImmutableList());
    }

    private List<String> regionNames()
    {
        return regionClients.clients().stream()
                .map(RegionClient::regionName)
                .collect(toImmutableList());
    }

    private <T> T fromFirstReachableRegion(Function<RegionClient, T> reader)
    {
        List<String> attemptedRegions = new ArrayList<>();
        List<TrinoException> failures = new ArrayList<>();
        for (RegionClient client : regionClients.clients()) {
            try {
                return reader.apply(client);
            }
            catch (TrinoException e) {
                if (!e.getErrorCode().equals(FEDERATION_REGION_UNREACHABLE.toErrorCode())) {
                    throw e;
                }
                attemptedRegions.add(client.regionName());
                failures.add(e);
            }
        }
        TrinoException exception = new TrinoException(
                FEDERATION_REGION_UNREACHABLE,
                "No region is reachable for metadata, attempted regions: " + String.join(", ", attemptedRegions));
        failures.forEach(exception::addSuppressed);
        throw exception;
    }

    private static <K, V> V cacheGet(Cache<K, V> cache, K key, Supplier<V> loader)
    {
        try {
            return uncheckedCacheGet(cache, key, loader);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw e;
        }
    }
}
