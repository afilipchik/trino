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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.federation.AggregateCombiners.Combiner;
import io.trino.plugin.federation.AggregateCombiningPageSource.KeyColumn;
import io.trino.plugin.federation.AggregateCombiningPageSource.OutputColumn;
import io.trino.plugin.federation.AggregateCombiningPageSource.OutputSource;
import io.trino.plugin.federation.client.RegionClient;
import io.trino.plugin.federation.client.RegionClients;
import io.trino.plugin.federation.sql.AggregateKind;
import io.trino.plugin.federation.sql.AggregateSpec;
import io.trino.plugin.federation.sql.AggregationSpec;
import io.trino.plugin.federation.sql.RemoteColumn;
import io.trino.plugin.federation.sql.RemoteQuery;
import io.trino.plugin.federation.sql.RemoteSqlBuilder;
import io.trino.plugin.federation.sql.RemoteTable;
import io.trino.plugin.federation.sql.SortSpec;
import io.trino.plugin.federation.sql.TopNSpec;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.federation.AggregateCombiningPageSource.REGION_KEY;
import static io.trino.plugin.federation.FederationPageSource.REGION_CHANNEL;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class FederationPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final String PROBE_COLUMN_NAME = "$probe";

    private final RegionClients regionClients;
    private final String remoteCatalog;
    private final int fanoutThreads;
    private final TypeOperators typeOperators = new TypeOperators();

    @Inject
    public FederationPageSourceProvider(RegionClients regionClients, FederationConfig config)
    {
        this.regionClients = requireNonNull(regionClients, "regionClients is null");
        this.remoteCatalog = config.getRemoteCatalog();
        this.fanoutThreads = config.getFanoutThreads();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext)
    {
        FederationTableHandle handle = (FederationTableHandle) table;
        if (split instanceof FederationAggregateSplit aggregateSplit) {
            return createAggregatePageSource(aggregateSplit, handle, columns, memoryContext);
        }
        return createScanPageSource((FederationSplit) split, handle, columns, memoryContext);
    }

    private ConnectorPageSource createScanPageSource(
            FederationSplit split,
            FederationTableHandle handle,
            List<ColumnHandle> columns,
            MemoryContext memoryContext)
    {
        checkState(handle.aggregation().isEmpty(), "aggregated scans use the fan-out page source, not per-region page sources");

        ImmutableList.Builder<RemoteColumn> projections = ImmutableList.builder();
        ImmutableList.Builder<Type> dataTypes = ImmutableList.builder();
        int[] outputChannels = new int[columns.size()];
        int dataChannel = 0;
        for (int channel = 0; channel < columns.size(); channel++) {
            FederationColumnHandle column = (FederationColumnHandle) columns.get(channel);
            if (column.regionColumn()) {
                outputChannels[channel] = REGION_CHANNEL;
                continue;
            }
            outputChannels[channel] = dataChannel;
            dataChannel++;
            projections.add(toRemoteColumn(column));
            dataTypes.add(column.type());
        }

        RemoteQuery query = RemoteSqlBuilder.buildSql(
                remoteTable(handle),
                projections.build(),
                remoteConstraint(handle),
                Optional.empty(),
                handle.topN().map(FederationPageSourceProvider::toTopNSpec),
                handle.limit());
        // applyFilter keeps only domains the SQL builder can render, so nothing may be left over
        checkState(query.unsupportedFilterColumns().isEmpty(), "constraint contains domains that cannot be pushed down: %s", query.unsupportedFilterColumns());

        return new FederationPageSource(
                regionClients.client(split.regionName()),
                query.sql(),
                dataTypes.build(),
                outputChannels,
                memoryContext);
    }

    /**
     * Builds the partial-aggregate query shared by all regions and the fan-out page source
     * combining their results. The remote result layout is the non-{@code _region} grouping
     * columns, then the partial columns of every aggregate in order (the AVG kinds
     * contribute a sum+count pair), then the emptiness probe when grouping degenerated to a
     * remote global aggregate.
     */
    private ConnectorPageSource createAggregatePageSource(
            FederationAggregateSplit split,
            FederationTableHandle handle,
            List<ColumnHandle> columns,
            MemoryContext memoryContext)
    {
        FederationAggregation aggregation = handle.aggregation()
                .orElseThrow(() -> new IllegalStateException("aggregate split for a handle without aggregation"));
        checkState(handle.limit().isEmpty() && handle.topN().isEmpty(), "aggregation cannot be combined with limit or topN");

        List<FederationColumnHandle> groupingColumns = aggregation.groupingColumns();
        ImmutableList.Builder<RemoteColumn> remoteGroupingBuilder = ImmutableList.builder();
        ImmutableList.Builder<KeyColumn> keyColumnsBuilder = ImmutableList.builder();
        int remoteChannel = 0;
        for (FederationColumnHandle groupingColumn : groupingColumns) {
            if (groupingColumn.regionColumn()) {
                // regions do not know the synthetic _region column; its key component is the
                // constant region name of each merged stream
                keyColumnsBuilder.add(new KeyColumn(REGION_KEY, groupingColumn.type()));
                continue;
            }
            remoteGroupingBuilder.add(toRemoteColumn(groupingColumn));
            keyColumnsBuilder.add(new KeyColumn(remoteChannel, groupingColumn.type()));
            remoteChannel++;
        }
        List<RemoteColumn> remoteGrouping = remoteGroupingBuilder.build();

        ImmutableList.Builder<AggregateSpec> partials = ImmutableList.builder();
        ImmutableList.Builder<Supplier<Combiner>> combinerFactories = ImmutableList.builder();
        for (FederationAggregateColumn aggregate : aggregation.aggregates()) {
            List<AggregateSpec> aggregatePartials = AggregateCombiners.remotePartials(aggregate);
            partials.addAll(aggregatePartials);
            combinerFactories.add(AggregateCombiners.combinerFactory(aggregate, remoteChannel, typeOperators));
            remoteChannel += aggregatePartials.size();
        }
        int probeChannel = -1;
        if (!groupingColumns.isEmpty() && remoteGrouping.isEmpty()) {
            // grouping only on _region turns the remote query into a global aggregate that
            // returns a row even for an empty shard; the probe filters those rows out
            partials.add(new AggregateSpec(AggregateKind.COUNT_ALL, Optional.empty(), new RemoteColumn(PROBE_COLUMN_NAME, BIGINT)));
            probeChannel = remoteChannel;
        }

        RemoteQuery query = RemoteSqlBuilder.buildSql(
                remoteTable(handle),
                ImmutableList.of(),
                remoteConstraint(handle),
                Optional.of(new AggregationSpec(remoteGrouping, partials.build())),
                Optional.empty(),
                OptionalLong.empty());
        checkState(query.unsupportedFilterColumns().isEmpty(), "constraint contains domains that cannot be pushed down: %s", query.unsupportedFilterColumns());

        ImmutableList.Builder<OutputColumn> outputColumns = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            FederationColumnHandle federationColumn = (FederationColumnHandle) column;
            outputColumns.add(toOutputColumn(federationColumn, aggregation));
        }

        List<RegionClient> clients = split.regionNames().stream()
                .map(regionClients::client)
                .collect(toImmutableList());
        return new AggregateCombiningPageSource(
                clients,
                query.sql(),
                query.outputColumns().stream()
                        .map(RemoteColumn::type)
                        .collect(toImmutableList()),
                keyColumnsBuilder.build(),
                combinerFactories.build(),
                probeChannel,
                outputColumns.build(),
                fanoutThreads,
                typeOperators,
                memoryContext);
    }

    private static OutputColumn toOutputColumn(FederationColumnHandle column, FederationAggregation aggregation)
    {
        int keyIndex = aggregation.groupingColumns().indexOf(column);
        if (keyIndex >= 0) {
            return new OutputColumn(OutputSource.GROUPING_KEY, keyIndex, column.type());
        }
        for (int index = 0; index < aggregation.aggregates().size(); index++) {
            if (aggregation.aggregates().get(index).outputName().equals(column.name())) {
                return new OutputColumn(OutputSource.AGGREGATE, index, column.type());
            }
        }
        throw new IllegalStateException("projected column is neither a grouping column nor an aggregate output: " + column.name());
    }

    private RemoteTable remoteTable(FederationTableHandle handle)
    {
        return new RemoteTable(remoteCatalog, handle.schemaTableName().getSchemaName(), handle.schemaTableName().getTableName());
    }

    private static TupleDomain<RemoteColumn> remoteConstraint(FederationTableHandle handle)
    {
        handle.constraint().getDomains().ifPresent(domains -> checkState(
                domains.keySet().stream().noneMatch(FederationColumnHandle::regionColumn),
                "region column domains must be consumed into activeRegions by applyFilter"));
        return handle.constraint().transformKeys(FederationPageSourceProvider::toRemoteColumn);
    }

    private static TopNSpec toTopNSpec(FederationTopN topN)
    {
        return new TopNSpec(
                topN.ordering().stream()
                        .map(sort -> SortSpec.of(toRemoteColumn(sort.column()), sort.sortOrder()))
                        .collect(toImmutableList()),
                topN.count());
    }

    private static RemoteColumn toRemoteColumn(FederationColumnHandle column)
    {
        return new RemoteColumn(column.name(), column.type());
    }
}
