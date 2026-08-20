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
import io.trino.plugin.federation.client.RegionClients;
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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.federation.FederationPageSource.REGION_CHANNEL;
import static java.util.Objects.requireNonNull;

public class FederationPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final RegionClients regionClients;
    private final String remoteCatalog;

    @Inject
    public FederationPageSourceProvider(RegionClients regionClients, FederationConfig config)
    {
        this.regionClients = requireNonNull(regionClients, "regionClients is null");
        this.remoteCatalog = config.getRemoteCatalog();
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
        FederationSplit federationSplit = (FederationSplit) split;
        FederationTableHandle handle = (FederationTableHandle) table;
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
                new RemoteTable(remoteCatalog, handle.schemaTableName().getSchemaName(), handle.schemaTableName().getTableName()),
                projections.build(),
                remoteConstraint(handle),
                Optional.empty(),
                handle.topN().map(FederationPageSourceProvider::toTopNSpec),
                handle.limit());
        // applyFilter keeps only domains the SQL builder can render, so nothing may be left over
        checkState(query.unsupportedFilterColumns().isEmpty(), "constraint contains domains that cannot be pushed down: %s", query.unsupportedFilterColumns());

        return new FederationPageSource(
                regionClients.client(federationSplit.regionName()),
                query.sql(),
                dataTypes.build(),
                outputChannels,
                memoryContext);
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
