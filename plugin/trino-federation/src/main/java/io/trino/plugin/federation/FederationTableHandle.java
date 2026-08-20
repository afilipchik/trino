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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * @param columns snapshot of all table columns in listing order, ending with the synthetic
 *         {@code _region} column
 * @param activeRegions names of the regions the scan fans out to, in configured order;
 *         pruned by {@code _region} filters
 * @param constraint predicate pushed into the regional queries
 * @param limit LIMIT pushed into the regional queries; not guaranteed, each region only
 *         pre-reduces
 * @param topN ORDER BY + LIMIT pushed into the regional queries; not guaranteed
 * @param aggregation partial aggregation run on every region and combined by the connector
 */
public record FederationTableHandle(
        SchemaTableName schemaTableName,
        List<FederationColumnHandle> columns,
        List<String> activeRegions,
        TupleDomain<FederationColumnHandle> constraint,
        OptionalLong limit,
        Optional<FederationTopN> topN,
        Optional<FederationAggregation> aggregation)
        implements ConnectorTableHandle
{
    public FederationTableHandle
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        activeRegions = ImmutableList.copyOf(requireNonNull(activeRegions, "activeRegions is null"));
        requireNonNull(constraint, "constraint is null");
        requireNonNull(limit, "limit is null");
        requireNonNull(topN, "topN is null");
        requireNonNull(aggregation, "aggregation is null");
    }

    public static FederationTableHandle of(SchemaTableName schemaTableName, List<FederationColumnHandle> columns, List<String> activeRegions)
    {
        return new FederationTableHandle(
                schemaTableName,
                columns,
                activeRegions,
                TupleDomain.all(),
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public FederationTableHandle withColumns(List<FederationColumnHandle> columns)
    {
        return new FederationTableHandle(schemaTableName, columns, activeRegions, constraint, limit, topN, aggregation);
    }

    public FederationTableHandle withActiveRegions(List<String> activeRegions)
    {
        return new FederationTableHandle(schemaTableName, columns, activeRegions, constraint, limit, topN, aggregation);
    }

    public FederationTableHandle withConstraint(TupleDomain<FederationColumnHandle> constraint)
    {
        return new FederationTableHandle(schemaTableName, columns, activeRegions, constraint, limit, topN, aggregation);
    }

    public FederationTableHandle withLimit(long limit)
    {
        return new FederationTableHandle(schemaTableName, columns, activeRegions, constraint, OptionalLong.of(limit), topN, aggregation);
    }

    public FederationTableHandle withTopN(FederationTopN topN)
    {
        return new FederationTableHandle(schemaTableName, columns, activeRegions, constraint, limit, Optional.of(topN), aggregation);
    }

    public FederationTableHandle withAggregation(FederationAggregation aggregation)
    {
        return new FederationTableHandle(schemaTableName, columns, activeRegions, constraint, limit, topN, Optional.of(aggregation));
    }
}
