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

import io.trino.plugin.federation.sql.AggregateKind;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * One aggregate pushed to the regions as a partial and combined by the connector.
 *
 * @param kind remote aggregate function
 * @param argument column the aggregate is applied to, empty only for {@code count(*)}
 * @param outputName alias of the aggregate in the remote SELECT list and name of the
 *         resulting output column
 * @param outputType Trino type of the combined result
 * @param combineKind how per-region partials are merged into the final value
 */
public record FederationAggregateColumn(
        AggregateKind kind,
        Optional<FederationColumnHandle> argument,
        String outputName,
        Type outputType,
        CombineKind combineKind)
{
    public FederationAggregateColumn
    {
        requireNonNull(kind, "kind is null");
        requireNonNull(argument, "argument is null");
        requireNonNull(outputName, "outputName is null");
        requireNonNull(outputType, "outputType is null");
        requireNonNull(combineKind, "combineKind is null");
        checkArgument((kind == AggregateKind.COUNT_ALL) == argument.isEmpty(),
                "count(*) takes no argument and every other aggregate requires one: %s",
                kind);
    }
}
