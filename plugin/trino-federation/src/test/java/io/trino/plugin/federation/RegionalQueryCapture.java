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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Runs a query on the central cluster and captures the queries each region received because
 * of it, by diffing every region's {@code system.runtime.queries} around the execution. The
 * diff is keyed on query ids because the runtime table evicts old entries, so the log may
 * shrink between the snapshots. Tests using this must run their methods on a single thread
 * ({@code @Execution(SAME_THREAD)}): concurrent queries would show up in each other's diffs.
 */
final class RegionalQueryCapture
{
    private final FederationQueryRunner runner;
    private final String remoteQueryPattern;

    /**
     * @param remoteQueryPattern SQL LIKE pattern selecting the remote queries of interest,
     *         for example {@code %"memory"."default"."items"%}. Metadata listing queries also carry
     *         the {@code trino-federation} source but target {@code information_schema}, so patterns
     *         anchored on the remote catalog and schema exclude them.
     */
    RegionalQueryCapture(FederationQueryRunner runner, String remoteQueryPattern)
    {
        this.runner = requireNonNull(runner, "runner is null");
        this.remoteQueryPattern = requireNonNull(remoteQueryPattern, "remoteQueryPattern is null");
    }

    Captured execute(String sql)
    {
        Map<String, Set<String>> before = new HashMap<>();
        for (String region : runner.regionNames()) {
            before.put(region, ImmutableSet.copyOf(regionQueryLog(region).keySet()));
        }
        MaterializedResult result = runner.execute(sql);
        ImmutableMap.Builder<String, List<String>> newQueries = ImmutableMap.builder();
        for (String region : runner.regionNames()) {
            Set<String> knownQueryIds = before.get(region);
            newQueries.put(region, regionQueryLog(region).entrySet().stream()
                    .filter(entry -> !knownQueryIds.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(toImmutableList()));
        }
        return new Captured(result, newQueries.buildOrThrow());
    }

    /**
     * Matching queries this region received, keyed by query id, oldest first.
     */
    private Map<String, String> regionQueryLog(String region)
    {
        Map<String, String> log = new LinkedHashMap<>();
        for (MaterializedRow row : runner.executeOnRegion(
                        region,
                        """
                        SELECT query_id, query FROM system.runtime.queries
                        WHERE source = 'trino-federation' AND query LIKE '%s'
                        ORDER BY created
                        """.formatted(remoteQueryPattern))
                .getMaterializedRows()) {
            log.put((String) row.getField(0), (String) row.getField(1));
        }
        return log;
    }

    record Captured(MaterializedResult result, Map<String, List<String>> remoteQueriesByRegion)
    {
        List<String> remoteQueries(String region)
        {
            return remoteQueriesByRegion.get(region);
        }

        List<String> allRemoteQueries()
        {
            return remoteQueriesByRegion.values().stream()
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }
    }
}
