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
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Verifies that supported aggregations run inside the regions and are combined by the
 * connector into final values, using plan assertions (which also compare every result
 * against a pushdown-disabled control run of the same query) and the SQL text the regions
 * received. Test methods must not run concurrently: each one diffs the shared regional
 * query logs around its own query.
 */
@Execution(SAME_THREAD)
final class TestFederationAggregationPushdown
        extends AbstractTestQueryFramework
{
    private FederationQueryRunner federation;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        federation = closeAfterClass(FederationQueryRunner.builder()
                .addRegion("east")
                .addRegion("west")
                .build());

        federation.executeOnAllRegions(
                """
                CREATE TABLE sales (
                    id BIGINT,
                    tiny TINYINT,
                    small SMALLINT,
                    qty INTEGER,
                    price DOUBLE,
                    ratio REAL,
                    amount DECIMAL(10,2),
                    category VARCHAR,
                    day DATE,
                    created TIMESTAMP(3))
                """);
        // per-region values are disjoint; category 'fruit', 'veg', and NULL span both
        // regions so their groups merge across regions, 'dairy' exists only in west; the
        // REAL and DOUBLE values are exactly representable so combined sums are exact
        federation.executeOnRegion("east",
                """
                INSERT INTO sales VALUES
                    (1, TINYINT '1', SMALLINT '11', 10, 1.5, REAL '0.5', 10.50, 'fruit', DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00.000'),
                    (2, TINYINT '2', SMALLINT '12', 20, 2.5, REAL '1.5', -3.25, 'fruit', DATE '2024-02-01', TIMESTAMP '2024-02-01 11:00:00.000'),
                    (3, NULL, NULL, NULL, NULL, NULL, NULL, 'veg', DATE '2024-03-01', NULL),
                    (4, TINYINT '4', SMALLINT '14', 40, 4.5, REAL '2.25', 7.75, 'veg', NULL, TIMESTAMP '2024-03-15 23:59:59.999'),
                    (5, TINYINT '5', SMALLINT '15', 50, 5.5, REAL '3.5', 99.99, NULL, DATE '2024-04-01', TIMESTAMP '2024-04-01 00:00:00.000')
                """);
        federation.executeOnRegion("west",
                """
                INSERT INTO sales VALUES
                    (6, TINYINT '6', SMALLINT '16', 60, 6.5, REAL '4.5', 1.10, 'fruit', DATE '2024-05-01', TIMESTAMP '2024-05-01 08:15:00.500'),
                    (7, NULL, NULL, NULL, NULL, NULL, NULL, 'veg', DATE '2024-06-01', NULL),
                    (8, TINYINT '8', SMALLINT '18', 80, 8.5, REAL '6.25', 42.00, NULL, NULL, TIMESTAMP '2024-06-15 12:00:00.000'),
                    (9, TINYINT '9', SMALLINT '19', 90, 9.5, REAL '7.5', -7.77, 'fruit', DATE '2024-07-04', TIMESTAMP '2024-07-04 04:04:04.004'),
                    (10, TINYINT '10', SMALLINT '20', 100, 10.5, REAL '8.5', 55.55, 'dairy', DATE '2024-08-01', TIMESTAMP '2024-08-01 20:00:00.123')
                """);

        federation.executeOnAllRegions("CREATE TABLE settlements (id BIGINT, value DOUBLE, label VARCHAR)");
        federation.executeOnRegion("west", "INSERT INTO settlements VALUES (1, 1.5, 'a'), (2, 2.5, NULL)");

        federation.executeOnAllRegions("CREATE TABLE audits (id BIGINT, value DOUBLE)");

        federation.executeOnAllRegions("CREATE TABLE big_values (v BIGINT)");
        federation.executeOnAllRegions("INSERT INTO big_values VALUES (9223372036854775000)");

        federation.executeOnAllRegions("CREATE TABLE offset_values (v BIGINT)");
        federation.executeOnRegion("east", "INSERT INTO offset_values VALUES (9223372036854775000)");
        federation.executeOnRegion("west", "INSERT INTO offset_values VALUES (-9223372036854775000)");

        federation.executeOnAllRegions("CREATE TABLE big_decimals (v DECIMAL(38,0))");
        federation.executeOnAllRegions("INSERT INTO big_decimals VALUES (DECIMAL '90000000000000000000000000000000000000')");

        return federation.central();
    }

    @Test
    void testSupportedAggregatesArePushedDownAndCorrect()
    {
        List<String> aggregates = ImmutableList.of(
                "count(*)",
                "count(qty)",
                "count(category)",
                "sum(id)",
                "sum(ratio)",
                "sum(price)",
                "sum(amount)",
                "min(id)",
                "max(id)",
                "min(price)",
                "max(price)",
                "min(ratio)",
                "max(ratio)",
                "min(amount)",
                "max(amount)",
                "min(category)",
                "max(category)",
                "min(day)",
                "max(day)",
                "min(created)",
                "max(created)",
                "avg(price)",
                "avg(ratio)");
        // null means global aggregation; the assertion also compares the results against a
        // pushdown-disabled control run of the same query
        List<String> groupings = Arrays.asList(null, "category", "_region", "category, day");
        for (String aggregate : aggregates) {
            for (String grouping : groupings) {
                String sql = grouping == null
                        ? "SELECT %s FROM sales".formatted(aggregate)
                        : "SELECT %s, %s FROM sales GROUP BY %s".formatted(grouping, aggregate, grouping);
                assertThat(query(sql)).isFullyPushedDown();
            }
        }
    }

    @Test
    void testCountStarShipsOneRowPerRegion()
    {
        String sql = "SELECT count(*) FROM sales";
        assertThat(query(sql)).isFullyPushedDown();

        Captured captured = executeAndCapture(sql);
        assertThat(captured.result().getOnlyValue()).isEqualTo(10L);
        for (String region : federation.regionNames()) {
            // the partial query returns exactly one pre-aggregated row by construction
            assertThat(captured.remoteQueries(region))
                    .containsExactly("SELECT count(*) AS \"$agg_0\" FROM \"memory\".\"default\".\"sales\"");
        }
    }

    @Test
    void testGroupByPushdownEvidence()
    {
        String sql = "SELECT category, sum(id) FROM sales GROUP BY category";
        assertThat(query(sql)).isFullyPushedDown();

        Captured captured = executeAndCapture(sql);
        assertThat(captured.result().getRowCount()).isEqualTo(4);
        assertThat(captured.allRemoteQueries())
                .hasSize(2)
                .allMatch(remoteSql -> remoteSql.contains("sum(\"id\") AS \"$agg_0\"")
                        && remoteSql.contains("GROUP BY \"category\""));
    }

    @Test
    void testFilterComposesWithAggregation()
    {
        String sql = "SELECT category, count(*) FROM sales WHERE id <= 8 GROUP BY category";
        assertThat(query(sql)).isFullyPushedDown();

        Captured captured = executeAndCapture(sql);
        assertThat(captured.allRemoteQueries())
                .hasSize(2)
                .allMatch(remoteSql -> remoteSql.contains("WHERE \"id\" <= 8")
                        && remoteSql.contains("GROUP BY \"category\""));
    }

    @Test
    void testRegionPruningComposesWithGlobalAggregation()
    {
        String sql = "SELECT sum(id) FROM sales WHERE _region = 'east'";
        assertThat(query(sql)).isFullyPushedDown();

        Captured captured = executeAndCapture(sql);
        assertThat(captured.result().getOnlyValue()).isEqualTo(15L);
        assertThat(captured.remoteQueries("west")).isEmpty();
        assertThat(captured.remoteQueries("east"))
                .containsExactly("SELECT sum(\"id\") AS \"$agg_0\" FROM \"memory\".\"default\".\"sales\"");
    }

    @Test
    void testPrunedToNoRegionsStillEmitsGlobalRow()
    {
        String sql = "SELECT count(*), sum(id) FROM sales WHERE _region = 'nowhere'";
        assertThat(query(sql)).isFullyPushedDown();

        Captured captured = executeAndCapture(sql);
        assertThat(captured.result().getMaterializedRows()).hasSize(1);
        assertThat(captured.result().getMaterializedRows().getFirst().getFields()).containsExactly(0L, null);
        assertThat(captured.remoteQueries("east")).isEmpty();
        assertThat(captured.remoteQueries("west")).isEmpty();
    }

    @Test
    void testGroupByRegionRunsGlobalPartials()
    {
        String sql = "SELECT _region, count(*), sum(id) FROM sales GROUP BY _region";
        assertThat(query(sql)).isFullyPushedDown();

        Captured captured = executeAndCapture(sql);
        List<String> rows = captured.result().getMaterializedRows().stream()
                .map(row -> row.getFields().toString())
                .sorted()
                .collect(toImmutableList());
        assertThat(rows).containsExactly("[east, 5, 15]", "[west, 5, 40]");
        // the _region grouping never reaches the regions: each runs a global partial with
        // an emptiness probe, and the combiner keys on the constant region of each stream
        assertThat(captured.allRemoteQueries())
                .hasSize(2)
                .allMatch(remoteSql -> !remoteSql.contains("_region")
                        && !remoteSql.contains("GROUP BY")
                        && remoteSql.contains("\"$probe\""));
    }

    @Test
    void testDistinctRegion()
    {
        String sql = "SELECT DISTINCT _region FROM sales";
        assertThat(query(sql)).isFullyPushedDown();
        assertThat(query(sql)).matches("VALUES CAST('east' AS varchar), CAST('west' AS varchar)");

        // settlements has rows only in west, so the emptiness probe drops east's group
        String sparse = "SELECT DISTINCT _region FROM settlements";
        assertThat(query(sparse)).isFullyPushedDown();
        assertThat(query(sparse)).matches("VALUES CAST('west' AS varchar)");
    }

    @Test
    void testDistinctDataColumn()
    {
        String sql = "SELECT DISTINCT category FROM sales";
        assertThat(query(sql)).isFullyPushedDown();

        Captured captured = executeAndCapture(sql);
        assertThat(captured.allRemoteQueries())
                .hasSize(2)
                .allMatch(remoteSql -> remoteSql.contains("GROUP BY \"category\""));
    }

    @Test
    void testSumDecimalIsExact()
    {
        assertThat(query("SELECT sum(amount) FROM sales")).isFullyPushedDown();
        assertThat(query("SELECT sum(amount) FROM sales"))
                .matches("SELECT CAST(205.87 AS decimal(38,2))");
        assertThat(query("SELECT sum(amount) FROM sales WHERE amount < 0"))
                .matches("SELECT CAST(-11.02 AS decimal(38,2))");
    }

    @Test
    void testAvgWithNullsAndEmptyGroups()
    {
        // both rows matching the filter carry a NULL price, so the combined count is 0
        String allNulls = "SELECT avg(price) FROM sales WHERE qty IS NULL";
        assertThat(query(allNulls)).isFullyPushedDown();
        assertThat(query(allNulls)).matches("SELECT CAST(NULL AS double)");

        String grouped = "SELECT category, avg(price), avg(ratio) FROM sales GROUP BY category";
        assertThat(query(grouped)).isFullyPushedDown();
    }

    @Test
    void testEmptyTables()
    {
        assertThat(query("SELECT count(*) FROM audits")).isFullyPushedDown();
        assertThat(query("SELECT count(*) FROM audits")).matches("VALUES BIGINT '0'");

        String globalNulls = "SELECT sum(value), min(value), max(value), avg(value) FROM audits";
        assertThat(query(globalNulls)).isFullyPushedDown();
        assertThat(query(globalNulls))
                .matches("SELECT CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double), CAST(NULL AS double)");

        assertThat(query("SELECT value, count(*) FROM audits GROUP BY value")).isFullyPushedDown();
        assertThat(query("SELECT value, count(*) FROM audits GROUP BY value")).returnsEmptyResult();

        // grouping only on _region over an empty table must produce no groups at all
        assertThat(query("SELECT _region, count(*) FROM audits GROUP BY _region")).isFullyPushedDown();
        assertThat(query("SELECT _region, count(*) FROM audits GROUP BY _region")).returnsEmptyResult();

        // settlements is empty in east only
        assertThat(query("SELECT count(*), sum(value) FROM settlements")).isFullyPushedDown();
        assertThat(query("SELECT count(*), sum(value) FROM settlements")).matches("SELECT BIGINT '2', DOUBLE '4.0'");
        assertThat(query("SELECT _region, count(*) FROM settlements GROUP BY _region"))
                .matches("VALUES (CAST('west' AS varchar), BIGINT '2')");
    }

    @Test
    void testCombineOverflowMatchesEngineSemantics()
    {
        // each region's partial fits in a bigint, the connector-side combine overflows
        assertThatThrownBy(() -> federation.execute("SELECT sum(v) FROM big_values"))
                .hasMessageContaining("bigint addition overflow");

        assertThatThrownBy(() -> federation.execute("SELECT sum(v) FROM big_decimals"))
                .hasMessageContaining("Decimal overflow");
    }

    @Test
    void testSumLargeBigintWithinRange()
    {
        // the partials nearly saturate the bigint range but their combined sum fits
        assertThat(query("SELECT sum(v) FROM offset_values")).isFullyPushedDown();
        assertThat(query("SELECT sum(v) FROM offset_values")).matches("VALUES BIGINT '0'");
    }

    @Test
    void testUnsupportedShapesFallBackWithCorrectResults()
    {
        // every assertion also compares the results against a pushdown-disabled control run

        // DISTINCT aggregates are not combinable from per-region partials
        assertThat(query("SELECT count(DISTINCT category) FROM sales"))
                .isNotFullyPushedDown(AggregationNode.class);
        // filtered aggregates are rejected; the mask predicate is a projection over the scan
        assertThat(query("SELECT sum(id) FILTER (WHERE id > 5) FROM sales"))
                .isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
        // avg over decimal must be exact; per-region partials cannot reproduce it
        assertThat(query("SELECT avg(amount) FROM sales"))
                .isNotFullyPushedDown(AggregationNode.class);
        // avg over integers divides the exact global sum; the implicit cast to bigint also
        // plans as a projection over the scan
        assertThat(query("SELECT avg(qty) FROM sales"))
                .isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
    }

    @Test
    void testSumOverNarrowIntegersFallsBack()
    {
        // the engine plans sum(tinyint/smallint/integer) as sum(CAST(x AS bigint)) with the
        // cast in a projection between the aggregation and the scan, so the pushdown rule
        // never fires; correctness comes from the engine aggregating raw rows
        for (String column : ImmutableList.of("tiny", "small", "qty")) {
            assertThat(query("SELECT sum(%s) FROM sales".formatted(column)))
                    .isNotFullyPushedDown(AggregationNode.class, ProjectNode.class);
        }
    }

    @Test
    void testUnsupportedFunctionFallsBack()
    {
        // arbitrary() has no supported decomposition; the single-row group keeps the
        // fallback result deterministic for the control comparison
        assertThat(query("SELECT arbitrary(category) FROM sales WHERE id = 10"))
                .isNotFullyPushedDown(AggregationNode.class);
        assertThat(query("SELECT arbitrary(category) FROM sales WHERE id = 10"))
                .matches("VALUES CAST('dairy' AS varchar)");
    }

    @Test
    void testAggregationOverPushedLimitFallsBack()
    {
        // the limit is pushed into the handle first and blocks aggregation pushdown; with
        // LIMIT larger than the table the result stays deterministic
        String sql = "SELECT sum(id) FROM (SELECT id FROM sales LIMIT 100)";
        assertThat(query(sql)).matches("VALUES BIGINT '55'");

        Captured captured = executeAndCapture(sql);
        // the regions receive the pre-reducing LIMIT scan, not an aggregate
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(remoteSql -> remoteSql.contains("LIMIT 100") && !remoteSql.contains("sum("));
    }

    @Test
    void testMinMaxOnVarcharDateTimestamp()
    {
        assertThat(query("SELECT min(category), max(category), min(day), max(day), min(created), max(created) FROM sales"))
                .matches(
                        """
                        SELECT CAST('dairy' AS varchar), CAST('veg' AS varchar),
                            DATE '2024-01-01', DATE '2024-08-01',
                            TIMESTAMP '2024-01-01 10:00:00.000', TIMESTAMP '2024-08-01 20:00:00.123'
                        """);
    }

    /**
     * Runs a query on the central cluster and captures the queries each region received
     * because of it. The diff is keyed on query ids because {@code system.runtime.queries}
     * evicts old entries, so the log may shrink between the snapshots.
     */
    private Captured executeAndCapture(String sql)
    {
        Map<String, Set<String>> before = new HashMap<>();
        for (String region : federation.regionNames()) {
            before.put(region, regionQueryLog(region).keySet());
        }
        MaterializedResult result = federation.execute(sql);
        ImmutableMap.Builder<String, List<String>> newQueries = ImmutableMap.builder();
        for (String region : federation.regionNames()) {
            Set<String> knownQueryIds = before.get(region);
            newQueries.put(region, regionQueryLog(region).entrySet().stream()
                    .filter(entry -> !knownQueryIds.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(toImmutableList()));
        }
        return new Captured(result, newQueries.buildOrThrow());
    }

    /**
     * Scan and partial-aggregate queries this region received, keyed by query id, oldest
     * first. Metadata listing queries also carry the {@code trino-federation} source but
     * target {@code information_schema}, so they are excluded.
     */
    private Map<String, String> regionQueryLog(String region)
    {
        Map<String, String> log = new LinkedHashMap<>();
        for (MaterializedRow row : federation.executeOnRegion(
                        region,
                        """
                        SELECT query_id, query FROM system.runtime.queries
                        WHERE source = 'trino-federation' AND query LIKE '%"memory"."default".%'
                        ORDER BY created
                        """)
                .getMaterializedRows()) {
            log.put((String) row.getField(0), (String) row.getField(1));
        }
        return log;
    }

    private record Captured(MaterializedResult result, Map<String, List<String>> remoteQueriesByRegion)
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
