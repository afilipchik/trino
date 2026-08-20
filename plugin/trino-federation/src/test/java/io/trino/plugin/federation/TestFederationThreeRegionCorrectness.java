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
import io.trino.plugin.federation.RegionalQueryCapture.Captured;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Correctness sweep over three regions holding disjoint TPC-H-derived shards, comparing
 * every federated query against the same query over unsharded control tables in a memory
 * catalog on the central cluster. All aggregated measures are exact types (bigint, decimal,
 * date, varchar), so the federated combine and the single-cluster control cannot diverge by
 * floating-point summation order. Runs on a single thread because one test diffs the shared
 * regional query logs around its own query.
 */
@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestFederationThreeRegionCorrectness
{
    private static final String ORDERS_PROJECTION =
            "orderkey, custkey, orderstatus, CAST(totalprice AS DECIMAL(12,2)) AS totalprice, orderdate, orderpriority, clerk";
    private static final String CUSTOMER_PROJECTION =
            "custkey, name, nationkey, CAST(acctbal AS DECIMAL(12,2)) AS acctbal, mktsegment";

    private FederationQueryRunner runner;
    private RegionalQueryCapture capture;

    @BeforeAll
    void setUp()
            throws Exception
    {
        runner = FederationQueryRunner.builder()
                .addRegion("apac")
                .addRegion("emea")
                .addRegion("amer")
                .build();
        capture = new RegionalQueryCapture(runner, "%\"memory\".\"default\".%");

        for (String region : runner.regionNames()) {
            runner.region(region).installPlugin(new TpchPlugin());
            runner.region(region).createCatalog("tpch", "tpch", ImmutableMap.of());
        }
        DistributedQueryRunner central = runner.central();
        central.installPlugin(new TpchPlugin());
        central.createCatalog("tpch", "tpch", ImmutableMap.of());
        central.installPlugin(new MemoryPlugin());
        central.createCatalog("control", "memory", ImmutableMap.of());

        // the shard predicates partition the key space, so the three shards union to the
        // control tables exactly
        runner.executeOnRegion(
                "apac",
                "CREATE TABLE orders AS SELECT %s FROM tpch.tiny.orders WHERE orderkey < 20000".formatted(ORDERS_PROJECTION));
        runner.executeOnRegion(
                "emea",
                "CREATE TABLE orders AS SELECT %s FROM tpch.tiny.orders WHERE orderkey >= 20000 AND orderkey < 40000".formatted(ORDERS_PROJECTION));
        runner.executeOnRegion(
                "amer",
                "CREATE TABLE orders AS SELECT %s FROM tpch.tiny.orders WHERE orderkey >= 40000".formatted(ORDERS_PROJECTION));
        central.execute(
                "CREATE TABLE control.default.orders AS SELECT %s FROM tpch.tiny.orders".formatted(ORDERS_PROJECTION));

        runner.executeOnRegion(
                "apac",
                "CREATE TABLE customer AS SELECT %s FROM tpch.tiny.customer WHERE custkey < 500".formatted(CUSTOMER_PROJECTION));
        runner.executeOnRegion(
                "emea",
                "CREATE TABLE customer AS SELECT %s FROM tpch.tiny.customer WHERE custkey >= 500 AND custkey < 1000".formatted(CUSTOMER_PROJECTION));
        runner.executeOnRegion(
                "amer",
                "CREATE TABLE customer AS SELECT %s FROM tpch.tiny.customer WHERE custkey >= 1000".formatted(CUSTOMER_PROJECTION));
        central.execute(
                "CREATE TABLE control.default.customer AS SELECT %s FROM tpch.tiny.customer".formatted(CUSTOMER_PROJECTION));
    }

    @AfterAll
    void tearDown()
    {
        runner.close();
    }

    @Test
    void testFullScanMatchesControl()
    {
        assertSameRows(
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk FROM orders",
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk FROM control.default.orders");
    }

    @Test
    void testRegionColumnReflectsShardRanges()
    {
        assertSameRows(
                "SELECT _region, count(*) FROM orders GROUP BY _region",
                """
                SELECT CAST('apac' AS varchar), count(*) FROM control.default.orders WHERE orderkey < 20000
                UNION ALL SELECT CAST('emea' AS varchar), count(*) FROM control.default.orders WHERE orderkey >= 20000 AND orderkey < 40000
                UNION ALL SELECT CAST('amer' AS varchar), count(*) FROM control.default.orders WHERE orderkey >= 40000
                """);
    }

    @Test
    void testFilterMatchesControl()
    {
        assertSameRows(
                "SELECT orderkey, totalprice FROM orders WHERE totalprice > 350000",
                "SELECT orderkey, totalprice FROM control.default.orders WHERE totalprice > 350000");
        // range crossing two shard boundaries
        assertSameRows(
                "SELECT orderkey FROM orders WHERE orderkey BETWEEN 15000 AND 45000",
                "SELECT orderkey FROM control.default.orders WHERE orderkey BETWEEN 15000 AND 45000");
    }

    @Test
    void testFilterAndProjectionMatchControl()
    {
        assertSameRows(
                "SELECT clerk, totalprice FROM orders WHERE orderstatus = 'F' AND orderdate >= DATE '1995-01-01'",
                "SELECT clerk, totalprice FROM control.default.orders WHERE orderstatus = 'F' AND orderdate >= DATE '1995-01-01'");
    }

    @Test
    void testGlobalAggregatesMatchControl()
    {
        assertSameRows(
                "SELECT count(*), count(clerk), sum(totalprice), min(orderdate), max(orderdate), min(clerk), max(clerk) FROM orders",
                "SELECT count(*), count(clerk), sum(totalprice), min(orderdate), max(orderdate), min(clerk), max(clerk) FROM control.default.orders");
    }

    @Test
    void testGroupedAggregatesMatchControl()
    {
        assertSameRows(
                "SELECT orderstatus, count(*), sum(totalprice), min(orderkey), max(orderkey) FROM orders GROUP BY orderstatus",
                "SELECT orderstatus, count(*), sum(totalprice), min(orderkey), max(orderkey) FROM control.default.orders GROUP BY orderstatus");
        assertSameRows(
                "SELECT orderpriority, count(*) FROM orders WHERE orderdate >= DATE '1996-01-01' GROUP BY orderpriority",
                "SELECT orderpriority, count(*) FROM control.default.orders WHERE orderdate >= DATE '1996-01-01' GROUP BY orderpriority");
    }

    @Test
    void testCountDistinctFallbackMatchesControl()
    {
        assertSameRows(
                "SELECT count(DISTINCT clerk) FROM orders",
                "SELECT count(DISTINCT clerk) FROM control.default.orders");
    }

    @Test
    void testTopNMatchesControl()
    {
        // the orderkey tiebreak makes the order fully deterministic
        assertSameRowsOrdered(
                "SELECT orderkey, totalprice FROM orders ORDER BY totalprice DESC, orderkey LIMIT 20",
                "SELECT orderkey, totalprice FROM control.default.orders ORDER BY totalprice DESC, orderkey LIMIT 20");
    }

    @Test
    void testLimitReturnsRowsFromControlSet()
    {
        MaterializedResult limited = runner.execute("SELECT orderkey FROM orders LIMIT 25");
        assertThat(limited.getRowCount()).isEqualTo(25);
        MaterializedResult control = runner.central().execute("SELECT orderkey FROM control.default.orders");
        assertThat(control.getOnlyColumn()).containsAll(limited.getOnlyColumn().toList());
    }

    @Test
    void testJoinOfTwoFederatedTablesAtCentralMatchesControl()
    {
        // the join is never pushed down: each federated table ships its (filtered) rows and
        // the central engine joins them
        assertSameRows(
                """
                SELECT c.mktsegment, count(*), sum(o.totalprice)
                FROM orders o JOIN customer c ON o.custkey = c.custkey
                GROUP BY c.mktsegment
                """,
                """
                SELECT c.mktsegment, count(*), sum(o.totalprice)
                FROM control.default.orders o JOIN control.default.customer c ON o.custkey = c.custkey
                GROUP BY c.mktsegment
                """);
        assertSameRows(
                """
                SELECT o.orderkey, c.name
                FROM orders o JOIN customer c ON o.custkey = c.custkey
                WHERE o.totalprice > 350000
                """,
                """
                SELECT o.orderkey, c.name
                FROM control.default.orders o JOIN control.default.customer c ON o.custkey = c.custkey
                WHERE o.totalprice > 350000
                """);
    }

    @Test
    void testMixedFilterAggregateTopNMatchesControl()
    {
        assertSameRowsOrdered(
                """
                SELECT orderstatus, count(*) AS c, sum(totalprice)
                FROM orders
                WHERE orderdate >= DATE '1995-06-01'
                GROUP BY orderstatus
                ORDER BY c DESC, orderstatus LIMIT 2
                """,
                """
                SELECT orderstatus, count(*) AS c, sum(totalprice)
                FROM control.default.orders
                WHERE orderdate >= DATE '1995-06-01'
                GROUP BY orderstatus
                ORDER BY c DESC, orderstatus LIMIT 2
                """);
    }

    @Test
    void testPartialRegionPruningWithAggregation()
    {
        // pruning to a strict subset of regions (existing two-region tests only cover
        // pruning to one region or to none)
        Captured captured = capture.execute(
                "SELECT orderstatus, count(*) FROM orders WHERE _region IN ('apac', 'amer') GROUP BY orderstatus");
        assertEqualsIgnoreOrder(
                captured.result().getMaterializedRows(),
                runner.central().execute(
                        """
                        SELECT orderstatus, count(*) FROM control.default.orders
                        WHERE orderkey < 20000 OR orderkey >= 40000
                        GROUP BY orderstatus
                        """).getMaterializedRows());
        assertThat(captured.remoteQueries("emea")).isEmpty();
        assertThat(captured.remoteQueries("apac")).hasSize(1);
        assertThat(captured.remoteQueries("amer")).hasSize(1);
    }

    private void assertSameRows(String federatedSql, String controlSql)
    {
        MaterializedResult federated = runner.execute(federatedSql);
        MaterializedResult control = runner.central().execute(controlSql);
        assertThat(federated.getTypes()).isEqualTo(control.getTypes());
        assertThat(federated.getRowCount()).isNotZero();
        assertEqualsIgnoreOrder(federated.getMaterializedRows(), control.getMaterializedRows());
    }

    private void assertSameRowsOrdered(String federatedSql, String controlSql)
    {
        MaterializedResult federated = runner.execute(federatedSql);
        MaterializedResult control = runner.central().execute(controlSql);
        assertThat(federated.getTypes()).isEqualTo(control.getTypes());
        assertThat(federated.getMaterializedRows())
                .isNotEmpty()
                .containsExactlyElementsOf(control.getMaterializedRows());
    }
}
