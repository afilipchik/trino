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

import io.trino.plugin.federation.RegionalQueryCapture.Captured;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Verifies that predicates, projections, LIMIT, and TopN run inside the regions, and that
 * {@code _region} predicates prune the fan-out, using both plan assertions and the SQL text
 * the regions received (via each region's {@code system.runtime.queries}). Test methods must
 * not run concurrently: each one diffs the shared regional query logs around its own query.
 */
@Execution(SAME_THREAD)
final class TestFederationPushdown
        extends AbstractTestQueryFramework
{
    private FederationQueryRunner federation;
    private RegionalQueryCapture capture;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        federation = closeAfterClass(FederationQueryRunner.builder()
                .addRegion("east")
                .addRegion("west")
                .build());
        capture = new RegionalQueryCapture(federation, "%\"memory\".\"default\".\"items\"%");

        federation.executeOnAllRegions(
                """
                CREATE TABLE items (
                    id BIGINT,
                    name VARCHAR,
                    ship_date DATE,
                    price DOUBLE)
                """);
        federation.executeOnRegion("east",
                """
                INSERT INTO items VALUES
                    (1, 'apple', DATE '2024-01-10', 1.5),
                    (2, 'banana', DATE '2024-02-20', 2.5),
                    (3, 'cherry', DATE '2024-03-30', 3.5),
                    (4, 'damson', DATE '2024-04-05', 4.5),
                    (5, NULL, DATE '2024-05-15', 5.5)
                """);
        federation.executeOnRegion("west",
                """
                INSERT INTO items VALUES
                    (6, 'fig', DATE '2024-06-01', 6.5),
                    (7, 'grape', DATE '2024-07-11', 7.5),
                    (8, 'honeydew', DATE '2024-08-21', 8.5),
                    (9, 'kiwi', DATE '2024-09-02', 9.5),
                    (10, NULL, DATE '2024-10-12', infinity())
                """);
        return federation.central();
    }

    @Test
    void testEqualityPredicate()
    {
        assertThat(query("SELECT name FROM items WHERE id = 2")).isFullyPushedDown();

        Captured captured = capture.execute("SELECT name FROM items WHERE id = 2");
        assertThat(captured.result().getOnlyColumn()).containsExactly("banana");
        assertThat(captured.remoteQueries("east")).anyMatch(sql -> sql.contains("WHERE \"id\" = 2"));
        assertThat(captured.remoteQueries("west")).anyMatch(sql -> sql.contains("WHERE \"id\" = 2"));
    }

    @Test
    void testRangePredicate()
    {
        assertThat(query("SELECT id FROM items WHERE id > 3 AND id <= 8")).isFullyPushedDown();

        Captured captured = capture.execute("SELECT id FROM items WHERE id > 3 AND id <= 8");
        assertThat(captured.result().getOnlyColumn()).containsExactlyInAnyOrder(4L, 5L, 6L, 7L, 8L);
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains("\"id\" > 3") && sql.contains("\"id\" <= 8"));
    }

    @Test
    void testDateRangePredicate()
    {
        assertThat(query("SELECT id FROM items WHERE ship_date >= DATE '2024-06-01'")).isFullyPushedDown();

        Captured captured = capture.execute("SELECT id FROM items WHERE ship_date >= DATE '2024-06-01'");
        assertThat(captured.result().getOnlyColumn()).containsExactlyInAnyOrder(6L, 7L, 8L, 9L, 10L);
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains("\"ship_date\" >= DATE '2024-06-01'"));
    }

    @Test
    void testInPredicate()
    {
        assertThat(query("SELECT id FROM items WHERE id IN (1, 6, 9)")).isFullyPushedDown();

        Captured captured = capture.execute("SELECT id FROM items WHERE id IN (1, 6, 9)");
        assertThat(captured.result().getOnlyColumn()).containsExactlyInAnyOrder(1L, 6L, 9L);
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains("\"id\" IN (1, 6, 9)"));
    }

    @Test
    void testIsNullPredicate()
    {
        assertThat(query("SELECT id FROM items WHERE name IS NULL")).isFullyPushedDown();

        Captured captured = capture.execute("SELECT id FROM items WHERE name IS NULL");
        assertThat(captured.result().getOnlyColumn()).containsExactlyInAnyOrder(5L, 10L);
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains("\"name\" IS NULL"));
    }

    @Test
    void testLikePredicateFallsBackToEngine()
    {
        assertThat(query("SELECT id FROM items WHERE name LIKE '%an%'")).isNotFullyPushedDown(FilterNode.class);

        Captured captured = capture.execute("SELECT id FROM items WHERE name LIKE '%an%'");
        assertThat(captured.result().getOnlyColumn()).containsExactly(2L);
        // the LIKE stays with the engine, so the regional scans carry no WHERE clause
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains("\"name\"") && !sql.contains("WHERE"));
    }

    @Test
    void testNonFiniteDomainFallsBackToEngine()
    {
        assertThat(query("SELECT id FROM items WHERE price = infinity()")).isNotFullyPushedDown(FilterNode.class);

        Captured captured = capture.execute("SELECT id FROM items WHERE price = infinity()");
        assertThat(captured.result().getOnlyColumn()).containsExactly(10L);
        // an infinity literal cannot be rendered, so the domain stays with the engine
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains("\"price\"") && !sql.contains("WHERE"));
    }

    @Test
    void testRegionPredicatePrunesRegions()
    {
        assertThat(query("SELECT id FROM items WHERE _region = 'east'")).isFullyPushedDown();

        Captured captured = capture.execute("SELECT id FROM items WHERE _region = 'east'");
        assertThat(captured.result().getOnlyColumn()).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L);
        assertThat(captured.remoteQueries("west")).isEmpty();
        assertThat(captured.remoteQueries("east"))
                .hasSize(1)
                // the _region domain is consumed by pruning, never sent to the region
                .allMatch(sql -> !sql.contains("_region") && !sql.contains("WHERE"));
    }

    @Test
    void testRegionInAllRegionsDoesNotPrune()
    {
        Captured captured = capture.execute("SELECT id FROM items WHERE _region IN ('east', 'west')");
        assertThat(captured.result().getOnlyColumn())
                .containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L);
        assertThat(captured.remoteQueries("east")).hasSize(1);
        assertThat(captured.remoteQueries("west")).hasSize(1);
    }

    @Test
    void testNonexistentRegionContactsNoRegion()
    {
        assertThat(query("SELECT id FROM items WHERE _region = 'nowhere'")).isFullyPushedDown();

        Captured captured = capture.execute("SELECT id FROM items WHERE _region = 'nowhere'");
        assertThat(captured.result().getRowCount()).isEqualTo(0);
        assertThat(captured.remoteQueries("east")).isEmpty();
        assertThat(captured.remoteQueries("west")).isEmpty();
    }

    @Test
    void testRegionAndDataPredicateCombine()
    {
        assertThat(query("SELECT id FROM items WHERE _region = 'east' AND id > 3")).isFullyPushedDown();

        Captured captured = capture.execute("SELECT id FROM items WHERE _region = 'east' AND id > 3");
        assertThat(captured.result().getOnlyColumn()).containsExactlyInAnyOrder(4L, 5L);
        assertThat(captured.remoteQueries("west")).isEmpty();
        assertThat(captured.remoteQueries("east"))
                .hasSize(1)
                .allMatch(sql -> sql.contains("WHERE \"id\" > 3") && !sql.contains("_region"));
    }

    @Test
    void testLimitPushdown()
    {
        Captured captured = capture.execute("SELECT id FROM items LIMIT 3");
        assertThat(captured.result().getRowCount()).isEqualTo(3);
        // the engine may satisfy its final LIMIT before every region is even contacted,
        // but each contacted region must have received the pre-reducing LIMIT
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains(" LIMIT 3"));
    }

    @Test
    void testTopNPushdown()
    {
        Captured captured = capture.execute("SELECT id FROM items ORDER BY id DESC LIMIT 4");
        assertThat(captured.result().getOnlyColumn()).containsExactly(10L, 9L, 8L, 7L);
        assertThat(captured.remoteQueries("east"))
                .hasSize(1)
                .allMatch(sql -> sql.contains("ORDER BY \"id\" DESC NULLS LAST LIMIT 4"));
        assertThat(captured.remoteQueries("west"))
                .hasSize(1)
                .allMatch(sql -> sql.contains("ORDER BY \"id\" DESC NULLS LAST LIMIT 4"));
    }

    @Test
    void testTopNNullsFirstPushdown()
    {
        Captured captured = capture.execute("SELECT name FROM items ORDER BY name DESC NULLS FIRST LIMIT 3");
        assertThat(captured.result().getOnlyColumn()).containsExactly(null, null, "kiwi");
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains("ORDER BY \"name\" DESC NULLS FIRST LIMIT 3"));
    }

    @Test
    void testTopNOnRegionColumnFallsBackToEngine()
    {
        Captured captured = capture.execute("SELECT _region FROM items ORDER BY _region LIMIT 3");
        assertThat(captured.result().getOnlyColumn()).containsExactly("east", "east", "east");
        // _region is constant within a region, so a per-region ORDER BY cannot honor it
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> !sql.contains("ORDER BY") && !sql.contains("LIMIT"));
    }

    @Test
    void testFilterProjectionTopNCompose()
    {
        Captured captured = capture.execute("SELECT name FROM items WHERE id BETWEEN 3 AND 9 ORDER BY id LIMIT 2");
        assertThat(captured.result().getOnlyColumn()).containsExactly("cherry", "damson");
        assertThat(captured.allRemoteQueries())
                .isNotEmpty()
                .allMatch(sql -> sql.contains("WHERE \"id\" BETWEEN 3 AND 9")
                        && sql.contains("ORDER BY \"id\" ASC NULLS LAST LIMIT 2")
                        && sql.contains("\"name\"")
                        && !sql.contains("ship_date")
                        && !sql.contains("price"));
    }

    @Test
    void testRepeatedQueryIsStable()
    {
        String sql = "SELECT id FROM items WHERE _region = 'east' AND id > 3 ORDER BY id LIMIT 2";
        List<Object> first = federation.execute(sql).getOnlyColumn().collect(toImmutableList());
        List<Object> second = federation.execute(sql).getOnlyColumn().collect(toImmutableList());
        assertThat(first).containsExactly(4L, 5L);
        assertThat(second).isEqualTo(first);
    }
}
