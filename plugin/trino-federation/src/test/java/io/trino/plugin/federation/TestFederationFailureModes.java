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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Verifies that a failing region surfaces an error naming the region — on the per-region
 * scan path and on the aggregation fan-out combine path — and that {@code _region} pruning
 * to healthy regions keeps working around the failure. The scan path with an unreachable
 * region is covered in {@link TestFederationScan#testUnreachableRegionFailsWithRegionName}.
 */
@TestInstance(PER_CLASS)
final class TestFederationFailureModes
{
    private FederationQueryRunner runner;

    @BeforeAll
    void setUp()
            throws Exception
    {
        runner = FederationQueryRunner.builder()
                .addRegion("east")
                .addRegion("west")
                .build();

        // the table exists everywhere when federation metadata is read (from east, the
        // first region), but west drops its shard before any federated query runs
        runner.executeOnAllRegions("CREATE TABLE half_dropped (id BIGINT, v DOUBLE)");
        runner.executeOnRegion("east", "INSERT INTO half_dropped VALUES (1, 1.5), (2, 2.5)");
        runner.executeOnRegion("west", "INSERT INTO half_dropped VALUES (3, 3.5)");
        runner.executeOnRegion("west", "DROP TABLE half_dropped");
    }

    @AfterAll
    void tearDown()
    {
        runner.close();
    }

    @Test
    void testScanFailureNamesRegionMissingTheTable()
    {
        assertThatThrownBy(() -> runner.execute("SELECT id FROM half_dropped"))
                .hasMessageContaining("Region 'west'")
                .hasMessageContaining("half_dropped");
    }

    @Test
    void testAggregationCombineFailureNamesRegionMissingTheTable()
    {
        // the failure happens inside the fan-out combine path, on the region's future
        assertThatThrownBy(() -> runner.execute("SELECT count(*) FROM half_dropped"))
                .hasMessageContaining("Region 'west'");
        assertThatThrownBy(() -> runner.execute("SELECT v, sum(id) FROM half_dropped GROUP BY v"))
                .hasMessageContaining("Region 'west'");
    }

    @Test
    void testRegionPruningIsolatesTheFailingRegion()
    {
        assertThat(runner.execute("SELECT id FROM half_dropped WHERE _region = 'east'").getOnlyColumn())
                .containsExactlyInAnyOrder(1L, 2L);
        assertThat(runner.execute("SELECT count(*) FROM half_dropped WHERE _region = 'east'").getOnlyValue())
                .isEqualTo(2L);
    }

    @Test
    void testUnreachableRegionFailsAggregationNamingRegion()
            throws Exception
    {
        try (FederationQueryRunner failing = FederationQueryRunner.builder()
                .addRegion("live")
                .addStaticRegion("down", URI.create("http://127.0.0.1:1"))
                .build()) {
            failing.executeOnRegion("live", "CREATE TABLE events (id BIGINT, category VARCHAR)");
            failing.executeOnRegion("live", "INSERT INTO events VALUES (1, 'a'), (2, 'b')");

            // metadata comes from the first reachable region, so listing still works
            assertThat(failing.execute("SHOW TABLES").getOnlyColumn()).contains("events");

            assertThatThrownBy(() -> failing.execute("SELECT count(*) FROM events"))
                    .hasMessageContaining("Region 'down'");
            assertThatThrownBy(() -> failing.execute("SELECT category, sum(id) FROM events GROUP BY category"))
                    .hasMessageContaining("Region 'down'");

            assertThat(failing.execute("SELECT count(*) FROM events WHERE _region = 'live'").getOnlyValue())
                    .isEqualTo(2L);
        }
    }
}
