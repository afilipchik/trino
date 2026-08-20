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

import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.MaterializedResult.resultBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Pins down the behavior when the homogeneous-shard assumption is broken and one region's
 * table drifts from the others: the federated schema comes from the first reachable region
 * (east here), columns present with the same type on every region stay fully queryable, and
 * per-region validation surfaces drift as an error naming the drifted region — at remote
 * query time for a missing column, and at scan time ({@code FEDERATION_TYPE_MISMATCH}) for a
 * changed type. Each test creates its own tables and applies the drift before the first
 * federated access, so the (briefly cached) federated metadata already sees the drifted
 * state.
 */
@TestInstance(PER_CLASS)
final class TestFederationSchemaDrift
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
    }

    @AfterAll
    void tearDown()
    {
        runner.close();
    }

    @Test
    void testColumnAddedOnNonMetadataRegionIsInvisibleAndHarmless()
    {
        runner.executeOnAllRegions("CREATE TABLE drift_extra_west (id BIGINT, name VARCHAR)");
        runner.executeOnRegion("east", "INSERT INTO drift_extra_west VALUES (1, 'a')");
        runner.executeOnRegion("west", "ALTER TABLE drift_extra_west ADD COLUMN extra VARCHAR");
        runner.executeOnRegion("west", "INSERT INTO drift_extra_west VALUES (2, 'b', 'x')");

        // the federated schema comes from east, so west's extra column is invisible ...
        MaterializedResult all = runner.execute("SELECT * FROM drift_extra_west");
        assertThat(all.getColumnNames()).containsExactly("id", "name", "_region");
        // ... and the common columns read correctly from both regions
        assertThat(all.getMaterializedRows()).containsExactlyInAnyOrderElementsOf(
                resultBuilder(runner.central().getDefaultSession(), all.getTypes())
                        .row(1L, "a", "east")
                        .row(2L, "b", "west")
                        .build()
                        .getMaterializedRows());
    }

    @Test
    void testColumnAddedOnMetadataRegionFailsClearlyOnRegionsMissingIt()
    {
        runner.executeOnAllRegions("CREATE TABLE drift_extra_east (id BIGINT, name VARCHAR)");
        runner.executeOnRegion("west", "INSERT INTO drift_extra_east VALUES (2, 'b')");
        runner.executeOnRegion("east", "ALTER TABLE drift_extra_east ADD COLUMN extra VARCHAR");
        runner.executeOnRegion("east", "INSERT INTO drift_extra_east VALUES (1, 'a', 'x')");

        // the added column is visible, because east is the metadata region ...
        assertThat(runner.execute("DESCRIBE drift_extra_east").getMaterializedRows())
                .anyMatch(row -> row.getField(0).equals("extra"));
        // ... selecting it fails on the region that lacks it, naming that region ...
        assertThatThrownBy(() -> runner.execute("SELECT id, extra FROM drift_extra_east"))
                .hasMessageContaining("Region 'west'");
        // ... while pruning to the drifted region, or selecting only common columns, works
        assertThat(runner.execute("SELECT extra FROM drift_extra_east WHERE _region = 'east'").getOnlyColumn())
                .containsExactly("x");
        assertThat(runner.execute("SELECT id FROM drift_extra_east").getOnlyColumn())
                .containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void testColumnTypeDriftFailsAtScanNamingRegion()
    {
        runner.executeOnAllRegions("CREATE TABLE drift_type (id BIGINT, v BIGINT)");
        runner.executeOnRegion("east", "INSERT INTO drift_type VALUES (1, 10)");
        runner.executeOnRegion("west", "DROP TABLE drift_type");
        runner.executeOnRegion("west", "CREATE TABLE drift_type (id BIGINT, v VARCHAR)");
        runner.executeOnRegion("west", "INSERT INTO drift_type VALUES (2, 'oops')");

        // columns whose type agrees on every region stay readable ...
        assertThat(runner.execute("SELECT id FROM drift_type").getOnlyColumn())
                .containsExactlyInAnyOrder(1L, 2L);
        // ... the drifted column fails per-region scan-time type validation
        assertThatThrownBy(() -> runner.execute("SELECT v FROM drift_type"))
                .hasMessageContaining("Region 'west' returned column types");
        // an aggregate over the drifted column fails remotely, still naming the region
        assertThatThrownBy(() -> runner.execute("SELECT sum(v) FROM drift_type"))
                .hasMessageContaining("Region 'west'");
        // pruning to a consistent region reads the drifted column fine
        assertThat(runner.execute("SELECT v FROM drift_type WHERE _region = 'east'").getOnlyColumn())
                .containsExactly(10L);
    }
}
