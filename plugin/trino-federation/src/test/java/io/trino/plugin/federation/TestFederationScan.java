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
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestFederationScan
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

        runner.executeOnAllRegions(
                """
                CREATE TABLE orders (
                    id BIGINT,
                    name VARCHAR,
                    price DECIMAL(10,2),
                    order_date DATE,
                    created TIMESTAMP(3),
                    active BOOLEAN)
                """);
        runner.executeOnRegion("east",
                """
                INSERT INTO orders VALUES
                    (1, 'apple', 10.50, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00.123', true),
                    (2, 'banana', 3.25, DATE '2024-02-01', TIMESTAMP '2024-02-01 11:30:00.001', false),
                    (3, 'cherry', NULL, NULL, NULL, NULL),
                    (4, NULL, 0.01, DATE '2024-03-15', TIMESTAMP '2024-03-15 23:59:59.999', true),
                    (5, 'elderberry', 99.99, DATE '2024-04-01', TIMESTAMP '2024-04-01 00:00:00.000', false)
                """);
        runner.executeOnRegion("west",
                """
                INSERT INTO orders VALUES
                    (6, 'fig', 1.10, DATE '2024-05-01', TIMESTAMP '2024-05-01 08:15:00.500', true),
                    (7, 'grape', NULL, DATE '2024-06-01', NULL, false),
                    (8, NULL, 42.00, NULL, TIMESTAMP '2024-06-15 12:00:00.000', NULL),
                    (9, 'kiwi', 7.77, DATE '2024-07-04', TIMESTAMP '2024-07-04 04:04:04.004', true),
                    (10, 'lemon', 55.55, DATE '2024-08-01', TIMESTAMP '2024-08-01 20:00:00.123', false)
                """);

        runner.executeOnAllRegions("CREATE TABLE returns (id BIGINT, reason VARCHAR)");
        runner.executeOnRegion("east", "INSERT INTO returns VALUES (1, 'damaged'), (2, 'late'), (3, 'wrong item')");
    }

    @AfterAll
    void tearDown()
    {
        runner.close();
    }

    @Test
    void testSelectAllUnionsAllRegions()
    {
        MaterializedResult actual = runner.execute("SELECT id, name, price, order_date, created, active, _region FROM orders");
        assertThat(actual.getTypes())
                .containsExactly(BIGINT, VARCHAR, createDecimalType(10, 2), DATE, createTimestampType(3), BOOLEAN, VARCHAR);

        List<MaterializedRow> expected = new ArrayList<>();
        for (String region : runner.regionNames()) {
            expected.addAll(runner.executeOnRegion(
                            region,
                            "SELECT id, name, price, order_date, created, active, '%s' FROM orders".formatted(region))
                    .getMaterializedRows());
        }
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected);
    }

    @Test
    void testColumnSubsetProjection()
    {
        MaterializedResult actual = runner.execute("SELECT name FROM orders");
        List<MaterializedRow> expected = new ArrayList<>();
        for (String region : runner.regionNames()) {
            expected.addAll(runner.executeOnRegion(region, "SELECT name FROM orders").getMaterializedRows());
        }
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected);

        // the connector tags remote queries with source=trino-federation, so the regional
        // query history shows that only the projected column was requested
        List<String> remoteQueries = runner.executeOnRegion(
                        "east",
                        "SELECT query FROM system.runtime.queries WHERE source = 'trino-federation'")
                .getOnlyColumn()
                .map(String.class::cast)
                .collect(toList());
        assertThat(remoteQueries)
                .anyMatch(query -> query.contains("\"name\"") && query.contains("\"orders\"") && !query.contains("\"price\""));
    }

    @Test
    void testRegionColumn()
    {
        Map<Object, Long> regionCounts = runner.execute("SELECT _region FROM orders").getOnlyColumn()
                .collect(groupingBy(region -> region, counting()));
        assertThat(regionCounts).isEqualTo(Map.of("east", 5L, "west", 5L));

        Map<String, List<Long>> idsByRegion = runner.execute("SELECT id, _region FROM orders").getMaterializedRows().stream()
                .collect(groupingBy(row -> (String) row.getField(1), mapping(row -> (Long) row.getField(0), toList())));
        assertThat(idsByRegion.get("east")).containsExactlyInAnyOrder(1L, 2L, 3L, 4L, 5L);
        assertThat(idsByRegion.get("west")).containsExactlyInAnyOrder(6L, 7L, 8L, 9L, 10L);
    }

    @Test
    void testCountStar()
    {
        assertThat(runner.execute("SELECT count(*) FROM orders").getOnlyValue()).isEqualTo(10L);
        assertThat(runner.execute("SELECT count(*) FROM returns").getOnlyValue()).isEqualTo(3L);
    }

    @Test
    void testValuesRoundTrip()
    {
        MaterializedRow row = getOnlyElement(runner.execute(
                "SELECT name, price, order_date, created, active FROM orders WHERE id = 1").getMaterializedRows());
        assertThat(row.getField(0)).isEqualTo("apple");
        assertThat(row.getField(1)).isEqualTo(new BigDecimal("10.50"));
        assertThat(row.getField(2)).isEqualTo(LocalDate.of(2024, 1, 1));
        assertThat(row.getField(3)).isEqualTo(LocalDateTime.of(2024, 1, 1, 10, 0, 0, 123_000_000));
        assertThat(row.getField(4)).isEqualTo(true);

        MaterializedRow nullRow = getOnlyElement(runner.execute(
                "SELECT price, order_date, created, active FROM orders WHERE id = 3").getMaterializedRows());
        assertThat(nullRow.getFields()).containsExactly(null, null, null, null);
    }

    @Test
    void testEmptyRegionStillUnions()
    {
        MaterializedResult actual = runner.execute("SELECT id, reason, _region FROM returns");
        assertThat(actual.getRowCount()).isEqualTo(3);
        assertThat(actual.getMaterializedRows())
                .allSatisfy(row -> assertThat(row.getField(2)).isEqualTo("east"));
    }

    @Test
    void testUnreachableRegionFailsWithRegionName()
            throws Exception
    {
        try (FederationQueryRunner failing = FederationQueryRunner.builder()
                .addRegion("live")
                .addStaticRegion("down", URI.create("http://127.0.0.1:1"))
                .build()) {
            failing.executeOnRegion("live", "CREATE TABLE events (id BIGINT)");
            failing.executeOnRegion("live", "INSERT INTO events VALUES (1), (2)");
            assertThatThrownBy(() -> failing.execute("SELECT * FROM events"))
                    .hasMessageContaining("Region 'down'");
        }
    }

    @Test
    void testLimitCompletes()
    {
        // LIMIT is not pushed down yet: the engine closes the page sources after the first
        // row, which must cancel the remote regional queries instead of hanging
        assertThat(runner.execute("SELECT id FROM orders LIMIT 1").getRowCount()).isEqualTo(1);
    }
}
