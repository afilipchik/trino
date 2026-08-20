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
package io.trino.plugin.federation.client;

import io.airlift.units.Duration;
import io.trino.plugin.federation.FederationConfig;
import io.trino.plugin.federation.Region;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.testing.DistributedQueryRunner;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigInteger;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_REGION_UNREACHABLE;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_REMOTE_ERROR;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_TYPE_MISMATCH;
import static io.trino.plugin.federation.client.FederationTypeMapper.toTrinoType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.lang.Float.floatToRawIntBits;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestRegionClient
{
    private DistributedQueryRunner queryRunner;
    private OkHttpClient httpClient;
    private URI baseUrl;
    private RegionClient tpchClient;
    private RegionClient memoryClient;

    @BeforeAll
    void setUp()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog("tpch").setSchema("tiny").build())
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");
        queryRunner.execute("CREATE TABLE memory.default.described (id integer, name varchar(10), tags array(integer), created timestamp(6))");

        httpClient = new OkHttpClient();
        baseUrl = queryRunner.getCoordinator().getBaseUrl();
        tpchClient = regionClient("tpch");
        memoryClient = regionClient("memory");
    }

    @AfterAll
    void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
        if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdownNow();
            httpClient.connectionPool().evictAll();
            httpClient = null;
        }
    }

    @Test
    void testExecuteScalarRoundTrip()
    {
        assertValue("true", BOOLEAN, true);
        assertValue("false", BOOLEAN, false);

        assertValue("TINYINT '42'", TINYINT, 42L);
        assertValue("TINYINT '-7'", TINYINT, -7L);
        assertValue("SMALLINT '32000'", SMALLINT, 32000L);
        assertValue("12345", INTEGER, 12345L);
        assertValue("-1", INTEGER, -1L);
        assertValue("0", INTEGER, 0L);
        assertValue("BIGINT '123456789012'", BIGINT, 123456789012L);

        assertValue("REAL '3.5'", REAL, (long) floatToRawIntBits(3.5f));
        assertValue("REAL '-0.5'", REAL, (long) floatToRawIntBits(-0.5f));
        assertValue("REAL '0.0'", REAL, (long) floatToRawIntBits(0.0f));
        assertValue("DOUBLE '2.25'", DOUBLE, 2.25d);
        assertValue("DOUBLE '-2.5'", DOUBLE, -2.5d);
        assertValue("DOUBLE '0.0'", DOUBLE, 0.0d);

        assertValue("DECIMAL '123.45'", createDecimalType(5, 2), 12345L);
        assertValue("DECIMAL '-123.45'", createDecimalType(5, 2), -12345L);
        assertValue(
                "CAST('12345678901234567890.12' AS decimal(30,2))",
                createDecimalType(30, 2),
                Int128.valueOf(new BigInteger("1234567890123456789012")));

        assertValue("CAST('héllo 🚀' AS varchar)", VARCHAR, utf8Slice("héllo 🚀"));
        assertValue("CAST('abc' AS varchar(10))", createVarcharType(10), utf8Slice("abc"));
        assertValue("X'0102FF'", VARBINARY, wrappedBuffer(new byte[] {1, 2, (byte) 0xFF}));

        assertValue("DATE '2020-05-01'", DATE, LocalDate.of(2020, 5, 1).toEpochDay());
        assertValue("DATE '1969-12-31'", DATE, -1L);

        assertValue("TIME '01:02:03.123'", createTimeType(3), 3723 * PICOSECONDS_PER_SECOND + 123_000_000_000L);
        assertValue("TIME '23:59:59'", createTimeType(0), 86399 * PICOSECONDS_PER_SECOND);

        long epochSecond = LocalDateTime.of(2020, 5, 1, 12, 34, 56).toEpochSecond(UTC);
        assertValue("TIMESTAMP '2020-05-01 12:34:56'", createTimestampType(0), epochSecond * MICROSECONDS_PER_SECOND);
        assertValue("TIMESTAMP '2020-05-01 12:34:56.123'", createTimestampType(3), epochSecond * MICROSECONDS_PER_SECOND + 123_000);
        assertValue("TIMESTAMP '2020-05-01 12:34:56.123456'", createTimestampType(6), epochSecond * MICROSECONDS_PER_SECOND + 123_456);
        assertValue(
                "TIMESTAMP '2020-05-01 12:34:56.123456789'",
                createTimestampType(9),
                new LongTimestamp(epochSecond * MICROSECONDS_PER_SECOND + 123_456, 789_000));

        assertValue(
                "TIMESTAMP '2020-05-01 12:34:56.123 UTC'",
                createTimestampWithTimeZoneType(3),
                packDateTimeWithZone(epochSecond * MILLISECONDS_PER_SECOND + 123, getTimeZoneKey("UTC")));

        long losAngelesEpochSecond = LocalDateTime.of(2020, 5, 1, 12, 34, 56).atZone(ZoneId.of("America/Los_Angeles")).toEpochSecond();
        assertValue(
                "TIMESTAMP '2020-05-01 12:34:56.123456 America/Los_Angeles'",
                createTimestampWithTimeZoneType(6),
                LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                        losAngelesEpochSecond * MILLISECONDS_PER_SECOND + 123,
                        456_000_000,
                        getTimeZoneKey("America/Los_Angeles")));
    }

    @Test
    void testExecuteNulls()
    {
        List<String> typeNames = List.of(
                "boolean",
                "tinyint",
                "smallint",
                "integer",
                "bigint",
                "real",
                "double",
                "decimal(5,2)",
                "decimal(30,2)",
                "varchar",
                "varchar(10)",
                "varbinary",
                "date",
                "time(3)",
                "timestamp(0)",
                "timestamp(3)",
                "timestamp(6)",
                "timestamp(9)",
                "timestamp(3) with time zone",
                "timestamp(6) with time zone");
        for (String typeName : typeNames) {
            Type type = toTrinoType(typeName).orElseThrow();
            assertValue("CAST(NULL AS %s)".formatted(typeName), type, null);
        }
    }

    @Test
    void testExecuteStreamsMultiplePages()
    {
        int count = 0;
        long orderKeySum = 0;
        try (RegionQueryResults results = tpchClient.execute("SELECT orderkey, orderdate FROM tpch.tiny.orders")) {
            assertThat(results.columns()).hasSize(2);
            assertThat(results.columns().get(0).name()).isEqualTo("orderkey");
            assertThat(results.types()).containsExactly(BIGINT, DATE);
            while (results.hasNext()) {
                List<Object> row = results.next();
                orderKeySum += (Long) row.get(0);
                count++;
            }
        }
        assertThat(count).isEqualTo(15000);
        assertThat(orderKeySum).isGreaterThan(0);
    }

    @Test
    void testListSchemas()
    {
        assertThat(tpchClient.listSchemas()).contains("tiny", "sf1", "information_schema");
        assertThat(memoryClient.listSchemas()).contains("default");
    }

    @Test
    void testListTables()
    {
        List<SchemaTableName> tinyTables = tpchClient.listTables(Optional.of("tiny"));
        assertThat(tinyTables).contains(new SchemaTableName("tiny", "nation"), new SchemaTableName("tiny", "orders"));
        assertThat(tinyTables).allMatch(table -> table.getSchemaName().equals("tiny"));

        assertThat(tpchClient.listTables(Optional.empty()))
                .contains(new SchemaTableName("tiny", "nation"), new SchemaTableName("sf1", "nation"));

        assertThat(memoryClient.listTables(Optional.of("default")))
                .contains(new SchemaTableName("default", "described"));
    }

    @Test
    void testDescribeTable()
    {
        assertThat(tpchClient.describeTable("tiny", "nation")).containsExactly(
                new RemoteColumn("nationkey", "bigint", Optional.of(BIGINT)),
                new RemoteColumn("name", "varchar(25)", Optional.of(createVarcharType(25))),
                new RemoteColumn("regionkey", "bigint", Optional.of(BIGINT)),
                new RemoteColumn("comment", "varchar(152)", Optional.of(createVarcharType(152))));

        assertThat(memoryClient.describeTable("default", "described")).containsExactly(
                new RemoteColumn("id", "integer", Optional.of(INTEGER)),
                new RemoteColumn("name", "varchar(10)", Optional.of(createVarcharType(10))),
                new RemoteColumn("tags", "array(integer)", Optional.empty()),
                new RemoteColumn("created", "timestamp(6)", Optional.of(createTimestampType(6))));

        assertThat(memoryClient.describeTable("default", "missing_table")).isEmpty();
    }

    @Test
    void testBadSqlFailsWithRemoteError()
    {
        assertTrinoExceptionThrownBy(() -> tpchClient.execute("SELECT bad_column FROM tpch.tiny.nation"))
                .hasErrorCode(FEDERATION_REMOTE_ERROR)
                .hasMessageContaining("region-a")
                .hasMessageContaining("bad_column");
    }

    @Test
    void testUnsupportedResultTypeFailsExecute()
    {
        assertTrinoExceptionThrownBy(() -> tpchClient.execute("SELECT ARRAY[1]"))
                .hasErrorCode(FEDERATION_TYPE_MISMATCH)
                .hasMessageContaining("region-a")
                .hasMessageContaining("array(integer)");
    }

    @Test
    void testUnreachableRegion()
    {
        RegionClient unreachable = new RegionClient(
                httpClient,
                new Region("region-x", URI.create("http://127.0.0.1:1")),
                "tpch",
                "federation-test",
                new Duration(10, SECONDS));
        assertTrinoExceptionThrownBy(() -> unreachable.execute("SELECT 1"))
                .hasErrorCode(FEDERATION_REGION_UNREACHABLE)
                .hasMessageContaining("region-x");
        assertTrinoExceptionThrownBy(unreachable::listSchemas)
                .hasErrorCode(FEDERATION_REGION_UNREACHABLE)
                .hasMessageContaining("region-x");
    }

    @Test
    void testCloseCancelsRemoteQuery()
    {
        RegionQueryResults results = tpchClient.execute("SELECT l1.orderkey FROM tpch.tiny.lineitem l1, tpch.tiny.lineitem l2");
        assertThat(results.hasNext()).isTrue();
        assertThat(results.next()).hasSize(1);
        results.close();

        assertEventually(new Duration(30, SECONDS), () -> {
            long runningQueries = (Long) queryRunner.execute(
                            "SELECT count(*) FROM system.runtime.queries " +
                                    "WHERE state = 'RUNNING' AND query LIKE '%lineitem l2%' AND query NOT LIKE '%system.runtime%'")
                    .getOnlyValue();
            assertThat(runningQueries).isEqualTo(0L);
        });
    }

    @Test
    void testRegionClients()
    {
        FederationConfig config = new FederationConfig()
                .setRegions("a=%s,b=%s".formatted(baseUrl, baseUrl))
                .setRemoteCatalog("tpch");
        try (RegionClients clients = new RegionClients(config)) {
            assertThat(clients.clients()).extracting(RegionClient::regionName).containsExactly("a", "b");
            assertThat(clients.client("b").regionName()).isEqualTo("b");
            assertThat(clients.client("a").listSchemas()).contains("tiny");
            assertThatThrownBy(() -> clients.client("missing"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unknown region: missing");
        }
    }

    private RegionClient regionClient(String remoteCatalog)
    {
        return new RegionClient(httpClient, new Region("region-a", baseUrl), remoteCatalog, "federation-test", new Duration(2, MINUTES));
    }

    private void assertValue(String expression, Type expectedType, Object expectedValue)
    {
        try (RegionQueryResults results = tpchClient.execute("SELECT " + expression)) {
            assertThat(results.columns()).hasSize(1);
            assertThat(results.columns().get(0).type()).isEqualTo(expectedType);
            assertThat(results.hasNext()).isTrue();
            List<Object> row = results.next();
            assertThat(row).hasSize(1);
            if (expectedValue == null) {
                assertThat(row.get(0)).isNull();
            }
            else {
                assertThat(row.get(0)).isEqualTo(expectedValue);
            }
            assertThat(results.hasNext()).isFalse();
        }
    }
}
