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
package io.trino.plugin.federation.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.ColorType.COLOR;
import static java.lang.Float.floatToRawIntBits;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestRemoteSqlBuilder
{
    private static final RemoteTable TABLE = new RemoteTable("remote", "tiny", "orders");
    private static final String FROM_TABLE = " FROM \"remote\".\"tiny\".\"orders\"";

    @Test
    void testPlainProjection()
    {
        RemoteQuery query = buildScan(
                ImmutableList.of(column("a", INTEGER), column("b", VARCHAR)),
                TupleDomain.all());
        assertThat(query.sql()).isEqualTo("SELECT \"a\", \"b\"" + FROM_TABLE);
        assertThat(query.outputColumns()).containsExactly(column("a", INTEGER), column("b", VARCHAR));
        assertThat(query.unsupportedFilterColumns()).isEmpty();
    }

    @Test
    void testEmptyProjections()
    {
        RemoteQuery query = buildScan(ImmutableList.of(), TupleDomain.all());
        assertThat(query.sql()).isEqualTo("SELECT 1 AS \"$dummy\"" + FROM_TABLE);
        assertThat(query.outputColumns()).containsExactly(column("$dummy", INTEGER));
    }

    @Test
    void testIdentifierQuoting()
    {
        RemoteTable table = new RemoteTable("my\"cat", "my\"schema", "my\"table");
        RemoteColumn column = column("col\"umn", INTEGER);
        RemoteQuery query = RemoteSqlBuilder.buildSql(
                table,
                ImmutableList.of(column),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.singleValue(INTEGER, 1L))),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        assertThat(query.sql()).isEqualTo(
                "SELECT \"col\"\"umn\" FROM \"my\"\"cat\".\"my\"\"schema\".\"my\"\"table\" WHERE \"col\"\"umn\" = 1");
    }

    @Test
    void testBooleanLiterals()
    {
        assertSingleValueFilter(BOOLEAN, true, "TRUE");
        assertSingleValueFilter(BOOLEAN, false, "FALSE");
    }

    @Test
    void testIntegerLiterals()
    {
        assertSingleValueFilter(TINYINT, -128L, "-128");
        assertSingleValueFilter(TINYINT, 127L, "127");
        assertSingleValueFilter(SMALLINT, -32768L, "-32768");
        assertSingleValueFilter(SMALLINT, 32767L, "32767");
        assertSingleValueFilter(INTEGER, (long) Integer.MIN_VALUE, "-2147483648");
        assertSingleValueFilter(INTEGER, (long) Integer.MAX_VALUE, "2147483647");
        assertSingleValueFilter(BIGINT, Long.MIN_VALUE, "-9223372036854775808");
        assertSingleValueFilter(BIGINT, Long.MAX_VALUE, "9223372036854775807");
        assertSingleValueFilter(BIGINT, 0L, "0");
    }

    @Test
    void testRealLiterals()
    {
        assertSingleValueFilter(REAL, (long) floatToRawIntBits(1.5f), "REAL '1.5'");
        assertSingleValueFilter(REAL, (long) floatToRawIntBits(-0.125f), "REAL '-0.125'");
        assertSingleValueFilter(REAL, (long) floatToRawIntBits(Float.MAX_VALUE), "REAL '3.4028235E38'");
    }

    @Test
    void testDoubleLiterals()
    {
        assertSingleValueFilter(DOUBLE, 2.5, "DOUBLE '2.5'");
        assertSingleValueFilter(DOUBLE, -0.001, "DOUBLE '-0.001'");
        assertSingleValueFilter(DOUBLE, -1.7976931348623157e308, "DOUBLE '-1.7976931348623157E308'");
    }

    @Test
    void testDecimalLiterals()
    {
        assertSingleValueFilter(createDecimalType(10, 2), 12345L, "DECIMAL '123.45'");
        assertSingleValueFilter(createDecimalType(10, 2), -12345L, "DECIMAL '-123.45'");
        assertSingleValueFilter(createDecimalType(5, 0), 42L, "DECIMAL '42'");
        assertSingleValueFilter(
                createDecimalType(38, 0),
                Int128.valueOf(new BigInteger("9".repeat(38))),
                "DECIMAL '" + "9".repeat(38) + "'");
        assertSingleValueFilter(
                createDecimalType(38, 38),
                Int128.valueOf(BigInteger.valueOf(5)),
                "DECIMAL '0.00000000000000000000000000000000000005'");
        assertSingleValueFilter(
                createDecimalType(30, 2),
                Int128.valueOf(BigInteger.valueOf(-12345)),
                "DECIMAL '-123.45'");
    }

    @Test
    void testVarcharLiterals()
    {
        assertSingleValueFilter(VARCHAR, utf8Slice("O'Brien"), "'O''Brien'");
        assertSingleValueFilter(VARCHAR, utf8Slice("café 中文 ☃"), "'café 中文 ☃'");
        assertSingleValueFilter(VARCHAR, utf8Slice(""), "''");
    }

    @Test
    void testVarbinaryLiteral()
    {
        assertSingleValueFilter(
                VARBINARY,
                wrappedBuffer(new byte[] {(byte) 0xAB, (byte) 0xCD, 0x01}),
                "X'ABCD01'");
    }

    @Test
    void testDateLiterals()
    {
        assertSingleValueFilter(DATE, LocalDate.of(2020, 2, 29).toEpochDay(), "DATE '2020-02-29'");
        assertSingleValueFilter(DATE, LocalDate.of(1950, 3, 14).toEpochDay(), "DATE '1950-03-14'");
        assertSingleValueFilter(DATE, 0L, "DATE '1970-01-01'");
    }

    @Test
    void testTimeLiterals()
    {
        assertSingleValueFilter(createTimeType(0), timePicos(13, 59, 59, 0), "TIME '13:59:59'");
        assertSingleValueFilter(createTimeType(3), timePicos(13, 59, 59, 123_000_000_000L), "TIME '13:59:59.123'");
        assertSingleValueFilter(createTimeType(6), timePicos(0, 0, 0, 123_456_000_000L), "TIME '00:00:00.123456'");
        assertSingleValueFilter(createTimeType(9), timePicos(23, 0, 1, 123_456_789_000L), "TIME '23:00:01.123456789'");
        assertSingleValueFilter(createTimeType(12), timePicos(1, 2, 3, 123_456_789_012L), "TIME '01:02:03.123456789012'");
    }

    @Test
    void testTimestampLiterals()
    {
        assertSingleValueFilter(
                createTimestampType(0),
                epochMicros(LocalDateTime.of(2020, 5, 1, 12, 34, 56), 0),
                "TIMESTAMP '2020-05-01 12:34:56'");
        assertSingleValueFilter(
                createTimestampType(3),
                epochMicros(LocalDateTime.of(2020, 5, 1, 12, 34, 56), 123_000),
                "TIMESTAMP '2020-05-01 12:34:56.123'");
        assertSingleValueFilter(
                createTimestampType(6),
                epochMicros(LocalDateTime.of(2020, 5, 1, 12, 34, 56), 123_456),
                "TIMESTAMP '2020-05-01 12:34:56.123456'");
        assertSingleValueFilter(
                createTimestampType(9),
                new LongTimestamp(epochMicros(LocalDateTime.of(2020, 5, 1, 12, 34, 56), 123_456), 789_000),
                "TIMESTAMP '2020-05-01 12:34:56.123456789'");
        assertSingleValueFilter(
                createTimestampType(3),
                epochMicros(LocalDateTime.of(1950, 6, 15, 1, 2, 3), 500_000),
                "TIMESTAMP '1950-06-15 01:02:03.500'");
    }

    @Test
    void testRangePredicates()
    {
        RemoteColumn column = column("c", INTEGER);
        assertThat(whereClause(column, domain(Range.greaterThan(INTEGER, 5L))))
                .isEqualTo("\"c\" > 5");
        assertThat(whereClause(column, domain(Range.greaterThanOrEqual(INTEGER, 5L))))
                .isEqualTo("\"c\" >= 5");
        assertThat(whereClause(column, domain(Range.lessThan(INTEGER, 5L))))
                .isEqualTo("\"c\" < 5");
        assertThat(whereClause(column, domain(Range.lessThanOrEqual(INTEGER, 5L))))
                .isEqualTo("\"c\" <= 5");
        assertThat(whereClause(column, domain(Range.range(INTEGER, 0L, true, 10L, true))))
                .isEqualTo("\"c\" BETWEEN 0 AND 10");
        assertThat(whereClause(column, domain(Range.range(INTEGER, 0L, false, 10L, true))))
                .isEqualTo("(\"c\" > 0 AND \"c\" <= 10)");
        assertThat(whereClause(column, domain(Range.equal(INTEGER, 5L), Range.greaterThan(INTEGER, 10L))))
                .isEqualTo("(\"c\" > 10 OR \"c\" = 5)");
    }

    @Test
    void testInPredicates()
    {
        RemoteColumn column = column("c", INTEGER);
        assertThat(whereClause(column, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L, 3L))))
                .isEqualTo("\"c\" IN (1, 2, 3)");
        assertThat(whereClause(column, Domain.create(ValueSet.of(INTEGER, 1L, 2L, 3L).complement(), false)))
                .isEqualTo("\"c\" NOT IN (1, 2, 3)");
        assertThat(whereClause(column, Domain.create(ValueSet.of(INTEGER, 5L).complement(), false)))
                .isEqualTo("\"c\" <> 5");
    }

    @Test
    void testNullHandling()
    {
        RemoteColumn column = column("c", INTEGER);
        assertThat(whereClause(column, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 5L)), true)))
                .isEqualTo("(\"c\" > 5 OR \"c\" IS NULL)");
        assertThat(whereClause(column, Domain.onlyNull(INTEGER)))
                .isEqualTo("\"c\" IS NULL");
        assertThat(whereClause(column, Domain.notNull(INTEGER)))
                .isEqualTo("\"c\" IS NOT NULL");
    }

    @Test
    void testAllDomainProducesNoWhereClause()
    {
        RemoteColumn column = column("c", INTEGER);
        RemoteQuery query = buildScan(
                ImmutableList.of(column),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, Domain.all(INTEGER))));
        assertThat(query.sql()).isEqualTo("SELECT \"c\"" + FROM_TABLE);
        assertThat(query.unsupportedFilterColumns()).isEmpty();
    }

    @Test
    void testNoneConstraint()
    {
        RemoteColumn column = column("c", INTEGER);
        RemoteQuery query = buildScan(ImmutableList.of(column), TupleDomain.none());
        assertThat(query.sql()).isEqualTo("SELECT \"c\"" + FROM_TABLE + " WHERE FALSE");
    }

    @Test
    void testConjunctsSortedByColumnName()
    {
        RemoteColumn columnA = column("a", INTEGER);
        RemoteColumn columnB = column("b", INTEGER);
        RemoteQuery query = buildScan(
                ImmutableList.of(columnA, columnB),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        columnB, Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 0L, true, 5L, true)), false),
                        columnA, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L)))));
        assertThat(query.sql()).isEqualTo(
                "SELECT \"a\", \"b\"" + FROM_TABLE + " WHERE \"a\" IN (1, 2) AND \"b\" BETWEEN 0 AND 5");
    }

    @Test
    void testUnsupportedTypeReported()
    {
        RemoteColumn supported = column("a", INTEGER);
        RemoteColumn arrayColumn = column("b", new ArrayType(INTEGER));
        RemoteQuery query = buildScan(
                ImmutableList.of(supported),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        supported, Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, 5L)), false),
                        arrayColumn, Domain.onlyNull(new ArrayType(INTEGER)))));
        assertThat(query.sql()).isEqualTo("SELECT \"a\"" + FROM_TABLE + " WHERE \"a\" > 5");
        assertThat(query.unsupportedFilterColumns()).containsExactly(arrayColumn);
    }

    @Test
    void testEquatableValueSetOnUnsupportedTypeReported()
    {
        RemoteColumn colorColumn = column("c", COLOR);
        RemoteQuery query = buildScan(
                ImmutableList.of(column("a", INTEGER)),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        colorColumn, Domain.create(ValueSet.of(COLOR, 1L, 2L), false))));
        assertThat(query.sql()).isEqualTo("SELECT \"a\"" + FROM_TABLE);
        assertThat(query.unsupportedFilterColumns()).containsExactly(colorColumn);
    }

    @Test
    void testNonFiniteValuesNotPushedDown()
    {
        RemoteColumn doubleColumn = column("d", DOUBLE);
        RemoteQuery query = buildScan(
                ImmutableList.of(doubleColumn),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        doubleColumn, Domain.singleValue(DOUBLE, Double.POSITIVE_INFINITY))));
        assertThat(query.sql()).isEqualTo("SELECT \"d\"" + FROM_TABLE);
        assertThat(query.unsupportedFilterColumns()).containsExactly(doubleColumn);

        query = buildScan(
                ImmutableList.of(doubleColumn),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        doubleColumn, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 1.0, true, Double.POSITIVE_INFINITY, true)), false))));
        assertThat(query.sql()).isEqualTo("SELECT \"d\"" + FROM_TABLE);
        assertThat(query.unsupportedFilterColumns()).containsExactly(doubleColumn);

        RemoteColumn realColumn = column("r", REAL);
        query = buildScan(
                ImmutableList.of(realColumn),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        realColumn, Domain.singleValue(REAL, (long) floatToRawIntBits(Float.NEGATIVE_INFINITY)))));
        assertThat(query.sql()).isEqualTo("SELECT \"r\"" + FROM_TABLE);
        assertThat(query.unsupportedFilterColumns()).containsExactly(realColumn);
    }

    @Test
    void testIsPushableType()
    {
        assertThat(RemoteSqlBuilder.isPushableType(BOOLEAN)).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(BIGINT)).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(REAL)).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(DOUBLE)).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(VARCHAR)).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(VARBINARY)).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(DATE)).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(createDecimalType(10, 2))).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(createTimeType(3))).isTrue();
        assertThat(RemoteSqlBuilder.isPushableType(createTimestampType(3))).isTrue();

        assertThat(RemoteSqlBuilder.isPushableType(new ArrayType(INTEGER))).isFalse();
        assertThat(RemoteSqlBuilder.isPushableType(COLOR)).isFalse();
    }

    @Test
    void testIsPushableDomainMatchesBuildSql()
    {
        // domains buildSql renders as WHERE conjuncts
        assertPushability(INTEGER, Domain.multipleValues(INTEGER, ImmutableList.of(1L, 2L)), true);
        assertPushability(INTEGER, domain(Range.greaterThan(INTEGER, 5L)), true);
        assertPushability(INTEGER, Domain.onlyNull(INTEGER), true);
        assertPushability(INTEGER, Domain.notNull(INTEGER), true);
        assertPushability(VARCHAR, Domain.singleValue(VARCHAR, utf8Slice("x")), true);
        assertPushability(DOUBLE, Domain.singleValue(DOUBLE, 1.5), true);

        // domains buildSql reports as unsupported filter columns
        assertPushability(new ArrayType(INTEGER), Domain.onlyNull(new ArrayType(INTEGER)), false);
        assertPushability(COLOR, Domain.create(ValueSet.of(COLOR, 1L), false), false);
        assertPushability(DOUBLE, Domain.singleValue(DOUBLE, Double.POSITIVE_INFINITY), false);
        assertPushability(DOUBLE, domain(Range.range(DOUBLE, 1.0, true, Double.POSITIVE_INFINITY, true)), false);
        assertPushability(REAL, Domain.singleValue(REAL, (long) floatToRawIntBits(Float.NEGATIVE_INFINITY)), false);
    }

    @Test
    void testAllDomainIsPushable()
    {
        assertThat(RemoteSqlBuilder.isPushableDomain(INTEGER, Domain.all(INTEGER))).isTrue();
        assertThat(RemoteSqlBuilder.isPushableDomain(new ArrayType(INTEGER), Domain.all(new ArrayType(INTEGER)))).isTrue();
    }

    private static void assertPushability(Type type, Domain domain, boolean expected)
    {
        assertThat(RemoteSqlBuilder.isPushableDomain(type, domain)).isEqualTo(expected);
        RemoteColumn column = column("c", type);
        RemoteQuery query = buildScan(
                ImmutableList.of(column("a", INTEGER)),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        assertThat(query.unsupportedFilterColumns().isEmpty()).isEqualTo(expected);
    }

    @Test
    void testGlobalAggregation()
    {
        RemoteQuery query = buildAggregation(new AggregationSpec(
                ImmutableList.of(),
                ImmutableList.of(new AggregateSpec(AggregateKind.COUNT_ALL, Optional.empty(), column("cnt", BIGINT)))));
        assertThat(query.sql()).isEqualTo("SELECT count(*) AS \"cnt\"" + FROM_TABLE);
        assertThat(query.outputColumns()).containsExactly(column("cnt", BIGINT));
    }

    @Test
    void testGroupedAggregation()
    {
        RemoteColumn region = column("region", VARCHAR);
        RemoteColumn id = column("id", BIGINT);
        RemoteColumn price = column("price", DOUBLE);
        RemoteQuery query = buildAggregation(new AggregationSpec(
                ImmutableList.of(region),
                ImmutableList.of(
                        new AggregateSpec(AggregateKind.COUNT_ALL, Optional.empty(), column("cnt", BIGINT)),
                        new AggregateSpec(AggregateKind.COUNT, Optional.of(id), column("id_count", BIGINT)),
                        new AggregateSpec(AggregateKind.SUM, Optional.of(price), column("price_sum", DOUBLE)),
                        new AggregateSpec(AggregateKind.MIN, Optional.of(price), column("price_min", DOUBLE)),
                        new AggregateSpec(AggregateKind.MAX, Optional.of(price), column("price_max", DOUBLE)))));
        assertThat(query.sql()).isEqualTo(
                "SELECT \"region\", count(*) AS \"cnt\", count(\"id\") AS \"id_count\", sum(\"price\") AS \"price_sum\", "
                        + "min(\"price\") AS \"price_min\", max(\"price\") AS \"price_max\""
                        + FROM_TABLE + " GROUP BY \"region\"");
        assertThat(query.outputColumns()).containsExactly(
                region,
                column("cnt", BIGINT),
                column("id_count", BIGINT),
                column("price_sum", DOUBLE),
                column("price_min", DOUBLE),
                column("price_max", DOUBLE));
    }

    @Test
    void testGroupingOnlyAggregation()
    {
        RemoteColumn region = column("region", VARCHAR);
        RemoteQuery query = buildAggregation(new AggregationSpec(ImmutableList.of(region), ImmutableList.of()));
        assertThat(query.sql()).isEqualTo("SELECT \"region\"" + FROM_TABLE + " GROUP BY \"region\"");
        assertThat(query.outputColumns()).containsExactly(region);
    }

    @Test
    void testAggregationWithConstraint()
    {
        RemoteColumn region = column("region", VARCHAR);
        RemoteColumn price = column("price", DOUBLE);
        RemoteQuery query = RemoteSqlBuilder.buildSql(
                TABLE,
                ImmutableList.of(),
                TupleDomain.withColumnDomains(ImmutableMap.of(region, Domain.singleValue(VARCHAR, utf8Slice("us")))),
                Optional.of(new AggregationSpec(
                        ImmutableList.of(region),
                        ImmutableList.of(new AggregateSpec(AggregateKind.SUM, Optional.of(price), column("s", DOUBLE))))),
                Optional.empty(),
                OptionalLong.empty());
        assertThat(query.sql()).isEqualTo(
                "SELECT \"region\", sum(\"price\") AS \"s\"" + FROM_TABLE + " WHERE \"region\" = 'us' GROUP BY \"region\"");
    }

    @Test
    void testAggregationRejectsProjections()
    {
        assertThatThrownBy(() -> RemoteSqlBuilder.buildSql(
                TABLE,
                ImmutableList.of(column("a", INTEGER)),
                TupleDomain.all(),
                Optional.of(new AggregationSpec(
                        ImmutableList.of(),
                        ImmutableList.of(new AggregateSpec(AggregateKind.COUNT_ALL, Optional.empty(), column("cnt", BIGINT))))),
                Optional.empty(),
                OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("projections must be empty");
    }

    @Test
    void testAggregateSpecValidation()
    {
        assertThatThrownBy(() -> new AggregateSpec(AggregateKind.COUNT_ALL, Optional.of(column("x", BIGINT)), column("cnt", BIGINT)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AggregateSpec(AggregateKind.SUM, Optional.empty(), column("s", BIGINT)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTopN()
    {
        RemoteColumn columnA = column("a", INTEGER);
        RemoteColumn columnB = column("b", VARCHAR);
        RemoteQuery query = RemoteSqlBuilder.buildSql(
                TABLE,
                ImmutableList.of(columnA, columnB),
                TupleDomain.all(),
                Optional.empty(),
                Optional.of(new TopNSpec(
                        ImmutableList.of(
                                new SortSpec(columnA, true, true),
                                new SortSpec(columnB, false, false)),
                        10)),
                OptionalLong.empty());
        assertThat(query.sql()).isEqualTo(
                "SELECT \"a\", \"b\"" + FROM_TABLE + " ORDER BY \"a\" ASC NULLS FIRST, \"b\" DESC NULLS LAST LIMIT 10");
    }

    @Test
    void testSortSpecFromSortOrder()
    {
        RemoteColumn column = column("c", INTEGER);
        assertThat(SortSpec.of(column, ASC_NULLS_LAST)).isEqualTo(new SortSpec(column, true, false));
        assertThat(SortSpec.of(column, DESC_NULLS_FIRST)).isEqualTo(new SortSpec(column, false, true));
    }

    @Test
    void testLimit()
    {
        RemoteColumn column = column("c", INTEGER);
        RemoteQuery query = RemoteSqlBuilder.buildSql(
                TABLE,
                ImmutableList.of(column),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(5));
        assertThat(query.sql()).isEqualTo("SELECT \"c\"" + FROM_TABLE + " LIMIT 5");
    }

    @Test
    void testTopNAndLimitAreExclusive()
    {
        RemoteColumn column = column("c", INTEGER);
        assertThatThrownBy(() -> RemoteSqlBuilder.buildSql(
                TABLE,
                ImmutableList.of(column),
                TupleDomain.all(),
                Optional.empty(),
                Optional.of(new TopNSpec(ImmutableList.of(new SortSpec(column, true, true)), 10)),
                OptionalLong.of(5)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    void testTopNSpecValidation()
    {
        RemoteColumn column = column("c", INTEGER);
        assertThatThrownBy(() -> new TopNSpec(ImmutableList.of(), 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TopNSpec(ImmutableList.of(new SortSpec(column, true, true)), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static RemoteColumn column(String name, Type type)
    {
        return new RemoteColumn(name, type);
    }

    private static RemoteQuery buildScan(List<RemoteColumn> projections, TupleDomain<RemoteColumn> constraint)
    {
        return RemoteSqlBuilder.buildSql(TABLE, projections, constraint, Optional.empty(), Optional.empty(), OptionalLong.empty());
    }

    private static RemoteQuery buildAggregation(AggregationSpec aggregation)
    {
        return RemoteSqlBuilder.buildSql(TABLE, ImmutableList.of(), TupleDomain.all(), Optional.of(aggregation), Optional.empty(), OptionalLong.empty());
    }

    private static Domain domain(Range first, Range... rest)
    {
        return Domain.create(ValueSet.ofRanges(first, rest), false);
    }

    private static void assertSingleValueFilter(Type type, Object value, String expectedLiteral)
    {
        RemoteColumn column = column("c", type);
        assertThat(whereClause(column, Domain.singleValue(type, value)))
                .isEqualTo("\"c\" = " + expectedLiteral);
    }

    private static String whereClause(RemoteColumn column, Domain domain)
    {
        RemoteQuery query = buildScan(
                ImmutableList.of(column),
                TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)));
        assertThat(query.unsupportedFilterColumns()).isEmpty();
        String prefix = "SELECT " + quoted(column.name()) + FROM_TABLE + " WHERE ";
        assertThat(query.sql()).startsWith(prefix);
        return query.sql().substring(prefix.length());
    }

    private static String quoted(String name)
    {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    private static long timePicos(int hour, int minute, int second, long picosOfSecond)
    {
        return ((((hour * 60L) + minute) * 60) + second) * PICOSECONDS_PER_SECOND + picosOfSecond;
    }

    private static long epochMicros(LocalDateTime dateTime, long microsOfSecond)
    {
        return dateTime.toEpochSecond(UTC) * 1_000_000 + microsOfSecond;
    }
}
