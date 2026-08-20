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

import io.trino.spi.TrinoException;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.federation.client.FederationTypeMapper.toNativeValue;
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
import static java.lang.Float.floatToRawIntBits;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestFederationTypeMapper
{
    @Test
    void testMapSupportedTypes()
    {
        assertThat(toTrinoType("boolean")).contains(BOOLEAN);
        assertThat(toTrinoType("tinyint")).contains(TINYINT);
        assertThat(toTrinoType("smallint")).contains(SMALLINT);
        assertThat(toTrinoType("integer")).contains(INTEGER);
        assertThat(toTrinoType("bigint")).contains(BIGINT);
        assertThat(toTrinoType("real")).contains(REAL);
        assertThat(toTrinoType("double")).contains(DOUBLE);
        assertThat(toTrinoType("decimal")).contains(createDecimalType());
        assertThat(toTrinoType("decimal(10)")).contains(createDecimalType(10));
        assertThat(toTrinoType("decimal(10,2)")).contains(createDecimalType(10, 2));
        assertThat(toTrinoType("decimal(38, 10)")).contains(createDecimalType(38, 10));
        assertThat(toTrinoType("varchar")).contains(VARCHAR);
        assertThat(toTrinoType("varchar(42)")).contains(createVarcharType(42));
        assertThat(toTrinoType("varbinary")).contains(VARBINARY);
        assertThat(toTrinoType("date")).contains(DATE);
        assertThat(toTrinoType("time")).contains(createTimeType(3));
        assertThat(toTrinoType("time(6)")).contains(createTimeType(6));
        assertThat(toTrinoType("timestamp")).contains(createTimestampType(3));
        assertThat(toTrinoType("timestamp(0)")).contains(createTimestampType(0));
        assertThat(toTrinoType("timestamp(9)")).contains(createTimestampType(9));
        assertThat(toTrinoType("timestamp with time zone")).contains(createTimestampWithTimeZoneType(3));
        assertThat(toTrinoType("timestamp(6) with time zone")).contains(createTimestampWithTimeZoneType(6));
    }

    @Test
    void testUnsupportedTypesAreUnmapped()
    {
        assertThat(toTrinoType("array(integer)")).isEmpty();
        assertThat(toTrinoType("map(varchar, integer)")).isEmpty();
        assertThat(toTrinoType("row(x integer, y varchar)")).isEmpty();
        assertThat(toTrinoType("char(5)")).isEmpty();
        assertThat(toTrinoType("json")).isEmpty();
        assertThat(toTrinoType("uuid")).isEmpty();
        assertThat(toTrinoType("ipaddress")).isEmpty();
        assertThat(toTrinoType("hyperloglog")).isEmpty();
        assertThat(toTrinoType("interval day to second")).isEmpty();
        assertThat(toTrinoType("interval year to month")).isEmpty();
        assertThat(toTrinoType("time(3) with time zone")).isEmpty();
        assertThat(toTrinoType("timestamp(13)")).isEmpty();
        assertThat(toTrinoType("decimal(39,2)")).isEmpty();
        assertThat(toTrinoType("integer(5)")).isEmpty();
        assertThat(toTrinoType("not a type")).isEmpty();
        assertThat(toTrinoType("")).isEmpty();
    }

    @Test
    void testConvertNull()
    {
        assertThat(toNativeValue(BIGINT, null)).isNull();
        assertThat(toNativeValue(VARCHAR, null)).isNull();
    }

    @Test
    void testConvertScalars()
    {
        assertThat(toNativeValue(BOOLEAN, true)).isEqualTo(true);
        assertThat(toNativeValue(TINYINT, (byte) -7)).isEqualTo(-7L);
        assertThat(toNativeValue(SMALLINT, (short) 32000)).isEqualTo(32000L);
        assertThat(toNativeValue(INTEGER, 12345)).isEqualTo(12345L);
        assertThat(toNativeValue(BIGINT, 123456789012L)).isEqualTo(123456789012L);
        assertThat(toNativeValue(REAL, 3.5f)).isEqualTo((long) floatToRawIntBits(3.5f));
        assertThat(toNativeValue(DOUBLE, 2.25d)).isEqualTo(2.25d);
        assertThat(toNativeValue(VARCHAR, "héllo 🚀")).isEqualTo(utf8Slice("héllo 🚀"));
        assertThat(toNativeValue(VARBINARY, new byte[] {1, 2, (byte) 0xFF})).isEqualTo(wrappedBuffer(new byte[] {1, 2, (byte) 0xFF}));
    }

    @Test
    void testConvertDecimals()
    {
        assertThat(toNativeValue(createDecimalType(5, 2), "123.45")).isEqualTo(12345L);
        assertThat(toNativeValue(createDecimalType(5, 2), "-123.45")).isEqualTo(-12345L);
        assertThat(toNativeValue(createDecimalType(30, 2), "12345678901234567890.12"))
                .isEqualTo(Int128.valueOf(new BigInteger("1234567890123456789012")));
    }

    @Test
    void testConvertTemporals()
    {
        assertThat(toNativeValue(DATE, "2020-05-01")).isEqualTo(LocalDate.of(2020, 5, 1).toEpochDay());
        assertThat(toNativeValue(DATE, "1969-12-31")).isEqualTo(-1L);

        assertThat(toNativeValue(createTimeType(3), "01:02:03.123"))
                .isEqualTo(3723 * PICOSECONDS_PER_SECOND + 123_000_000_000L);
        assertThat(toNativeValue(createTimeType(0), "23:59:59"))
                .isEqualTo(86399 * PICOSECONDS_PER_SECOND);

        long epochSecond = LocalDateTime.of(2020, 5, 1, 12, 34, 56).toEpochSecond(UTC);
        assertThat(toNativeValue(createTimestampType(0), "2020-05-01 12:34:56"))
                .isEqualTo(epochSecond * MICROSECONDS_PER_SECOND);
        assertThat(toNativeValue(createTimestampType(6), "2020-05-01 12:34:56.123456"))
                .isEqualTo(epochSecond * MICROSECONDS_PER_SECOND + 123456);
        assertThat(toNativeValue(createTimestampType(9), "2020-05-01 12:34:56.123456789"))
                .isEqualTo(new LongTimestamp(epochSecond * MICROSECONDS_PER_SECOND + 123456, 789_000));

        assertThat(toNativeValue(createTimestampWithTimeZoneType(3), "2020-05-01 12:34:56.123 UTC"))
                .isEqualTo(packDateTimeWithZone(epochSecond * MILLISECONDS_PER_SECOND + 123, getTimeZoneKey("UTC")));

        long losAngelesEpochSecond = LocalDateTime.of(2020, 5, 1, 12, 34, 56).atZone(ZoneId.of("America/Los_Angeles")).toEpochSecond();
        assertThat(toNativeValue(createTimestampWithTimeZoneType(6), "2020-05-01 12:34:56.123456 America/Los_Angeles"))
                .isEqualTo(LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                        losAngelesEpochSecond * MILLISECONDS_PER_SECOND + 123,
                        456_000_000,
                        getTimeZoneKey("America/Los_Angeles")));
    }

    @Test
    void testConvertInvalidValue()
    {
        assertThatThrownBy(() -> toNativeValue(DATE, "not a date"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to convert value");
        assertThatThrownBy(() -> toNativeValue(BIGINT, "not a number"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to convert value");
    }
}
