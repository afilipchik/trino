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

import com.google.common.math.LongMath;
import io.trino.spi.TrinoException;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_TYPE_MISMATCH;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.ENGLISH;

/**
 * Maps remote Trino type names (as reported by the client protocol and by
 * {@code information_schema.columns.data_type}) to Trino {@link Type}s, and converts
 * JSON-decoded protocol values to the stack representation those types expect.
 */
public final class FederationTypeMapper
{
    private static final Pattern TYPE_PATTERN = Pattern.compile(
            "(?<base>[a-z]+(?: [a-z]+)*?)" +
                    "(?:\\((?<precision>\\d{1,9})(?:,\\s*(?<scale>\\d{1,9}))?\\))?" +
                    "(?<zone> with time zone)?");

    private static final Pattern TIME_PATTERN = Pattern.compile(
            "(?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})(?:\\.(?<fraction>\\d{1,12}))?");

    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile(
            "(?<year>[-+]?\\d{4,})-(?<month>\\d{1,2})-(?<day>\\d{1,2})" +
                    " (?<hour>\\d{1,2}):(?<minute>\\d{1,2}):(?<second>\\d{1,2})(?:\\.(?<fraction>\\d{1,12}))?" +
                    "(?: (?<timezone>.+))?");

    private FederationTypeMapper() {}

    /**
     * Returns the Trino type for a remote type name, or empty when the remote type is not
     * supported by the federation connector, so metadata listing can skip such columns.
     */
    public static Optional<Type> toTrinoType(String remoteType)
    {
        Matcher matcher = TYPE_PATTERN.matcher(remoteType.trim().toLowerCase(ENGLISH));
        if (!matcher.matches()) {
            return Optional.empty();
        }
        String base = matcher.group("base");
        Optional<Integer> precision = Optional.ofNullable(matcher.group("precision")).map(Integer::parseInt);
        Optional<Integer> scale = Optional.ofNullable(matcher.group("scale")).map(Integer::parseInt);
        boolean withTimeZone = matcher.group("zone") != null;

        try {
            if (withTimeZone) {
                if (base.equals("timestamp") && scale.isEmpty()) {
                    return Optional.of(createTimestampWithTimeZoneType(precision.orElse(TimestampWithTimeZoneType.DEFAULT_PRECISION)));
                }
                return Optional.empty();
            }
            if (scale.isPresent()) {
                if (base.equals("decimal")) {
                    return Optional.of(createDecimalType(precision.orElseThrow(), scale.get()));
                }
                return Optional.empty();
            }
            if (precision.isPresent()) {
                return switch (base) {
                    case "varchar" -> Optional.of(createVarcharType(precision.get()));
                    case "decimal" -> Optional.of(createDecimalType(precision.get()));
                    case "time" -> Optional.of(createTimeType(precision.get()));
                    case "timestamp" -> Optional.of(createTimestampType(precision.get()));
                    default -> Optional.empty();
                };
            }
            return switch (base) {
                case "boolean" -> Optional.of(BOOLEAN);
                case "tinyint" -> Optional.of(TINYINT);
                case "smallint" -> Optional.of(SMALLINT);
                case "integer" -> Optional.of(INTEGER);
                case "bigint" -> Optional.of(BIGINT);
                case "real" -> Optional.of(REAL);
                case "double" -> Optional.of(DOUBLE);
                case "decimal" -> Optional.of(createDecimalType());
                case "varchar" -> Optional.of(VarcharType.VARCHAR);
                case "varbinary" -> Optional.of(VARBINARY);
                case "date" -> Optional.of(DATE);
                case "time" -> Optional.of(createTimeType(TimeType.DEFAULT_PRECISION));
                case "timestamp" -> Optional.of(createTimestampType(TimestampType.DEFAULT_PRECISION));
                default -> Optional.empty();
            };
        }
        catch (TrinoException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    /**
     * Converts a JSON-decoded protocol value (Boolean, Number, String, or byte[]) to the
     * stack representation of the given Trino type.
     */
    @Nullable
    public static Object toNativeValue(Type type, @Nullable Object value)
    {
        if (value == null) {
            return null;
        }
        try {
            if (type.equals(BOOLEAN)) {
                return (Boolean) value;
            }
            if (type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT)) {
                return ((Number) value).longValue();
            }
            if (type.equals(REAL)) {
                return (long) floatToRawIntBits(((Number) value).floatValue());
            }
            if (type.equals(DOUBLE)) {
                return ((Number) value).doubleValue();
            }
            if (type instanceof DecimalType decimalType) {
                BigDecimal decimal = new BigDecimal((String) value);
                if (decimalType.isShort()) {
                    return encodeShortScaledValue(decimal, decimalType.getScale());
                }
                return encodeScaledValue(decimal, decimalType.getScale());
            }
            if (type instanceof VarcharType) {
                return utf8Slice((String) value);
            }
            if (type.equals(VARBINARY)) {
                return wrappedBuffer((byte[]) value);
            }
            if (type.equals(DATE)) {
                return LocalDate.parse((String) value).toEpochDay();
            }
            if (type instanceof TimeType) {
                return toTimePicos((String) value);
            }
            if (type instanceof TimestampType timestampType) {
                return toTimestamp(timestampType, (String) value);
            }
            if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
                return toTimestampWithTimeZone(timestampWithTimeZoneType, (String) value);
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(FEDERATION_TYPE_MISMATCH, "Failed to convert value '%s' to type %s".formatted(value, type), e);
        }
        throw new TrinoException(FEDERATION_TYPE_MISMATCH, "Unsupported type: " + type);
    }

    private static long toTimePicos(String value)
    {
        Matcher matcher = TIME_PATTERN.matcher(value);
        checkArgument(matcher.matches(), "Invalid time: %s", value);
        long secondsOfDay = (Long.parseLong(matcher.group("hour")) * 60 + Long.parseLong(matcher.group("minute"))) * 60 + Long.parseLong(matcher.group("second"));
        return secondsOfDay * PICOSECONDS_PER_SECOND + fractionPicos(matcher.group("fraction"));
    }

    private static Object toTimestamp(TimestampType type, String value)
    {
        Matcher matcher = TIMESTAMP_PATTERN.matcher(value);
        checkArgument(matcher.matches() && matcher.group("timezone") == null, "Invalid timestamp: %s", value);
        long epochSecond = localDateTime(matcher).toEpochSecond(UTC);
        long picosOfSecond = fractionPicos(matcher.group("fraction"));
        long epochMicros = epochSecond * MICROSECONDS_PER_SECOND + picosOfSecond / PICOSECONDS_PER_MICROSECOND;
        if (type.isShort()) {
            return epochMicros;
        }
        return new LongTimestamp(epochMicros, (int) (picosOfSecond % PICOSECONDS_PER_MICROSECOND));
    }

    private static Object toTimestampWithTimeZone(TimestampWithTimeZoneType type, String value)
    {
        Matcher matcher = TIMESTAMP_PATTERN.matcher(value);
        checkArgument(matcher.matches() && matcher.group("timezone") != null, "Invalid timestamp with time zone: %s", value);
        ZoneId zone = ZoneId.of(matcher.group("timezone"));
        long epochSecond = localDateTime(matcher).atZone(zone).toEpochSecond();
        long picosOfSecond = fractionPicos(matcher.group("fraction"));
        long epochMillis = epochSecond * MILLISECONDS_PER_SECOND + picosOfSecond / PICOSECONDS_PER_MILLISECOND;
        TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey(zone.getId());
        if (type.isShort()) {
            return packDateTimeWithZone(epochMillis, timeZoneKey);
        }
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, (int) (picosOfSecond % PICOSECONDS_PER_MILLISECOND), timeZoneKey);
    }

    private static LocalDateTime localDateTime(Matcher matcher)
    {
        return LocalDateTime.of(
                Integer.parseInt(matcher.group("year")),
                Integer.parseInt(matcher.group("month")),
                Integer.parseInt(matcher.group("day")),
                Integer.parseInt(matcher.group("hour")),
                Integer.parseInt(matcher.group("minute")),
                Integer.parseInt(matcher.group("second")));
    }

    private static long fractionPicos(@Nullable String fraction)
    {
        if (fraction == null) {
            return 0;
        }
        return Long.parseLong(fraction) * LongMath.pow(10, 12 - fraction.length());
    }
}
