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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import io.airlift.slice.Slice;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Renders the standard Trino SQL sent to regional clusters. Pure string building over
 * engine-free value objects: no ConnectorSession, no ColumnHandle, no side effects.
 */
public final class RemoteSqlBuilder
{
    private static final RemoteColumn DUMMY_COLUMN = new RemoteColumn("$dummy", INTEGER);
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private RemoteSqlBuilder() {}

    public static RemoteQuery buildSql(
            RemoteTable table,
            List<RemoteColumn> projections,
            TupleDomain<RemoteColumn> constraint,
            Optional<AggregationSpec> aggregation,
            Optional<TopNSpec> topN,
            OptionalLong limit)
    {
        requireNonNull(table, "table is null");
        requireNonNull(projections, "projections is null");
        requireNonNull(constraint, "constraint is null");
        requireNonNull(aggregation, "aggregation is null");
        requireNonNull(topN, "topN is null");
        requireNonNull(limit, "limit is null");
        checkArgument(topN.isEmpty() || limit.isEmpty(), "topN and limit are mutually exclusive");
        checkArgument(aggregation.isEmpty() || projections.isEmpty(), "projections must be empty when aggregation is present");

        List<RemoteColumn> outputColumns = outputColumns(projections, aggregation);
        StringBuilder sql = new StringBuilder()
                .append("SELECT ")
                .append(selectList(projections, aggregation))
                .append(" FROM ")
                .append(quoted(table.catalog()))
                .append('.')
                .append(quoted(table.schema()))
                .append('.')
                .append(quoted(table.table()));

        ImmutableSet.Builder<RemoteColumn> unsupportedFilterColumns = ImmutableSet.builder();
        List<String> conjuncts = toConjuncts(constraint, unsupportedFilterColumns);
        if (!conjuncts.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conjuncts));
        }

        if (aggregation.isPresent() && !aggregation.get().groupingColumns().isEmpty()) {
            sql.append(" GROUP BY ").append(aggregation.get().groupingColumns().stream()
                    .map(column -> quoted(column.name()))
                    .collect(joining(", ")));
        }

        if (topN.isPresent()) {
            sql.append(" ORDER BY ").append(topN.get().ordering().stream()
                    .map(RemoteSqlBuilder::sortItem)
                    .collect(joining(", ")));
            sql.append(" LIMIT ").append(topN.get().count());
        }
        else if (limit.isPresent()) {
            sql.append(" LIMIT ").append(limit.orElseThrow());
        }

        return new RemoteQuery(sql.toString(), outputColumns, unsupportedFilterColumns.build());
    }

    private static List<RemoteColumn> outputColumns(List<RemoteColumn> projections, Optional<AggregationSpec> aggregation)
    {
        if (aggregation.isPresent()) {
            ImmutableList.Builder<RemoteColumn> columns = ImmutableList.builder();
            columns.addAll(aggregation.get().groupingColumns());
            aggregation.get().aggregates().forEach(aggregate -> columns.add(aggregate.output()));
            return columns.build();
        }
        if (projections.isEmpty()) {
            return ImmutableList.of(DUMMY_COLUMN);
        }
        return ImmutableList.copyOf(projections);
    }

    private static String selectList(List<RemoteColumn> projections, Optional<AggregationSpec> aggregation)
    {
        if (aggregation.isPresent()) {
            ImmutableList.Builder<String> items = ImmutableList.builder();
            for (RemoteColumn column : aggregation.get().groupingColumns()) {
                items.add(quoted(column.name()));
            }
            for (AggregateSpec aggregate : aggregation.get().aggregates()) {
                items.add(aggregateExpression(aggregate));
            }
            return String.join(", ", items.build());
        }
        if (projections.isEmpty()) {
            return "1 AS " + quoted(DUMMY_COLUMN.name());
        }
        return projections.stream()
                .map(column -> quoted(column.name()))
                .collect(joining(", "));
    }

    private static String aggregateExpression(AggregateSpec aggregate)
    {
        String expression = switch (aggregate.kind()) {
            case COUNT_ALL -> "count(*)";
            case COUNT -> "count(" + quoted(aggregate.argument().orElseThrow().name()) + ")";
            case SUM -> "sum(" + quoted(aggregate.argument().orElseThrow().name()) + ")";
            case MIN -> "min(" + quoted(aggregate.argument().orElseThrow().name()) + ")";
            case MAX -> "max(" + quoted(aggregate.argument().orElseThrow().name()) + ")";
        };
        return expression + " AS " + quoted(aggregate.output().name());
    }

    private static String sortItem(SortSpec sort)
    {
        return quoted(sort.column().name())
                + (sort.ascending() ? " ASC" : " DESC")
                + (sort.nullsFirst() ? " NULLS FIRST" : " NULLS LAST");
    }

    private static List<String> toConjuncts(TupleDomain<RemoteColumn> constraint, ImmutableSet.Builder<RemoteColumn> unsupportedFilterColumns)
    {
        if (constraint.isNone()) {
            return ImmutableList.of("FALSE");
        }
        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();
        List<Map.Entry<RemoteColumn, Domain>> entries = constraint.getDomains().orElseThrow().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(RemoteColumn::name)))
                .collect(toImmutableList());
        for (Map.Entry<RemoteColumn, Domain> entry : entries) {
            RemoteColumn column = entry.getKey();
            Domain domain = entry.getValue();
            if (domain.isAll()) {
                continue;
            }
            if (!isSupportedType(column.type())) {
                unsupportedFilterColumns.add(column);
                continue;
            }
            try {
                conjuncts.add(toPredicate(column, domain));
            }
            catch (UnsupportedPushdownException ignored) {
                unsupportedFilterColumns.add(column);
            }
        }
        return conjuncts.build();
    }

    private static String toPredicate(RemoteColumn column, Domain domain)
    {
        ValueSet values = domain.getValues();
        if (values.isNone()) {
            return domain.isNullAllowed() ? quoted(column.name()) + " IS NULL" : "FALSE";
        }
        if (values.isAll()) {
            checkState(!domain.isNullAllowed(), "all domain must be skipped by the caller");
            return quoted(column.name()) + " IS NOT NULL";
        }
        String predicate = valueSetPredicate(column, values);
        if (domain.isNullAllowed()) {
            return "(" + predicate + " OR " + quoted(column.name()) + " IS NULL)";
        }
        return predicate;
    }

    private static String valueSetPredicate(RemoteColumn column, ValueSet valueSet)
    {
        if (valueSet instanceof EquatableValueSet equatable) {
            List<Object> values = ImmutableList.copyOf(equatable.getValues());
            checkState(!values.isEmpty(), "none and all value sets must be handled by the caller");
            if (equatable.inclusive()) {
                return inPredicate(column, values);
            }
            return notInPredicate(column, values);
        }
        if (!valueSet.isDiscreteSet()) {
            ValueSet complement = valueSet.complement();
            if (complement.isDiscreteSet()) {
                return notInPredicate(column, complement.getDiscreteSet());
            }
        }
        return rangesPredicate(column, valueSet.getRanges().getOrderedRanges());
    }

    private static String rangesPredicate(RemoteColumn column, List<Range> orderedRanges)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();
        for (Range range : orderedRanges) {
            checkState(!range.isAll(), "all range must be handled by the caller");
            if (range.isSingleValue()) {
                singleValues.add(range.getSingleValue());
                continue;
            }
            if (!range.isLowUnbounded() && !range.isHighUnbounded() && range.isLowInclusive() && range.isHighInclusive()) {
                disjuncts.add("%s BETWEEN %s AND %s".formatted(
                        quoted(column.name()),
                        literal(column.type(), range.getLowBoundedValue()),
                        literal(column.type(), range.getHighBoundedValue())));
                continue;
            }
            List<String> rangeConjuncts = new ArrayList<>();
            if (!range.isLowUnbounded()) {
                rangeConjuncts.add(comparison(column, range.isLowInclusive() ? ">=" : ">", range.getLowBoundedValue()));
            }
            if (!range.isHighUnbounded()) {
                rangeConjuncts.add(comparison(column, range.isHighInclusive() ? "<=" : "<", range.getHighBoundedValue()));
            }
            checkState(!rangeConjuncts.isEmpty(), "range with no bounds must be an all range");
            if (rangeConjuncts.size() == 1) {
                disjuncts.add(rangeConjuncts.getFirst());
            }
            else {
                disjuncts.add("(" + String.join(" AND ", rangeConjuncts) + ")");
            }
        }
        if (!singleValues.isEmpty()) {
            disjuncts.add(inPredicate(column, singleValues));
        }
        checkState(!disjuncts.isEmpty(), "value set with no ranges must be a none value set");
        if (disjuncts.size() == 1) {
            return disjuncts.getFirst();
        }
        return "(" + String.join(" OR ", disjuncts) + ")";
    }

    private static String inPredicate(RemoteColumn column, List<Object> values)
    {
        if (values.size() == 1) {
            return comparison(column, "=", values.getFirst());
        }
        return quoted(column.name()) + " IN (" + literals(column.type(), values) + ")";
    }

    private static String notInPredicate(RemoteColumn column, List<Object> values)
    {
        if (values.size() == 1) {
            return comparison(column, "<>", values.getFirst());
        }
        return quoted(column.name()) + " NOT IN (" + literals(column.type(), values) + ")";
    }

    private static String comparison(RemoteColumn column, String operator, Object value)
    {
        return quoted(column.name()) + " " + operator + " " + literal(column.type(), value);
    }

    private static String literals(Type type, List<Object> values)
    {
        return values.stream()
                .map(value -> literal(type, value))
                .collect(joining(", "));
    }

    private static boolean isSupportedType(Type type)
    {
        return type == BOOLEAN
                || type == TINYINT
                || type == SMALLINT
                || type == INTEGER
                || type == BIGINT
                || type == REAL
                || type == DOUBLE
                || type == DATE
                || type == VARBINARY
                || type instanceof DecimalType
                || type instanceof VarcharType
                || type instanceof TimeType
                || type instanceof TimestampType;
    }

    private static String literal(Type type, Object value)
    {
        if (type == BOOLEAN) {
            return (boolean) value ? "TRUE" : "FALSE";
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return String.valueOf((long) value);
        }
        if (type == REAL) {
            float floatValue = intBitsToFloat(toIntExact((long) value));
            if (!Float.isFinite(floatValue)) {
                throw new UnsupportedPushdownException();
            }
            return "REAL '" + floatValue + "'";
        }
        if (type == DOUBLE) {
            double doubleValue = (double) value;
            if (!Double.isFinite(doubleValue)) {
                throw new UnsupportedPushdownException();
            }
            return "DOUBLE '" + doubleValue + "'";
        }
        if (type instanceof DecimalType decimalType) {
            BigDecimal decimal;
            if (decimalType.isShort()) {
                decimal = BigDecimal.valueOf((long) value, decimalType.getScale());
            }
            else {
                decimal = new BigDecimal(((Int128) value).toBigInteger(), decimalType.getScale());
            }
            return "DECIMAL '" + decimal.toPlainString() + "'";
        }
        if (type instanceof VarcharType) {
            return "'" + ((Slice) value).toStringUtf8().replace("'", "''") + "'";
        }
        if (type == VARBINARY) {
            return "X'" + BaseEncoding.base16().encode(((Slice) value).getBytes()) + "'";
        }
        if (type == DATE) {
            return "DATE '" + LocalDate.ofEpochDay((long) value) + "'";
        }
        if (type instanceof TimeType timeType) {
            long picos = (long) value;
            LocalTime time = LocalTime.ofSecondOfDay(picos / PICOSECONDS_PER_SECOND);
            return "TIME '" + TIME_FORMAT.format(time) + fraction(picos % PICOSECONDS_PER_SECOND, timeType.getPrecision()) + "'";
        }
        if (type instanceof TimestampType timestampType) {
            return timestampLiteral(timestampType, value);
        }
        throw new UnsupportedPushdownException();
    }

    private static String timestampLiteral(TimestampType type, Object value)
    {
        long epochMicros;
        int picosOfMicro;
        if (type.isShort()) {
            epochMicros = (long) value;
            picosOfMicro = 0;
        }
        else {
            LongTimestamp timestamp = (LongTimestamp) value;
            epochMicros = timestamp.getEpochMicros();
            picosOfMicro = timestamp.getPicosOfMicro();
        }
        long epochSecond = floorDiv(epochMicros, (long) MICROSECONDS_PER_SECOND);
        long picosOfSecond = floorMod(epochMicros, (long) MICROSECONDS_PER_SECOND) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochSecond, 0, UTC);
        return "TIMESTAMP '" + DATE_TIME_FORMAT.format(dateTime) + fraction(picosOfSecond, type.getPrecision()) + "'";
    }

    private static String fraction(long picosOfSecond, int precision)
    {
        String picosText = "%012d".formatted(picosOfSecond);
        checkState(picosText.substring(precision).chars().allMatch(digit -> digit == '0'),
                "fraction %s has more digits than the type precision %s",
                picosOfSecond,
                precision);
        if (precision == 0) {
            return "";
        }
        return "." + picosText.substring(0, precision);
    }

    private static String quoted(String name)
    {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    private static final class UnsupportedPushdownException
            extends RuntimeException
    {
        public UnsupportedPushdownException()
        {
            super(null, null, false, false);
        }
    }
}
