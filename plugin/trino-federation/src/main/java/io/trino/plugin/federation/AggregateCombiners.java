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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.federation.sql.AggregateKind;
import io.trino.plugin.federation.sql.AggregateSpec;
import io.trino.plugin.federation.sql.RemoteColumn;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;

/**
 * Maps each {@link FederationAggregateColumn} to its remote partial aggregates and to the
 * {@link Combiner} merging the per-region partial values into the final result. Both sides
 * live here so the partial column layout and the combiner input layout cannot diverge.
 */
public final class AggregateCombiners
{
    private static final BigInteger MAX_UNSCALED_DECIMAL = BigInteger.TEN.pow(MAX_PRECISION).subtract(BigInteger.ONE);

    private AggregateCombiners() {}

    /**
     * The partial aggregates the regions compute for this column, in the channel order the
     * matching combiner reads them.
     */
    public static List<AggregateSpec> remotePartials(FederationAggregateColumn column)
    {
        Optional<RemoteColumn> argument = column.argument().map(handle -> new RemoteColumn(handle.name(), handle.type()));
        return switch (column.combineKind()) {
            case COUNT_SUM -> ImmutableList.of(new AggregateSpec(
                    argument.isEmpty() ? AggregateKind.COUNT_ALL : AggregateKind.COUNT,
                    argument,
                    new RemoteColumn(column.outputName(), BIGINT)));
            case SUM_LONG -> sumPartial(column, argument, BIGINT);
            case SUM_DOUBLE -> sumPartial(column, argument, DOUBLE);
            case SUM_REAL -> sumPartial(column, argument, REAL);
            case SUM_DECIMAL -> sumPartial(column, argument, column.outputType());
            case MIN -> ImmutableList.of(new AggregateSpec(AggregateKind.MIN, argument, new RemoteColumn(column.outputName(), column.outputType())));
            case MAX -> ImmutableList.of(new AggregateSpec(AggregateKind.MAX, argument, new RemoteColumn(column.outputName(), column.outputType())));
            case AVG_DOUBLE -> avgPartials(column, argument, DOUBLE);
            case AVG_REAL -> avgPartials(column, argument, REAL);
        };
    }

    public static Supplier<Combiner> combinerFactory(FederationAggregateColumn column, int channel, TypeOperators typeOperators)
    {
        return switch (column.combineKind()) {
            case COUNT_SUM -> () -> new CountSumCombiner(channel);
            case SUM_LONG -> () -> new LongSumCombiner(channel);
            case SUM_DOUBLE -> () -> new DoubleSumCombiner(channel);
            case SUM_REAL -> () -> new RealSumCombiner(channel);
            case SUM_DECIMAL -> () -> new DecimalSumCombiner(channel);
            case MIN -> {
                MethodHandle comparison = typeOperators.getComparisonUnorderedLastOperator(
                        column.outputType(),
                        simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
                yield () -> new MinMaxCombiner(channel, comparison, true);
            }
            case MAX -> {
                MethodHandle comparison = typeOperators.getComparisonUnorderedFirstOperator(
                        column.outputType(),
                        simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
                yield () -> new MinMaxCombiner(channel, comparison, false);
            }
            case AVG_DOUBLE -> () -> new AverageCombiner(channel, false);
            case AVG_REAL -> () -> new AverageCombiner(channel, true);
        };
    }

    private static List<AggregateSpec> sumPartial(FederationAggregateColumn column, Optional<RemoteColumn> argument, Type partialType)
    {
        return ImmutableList.of(new AggregateSpec(AggregateKind.SUM, argument, new RemoteColumn(column.outputName(), partialType)));
    }

    private static List<AggregateSpec> avgPartials(FederationAggregateColumn column, Optional<RemoteColumn> argument, Type sumType)
    {
        return ImmutableList.of(
                new AggregateSpec(AggregateKind.SUM, argument, new RemoteColumn(column.outputName() + "$sum", sumType)),
                new AggregateSpec(AggregateKind.COUNT, argument, new RemoteColumn(column.outputName() + "$count", BIGINT)));
    }

    /**
     * Merges the partial values of one aggregate across all regional rows of one group. The
     * inputs are stack-representation values at the combiner's channels of a remote row, and
     * the result is the stack representation of the aggregate's output type.
     */
    public interface Combiner
    {
        void add(List<Object> row);

        @Nullable
        Object result();
    }

    private static final class CountSumCombiner
            implements Combiner
    {
        private final int channel;
        private long count;

        private CountSumCombiner(int channel)
        {
            this.channel = channel;
        }

        @Override
        public void add(List<Object> row)
        {
            // a remote count partial is never null
            count += (long) row.get(channel);
        }

        @Override
        public Object result()
        {
            return count;
        }
    }

    private static final class LongSumCombiner
            implements Combiner
    {
        private final int channel;
        private boolean hasValue;
        private long sum;

        private LongSumCombiner(int channel)
        {
            this.channel = channel;
        }

        @Override
        public void add(List<Object> row)
        {
            Object value = row.get(channel);
            if (value == null) {
                return;
            }
            long addend = (long) value;
            try {
                sum = hasValue ? Math.addExact(sum, addend) : addend;
            }
            catch (ArithmeticException e) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "bigint addition overflow: %s + %s".formatted(sum, addend), e);
            }
            hasValue = true;
        }

        @Override
        public Object result()
        {
            return hasValue ? sum : null;
        }
    }

    private static final class DoubleSumCombiner
            implements Combiner
    {
        private final int channel;
        private boolean hasValue;
        private double sum;

        private DoubleSumCombiner(int channel)
        {
            this.channel = channel;
        }

        @Override
        public void add(List<Object> row)
        {
            Object value = row.get(channel);
            if (value == null) {
                return;
            }
            hasValue = true;
            sum += (double) value;
        }

        @Override
        public Object result()
        {
            return hasValue ? sum : null;
        }
    }

    /**
     * Sums REAL partials in a double and narrows at emit time, like the engine's
     * {@code sum(real)} accumulator.
     */
    private static final class RealSumCombiner
            implements Combiner
    {
        private final int channel;
        private boolean hasValue;
        private double sum;

        private RealSumCombiner(int channel)
        {
            this.channel = channel;
        }

        @Override
        public void add(List<Object> row)
        {
            Object value = row.get(channel);
            if (value == null) {
                return;
            }
            hasValue = true;
            sum += intBitsToFloat(toIntExact((long) value));
        }

        @Override
        public Object result()
        {
            if (!hasValue) {
                return null;
            }
            return (long) floatToRawIntBits((float) sum);
        }
    }

    /**
     * Sums {@code decimal(38, s)} partials exactly in a BigInteger and checks the decimal
     * range at emit time. Like the engine's overflow-counting accumulator, a sum whose
     * intermediate values transiently leave the range does not fail as long as the final
     * value fits.
     */
    private static final class DecimalSumCombiner
            implements Combiner
    {
        private final int channel;
        @Nullable
        private BigInteger sum;

        private DecimalSumCombiner(int channel)
        {
            this.channel = channel;
        }

        @Override
        public void add(List<Object> row)
        {
            Object value = row.get(channel);
            if (value == null) {
                return;
            }
            BigInteger addend = ((Int128) value).toBigInteger();
            sum = sum == null ? addend : sum.add(addend);
        }

        @Override
        public Object result()
        {
            if (sum == null) {
                return null;
            }
            if (sum.abs().compareTo(MAX_UNSCALED_DECIMAL) > 0) {
                throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Decimal overflow");
            }
            return Int128.valueOf(sum);
        }
    }

    private static final class MinMaxCombiner
            implements Combiner
    {
        private final int channel;
        private final MethodHandle comparison;
        private final boolean min;
        @Nullable
        private Object best;

        private MinMaxCombiner(int channel, MethodHandle comparison, boolean min)
        {
            this.channel = channel;
            this.comparison = comparison;
            this.min = min;
        }

        @Override
        public void add(List<Object> row)
        {
            Object value = row.get(channel);
            if (value == null) {
                return;
            }
            if (best == null) {
                best = value;
                return;
            }
            long compared;
            try {
                compared = (long) comparison.invokeWithArguments(value, best);
            }
            catch (Throwable e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
            if (min ? compared < 0 : compared > 0) {
                best = value;
            }
        }

        @Override
        public Object result()
        {
            return best;
        }
    }

    /**
     * Divides the combined remote sum by the combined remote count, like the engine's
     * decomposed {@code avg} accumulators. The sum partial is at {@code channel} and the
     * count partial at {@code channel + 1}, matching {@link #avgPartials}.
     */
    private static final class AverageCombiner
            implements Combiner
    {
        private final int channel;
        private final boolean real;
        private double sum;
        private long count;

        private AverageCombiner(int channel, boolean real)
        {
            this.channel = channel;
            this.real = real;
        }

        @Override
        public void add(List<Object> row)
        {
            Object partialSum = row.get(channel);
            if (partialSum != null) {
                if (real) {
                    sum += intBitsToFloat(toIntExact((long) partialSum));
                }
                else {
                    sum += (double) partialSum;
                }
            }
            count += (long) row.get(channel + 1);
        }

        @Override
        public Object result()
        {
            if (count == 0) {
                return null;
            }
            double average = sum / count;
            if (real) {
                return (long) floatToRawIntBits((float) average);
            }
            return average;
        }
    }
}
