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
import io.airlift.slice.Slice;
import io.trino.plugin.federation.AggregateCombiners.Combiner;
import io.trino.plugin.federation.client.RegionClient;
import io.trino.plugin.federation.client.RegionQueryResults;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import jakarta.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_REMOTE_ERROR;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_TYPE_MISMATCH;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * Page source of the single fan-out split of an aggregated scan. On the first page request
 * it runs the partial-aggregate SQL on every active region concurrently (bounded by the
 * fan-out thread cap), drains each region's pre-aggregated rows on its own worker thread,
 * and then merges the partials on the calling thread into a hash table keyed on the grouping
 * values, so no combine state is shared between threads. Grouping equality and hashing use
 * the engine's type operators, matching GROUP BY semantics exactly (NaNs group together,
 * {@code -0.0} groups with {@code 0.0}).
 * <p>
 * A grouped {@code _region} column never reaches the regions: its key component is the
 * constant region name of the stream being merged. When it is the only grouping column the
 * remote query degenerates to a global aggregate that returns one row even for an empty
 * shard, so a {@code count(*)} probe partial is appended and rows with a zero probe are
 * dropped. A global aggregation (no grouping columns) always emits exactly one row, seeded
 * before any region is read so it survives an empty — or fully pruned — region list.
 */
public class AggregateCombiningPageSource
        implements ConnectorPageSource
{
    /**
     * Marker in {@link KeyColumn#remoteChannel()} for the synthetic {@code _region} column.
     */
    public static final int REGION_KEY = -1;

    // coarse retained-size estimates for the combine hash table
    private static final long GROUP_ENTRY_SIZE = 128;
    private static final long COMBINER_SIZE = 48;
    private static final long KEY_VALUE_SIZE = 32;

    private final List<RegionClient> clients;
    private final String sql;
    private final List<Type> remoteTypes;
    private final List<KeyColumn> keyColumns;
    private final List<Supplier<Combiner>> combinerFactories;
    private final int probeChannel;
    private final List<OutputColumn> outputColumns;
    private final int fanoutThreads;
    private final MemoryContext memoryContext;
    private final KeyOperators keyOperators;
    private final PageBuilder pageBuilder;
    private final List<RegionQueryResults> openResults = new CopyOnWriteArrayList<>();

    @Nullable
    private Iterator<Entry<GroupKey, List<Combiner>>> outputIterator;
    private volatile boolean closed;
    private boolean finished;
    private long retainedBytes;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    /**
     * @param remoteTypes expected column types of the partial-aggregate query results
     * @param keyColumns one entry per grouping column in engine order, holding the remote
     *         channel its values are read from, or {@link #REGION_KEY}
     * @param combinerFactories one factory per aggregate, each bound to its remote channels
     * @param probeChannel remote channel of the {@code count(*)} emptiness probe, or -1
     * @param outputColumns source of every projected output channel
     */
    public AggregateCombiningPageSource(
            List<RegionClient> clients,
            String sql,
            List<Type> remoteTypes,
            List<KeyColumn> keyColumns,
            List<Supplier<Combiner>> combinerFactories,
            int probeChannel,
            List<OutputColumn> outputColumns,
            int fanoutThreads,
            TypeOperators typeOperators,
            MemoryContext memoryContext)
    {
        this.clients = ImmutableList.copyOf(requireNonNull(clients, "clients is null"));
        this.sql = requireNonNull(sql, "sql is null");
        this.remoteTypes = ImmutableList.copyOf(requireNonNull(remoteTypes, "remoteTypes is null"));
        this.keyColumns = ImmutableList.copyOf(requireNonNull(keyColumns, "keyColumns is null"));
        this.combinerFactories = ImmutableList.copyOf(requireNonNull(combinerFactories, "combinerFactories is null"));
        this.probeChannel = probeChannel;
        this.outputColumns = ImmutableList.copyOf(requireNonNull(outputColumns, "outputColumns is null"));
        this.fanoutThreads = fanoutThreads;
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.keyOperators = new KeyOperators(this.keyColumns, requireNonNull(typeOperators, "typeOperators is null"));
        this.pageBuilder = new PageBuilder(this.outputColumns.stream()
                .map(OutputColumn::type)
                .collect(toImmutableList()));
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished || closed) {
            return null;
        }
        long start = System.nanoTime();
        try {
            if (outputIterator == null) {
                outputIterator = combineRegionPartials();
            }
            while (outputIterator.hasNext() && !pageBuilder.isFull()) {
                writeRow(outputIterator.next());
            }
            if (!outputIterator.hasNext()) {
                finished = true;
            }
            if (pageBuilder.isEmpty()) {
                return null;
            }
            Page page = pageBuilder.build();
            pageBuilder.reset();
            completedBytes += page.getSizeInBytes();
            completedPositions += page.getPositionCount();
            return SourcePage.create(page);
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
            if (!closed) {
                memoryContext.setBytes(retainedBytes + pageBuilder.getRetainedSizeInBytes());
            }
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        finished = true;
        memoryContext.setBytes(0);
        // cancels the remote queries of region readers that are still streaming
        openResults.forEach(RegionQueryResults::close);
    }

    private Iterator<Entry<GroupKey, List<Combiner>>> combineRegionPartials()
    {
        Map<GroupKey, List<Combiner>> groups = new LinkedHashMap<>();
        if (keyColumns.isEmpty()) {
            // a global aggregation emits exactly one row even when no region contributes
            groups.put(new GroupKey(keyOperators, new Object[0]), newCombiners());
            retainedBytes += GROUP_ENTRY_SIZE + combinerFactories.size() * COMBINER_SIZE;
        }
        if (!clients.isEmpty()) {
            ExecutorService executor = newFixedThreadPool(
                    min(clients.size(), fanoutThreads),
                    daemonThreadsNamed("trino-federation-aggregate-%s"));
            try {
                List<Future<List<List<Object>>>> futures = new ArrayList<>();
                for (RegionClient client : clients) {
                    futures.add(executor.submit(() -> readRegionRows(client)));
                }
                for (int region = 0; region < clients.size(); region++) {
                    String regionName = clients.get(region).regionName();
                    mergeRegion(regionName, regionRows(regionName, futures.get(region)), groups);
                    memoryContext.setBytes(retainedBytes);
                }
            }
            finally {
                executor.shutdownNow();
            }
        }
        return groups.entrySet().iterator();
    }

    /**
     * Runs on a fan-out worker thread: streams one region's pre-aggregated rows into a local
     * list, touching no shared combine state.
     */
    private List<List<Object>> readRegionRows(RegionClient client)
    {
        if (closed) {
            return ImmutableList.of();
        }
        RegionQueryResults results = client.execute(sql);
        openResults.add(results);
        try (results) {
            List<Type> actualTypes = results.types();
            if (!actualTypes.equals(remoteTypes)) {
                throw new TrinoException(
                        FEDERATION_TYPE_MISMATCH,
                        "Region '%s' returned column types %s, expected %s".formatted(client.regionName(), actualTypes, remoteTypes));
            }
            ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
            while (results.hasNext()) {
                rows.add(results.next());
            }
            return rows.build();
        }
        finally {
            openResults.remove(results);
        }
    }

    private List<List<Object>> regionRows(String regionName, Future<List<List<Object>>> future)
    {
        try {
            return future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(FEDERATION_REMOTE_ERROR, "Interrupted while aggregating region '%s'".formatted(regionName), e);
        }
        catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw new TrinoException(
                    FEDERATION_REMOTE_ERROR,
                    "Region '%s' partial aggregation failed: %s".formatted(regionName, cause.getMessage()),
                    cause);
        }
    }

    private void mergeRegion(String regionName, List<List<Object>> rows, Map<GroupKey, List<Combiner>> groups)
    {
        Slice regionValue = utf8Slice(regionName);
        for (List<Object> row : rows) {
            if (probeChannel >= 0 && (long) row.get(probeChannel) == 0) {
                // the shard is empty after filtering, so this region contributes no group
                continue;
            }
            Object[] keyValues = new Object[keyColumns.size()];
            for (int index = 0; index < keyColumns.size(); index++) {
                int remoteChannel = keyColumns.get(index).remoteChannel();
                keyValues[index] = remoteChannel == REGION_KEY ? regionValue : row.get(remoteChannel);
            }
            GroupKey key = new GroupKey(keyOperators, keyValues);
            List<Combiner> combiners = groups.get(key);
            if (combiners == null) {
                combiners = newCombiners();
                groups.put(key, combiners);
                retainedBytes += GROUP_ENTRY_SIZE + estimatedKeySize(keyValues) + combiners.size() * COMBINER_SIZE;
            }
            for (Combiner combiner : combiners) {
                combiner.add(row);
            }
        }
    }

    private List<Combiner> newCombiners()
    {
        return combinerFactories.stream()
                .map(Supplier::get)
                .collect(toImmutableList());
    }

    private void writeRow(Entry<GroupKey, List<Combiner>> group)
    {
        pageBuilder.declarePosition();
        for (int channel = 0; channel < outputColumns.size(); channel++) {
            OutputColumn output = outputColumns.get(channel);
            Object value = switch (output.source()) {
                case GROUPING_KEY -> group.getKey().value(output.index());
                case AGGREGATE -> group.getValue().get(output.index()).result();
            };
            writeNativeValue(output.type(), pageBuilder.getBlockBuilder(channel), value);
        }
    }

    private static long estimatedKeySize(Object[] keyValues)
    {
        long size = 0;
        for (Object value : keyValues) {
            if (value instanceof Slice slice) {
                size += slice.getRetainedSize();
            }
            else {
                size += KEY_VALUE_SIZE;
            }
        }
        return size;
    }

    /**
     * @param remoteChannel channel of the grouping column in the remote partial results, or
     *         {@link #REGION_KEY} for the synthetic {@code _region} column
     */
    public record KeyColumn(int remoteChannel, Type type)
    {
        public KeyColumn
        {
            requireNonNull(type, "type is null");
        }
    }

    public enum OutputSource
    {
        GROUPING_KEY,
        AGGREGATE,
    }

    /**
     * @param source whether the output channel is fed by a grouping key or a combined
     *         aggregate
     * @param index position in the grouping key or in the aggregates list
     */
    public record OutputColumn(OutputSource source, int index, Type type)
    {
        public OutputColumn
        {
            requireNonNull(source, "source is null");
            requireNonNull(type, "type is null");
        }
    }

    /**
     * Per-grouping-column hash and equality method handles. Grouping matches the engine's
     * GROUP BY semantics by using the types' IDENTICAL operators over the key's stack values.
     */
    private static final class KeyOperators
    {
        private final MethodHandle[] hashOperators;
        private final MethodHandle[] identicalOperators;

        KeyOperators(List<KeyColumn> keyColumns, TypeOperators typeOperators)
        {
            hashOperators = new MethodHandle[keyColumns.size()];
            identicalOperators = new MethodHandle[keyColumns.size()];
            for (int index = 0; index < keyColumns.size(); index++) {
                Type type = keyColumns.get(index).type();
                hashOperators[index] = typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL));
                identicalOperators[index] = typeOperators.getIdenticalOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL));
            }
        }

        int hash(Object[] values)
        {
            long hash = 0;
            for (int index = 0; index < values.length; index++) {
                long valueHash = values[index] == null ? 0 : (long) invoke(hashOperators[index], values[index]);
                hash = hash * 31 + valueHash;
            }
            return Long.hashCode(hash);
        }

        boolean identical(Object[] left, Object[] right)
        {
            for (int index = 0; index < left.length; index++) {
                Object leftValue = left[index];
                Object rightValue = right[index];
                if (leftValue == null || rightValue == null) {
                    if ((leftValue == null) != (rightValue == null)) {
                        return false;
                    }
                    continue;
                }
                if (!(boolean) invoke(identicalOperators[index], leftValue, rightValue)) {
                    return false;
                }
            }
            return true;
        }

        private static Object invoke(MethodHandle handle, Object... arguments)
        {
            try {
                return handle.invokeWithArguments(arguments);
            }
            catch (Throwable e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
    }

    private static final class GroupKey
    {
        private final KeyOperators operators;
        private final Object[] values;
        private final int hash;

        GroupKey(KeyOperators operators, Object[] values)
        {
            this.operators = operators;
            this.values = values;
            this.hash = operators.hash(values);
        }

        @Nullable
        Object value(int index)
        {
            return values[index];
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof GroupKey groupKey && operators.identical(values, groupKey.values);
        }

        @Override
        public int hashCode()
        {
            return hash;
        }
    }
}
