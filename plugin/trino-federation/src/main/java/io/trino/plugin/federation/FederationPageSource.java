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
import io.trino.plugin.federation.client.RegionClient;
import io.trino.plugin.federation.client.RegionPageBuilder;
import io.trino.plugin.federation.client.RegionQueryResults;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_TYPE_MISMATCH;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Streams one region's rows for a scan split. The remote query is opened lazily on the first
 * {@link #getNextSourcePage()} call, so creating the page source never blocks on the network.
 * Rows are batched into pages by {@link RegionPageBuilder}; the synthetic {@code _region}
 * column is materialized as a run-length encoded block of the split's region name. When no
 * data column is projected (for example a raw {@code count(*)} scan), the remote query
 * produces one dummy value per row and only positions are emitted.
 */
public class FederationPageSource
        implements ConnectorPageSource
{
    /**
     * Marker in {@code outputChannels} for the synthetic {@code _region} column.
     */
    public static final int REGION_CHANNEL = -1;

    private static final int NO_PROJECTION_BATCH_SIZE = 8192;

    private final RegionClient client;
    private final String sql;
    private final List<Type> dataTypes;
    private final int[] outputChannels;
    private final MemoryContext memoryContext;
    @Nullable
    private final RegionPageBuilder pageBuilder;

    @Nullable
    private RegionQueryResults results;
    private boolean finished;
    private boolean closed;
    private long completedBytes;
    private long completedPositions;
    private long readTimeNanos;

    /**
     * @param dataTypes types of the projected data columns, in remote SELECT list order
     * @param outputChannels for every output channel, the index of the data column feeding
     *         it, or {@link #REGION_CHANNEL} for the synthetic {@code _region} column
     */
    public FederationPageSource(RegionClient client, String sql, List<Type> dataTypes, int[] outputChannels, MemoryContext memoryContext)
    {
        this.client = requireNonNull(client, "client is null");
        this.sql = requireNonNull(sql, "sql is null");
        this.dataTypes = ImmutableList.copyOf(requireNonNull(dataTypes, "dataTypes is null"));
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null").clone();
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        this.pageBuilder = this.dataTypes.isEmpty() ? null : new RegionPageBuilder(this.dataTypes);
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
            if (results == null) {
                results = client.execute(sql);
                if (pageBuilder != null) {
                    validateResultTypes(results.types());
                }
            }
            Page page;
            if (pageBuilder == null) {
                page = nextCountedPage();
            }
            else {
                page = nextDataPage();
            }
            if (page == null) {
                return null;
            }
            completedBytes += page.getSizeInBytes();
            completedPositions += page.getPositionCount();
            return SourcePage.create(page);
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
            if (pageBuilder != null) {
                memoryContext.setBytes(pageBuilder.retainedSizeInBytes());
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
        if (results != null) {
            // cancels the remote query when the stream is not exhausted yet
            results.close();
        }
    }

    @Nullable
    private Page nextDataPage()
    {
        while (!pageBuilder.isFull() && results.hasNext()) {
            pageBuilder.appendRow(results.next());
        }
        if (!results.hasNext()) {
            finished = true;
        }
        if (pageBuilder.isEmpty()) {
            return null;
        }
        return toOutputPage(pageBuilder.flush());
    }

    @Nullable
    private Page nextCountedPage()
    {
        int positions = 0;
        while (positions < NO_PROJECTION_BATCH_SIZE && results.hasNext()) {
            results.next();
            positions++;
        }
        if (positions < NO_PROJECTION_BATCH_SIZE) {
            finished = true;
        }
        if (positions == 0) {
            return null;
        }
        return toOutputPage(new Page(positions));
    }

    private Page toOutputPage(Page dataPage)
    {
        int positionCount = dataPage.getPositionCount();
        Block[] blocks = new Block[outputChannels.length];
        for (int channel = 0; channel < outputChannels.length; channel++) {
            if (outputChannels[channel] == REGION_CHANNEL) {
                blocks[channel] = RunLengthEncodedBlock.create(VARCHAR, utf8Slice(client.regionName()), positionCount);
            }
            else {
                blocks[channel] = dataPage.getBlock(outputChannels[channel]);
            }
        }
        return new Page(positionCount, blocks);
    }

    private void validateResultTypes(List<Type> actualTypes)
    {
        if (!actualTypes.equals(dataTypes)) {
            throw new TrinoException(
                    FEDERATION_TYPE_MISMATCH,
                    "Region '%s' returned column types %s, expected %s".formatted(client.regionName(), actualTypes, dataTypes));
        }
    }
}
