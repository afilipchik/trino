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
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * The single split of an aggregated scan. Its page source queries every listed region
 * concurrently with the partial-aggregate SQL and combines the partials into final rows, so
 * exactly one split exists per aggregated scan — even when the region list is empty, since a
 * global aggregation must still emit its one row.
 */
public record FederationAggregateSplit(List<String> regionNames)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(FederationAggregateSplit.class);

    public FederationAggregateSplit
    {
        regionNames = ImmutableList.copyOf(requireNonNull(regionNames, "regionNames is null"));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(regionNames, SizeOf::estimatedSizeOf);
    }
}
