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

import io.trino.spi.connector.ConnectorSplit;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

/**
 * A scan of one region's shard of the table; the page source streams that region's rows.
 * The remote endpoint is resolved from the region name on the worker, so splits carry no
 * host affinity and can be scheduled anywhere.
 */
public record FederationSplit(String regionName)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(FederationSplit.class);

    public FederationSplit
    {
        requireNonNull(regionName, "regionName is null");
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(regionName);
    }
}
