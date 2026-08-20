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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;

import java.io.Closeable;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Multi-cluster test harness for the federation connector: N embedded "region" Trino
 * clusters, each with a memory catalog holding its shard of the data, and one central
 * cluster with a federation catalog spanning all of them. All clusters are
 * coordinator-only to keep startup fast.
 */
public final class FederationQueryRunner
        implements Closeable
{
    public static final String FEDERATION_CATALOG = "federation";
    public static final String REMOTE_CATALOG = "memory";
    private static final String SCHEMA = "default";

    private final Map<String, DistributedQueryRunner> regions;
    private final DistributedQueryRunner central;

    private FederationQueryRunner(Map<String, DistributedQueryRunner> regions, DistributedQueryRunner central)
    {
        this.regions = ImmutableMap.copyOf(regions);
        this.central = requireNonNull(central, "central is null");
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public DistributedQueryRunner central()
    {
        return central;
    }

    public DistributedQueryRunner region(String name)
    {
        DistributedQueryRunner region = regions.get(name);
        checkArgument(region != null, "Unknown region: %s", name);
        return region;
    }

    public Set<String> regionNames()
    {
        return regions.keySet();
    }

    /**
     * Runs SQL on the central cluster in the federation catalog.
     */
    public MaterializedResult execute(String sql)
    {
        return central.execute(sql);
    }

    /**
     * Runs SQL on one region cluster in the remote memory catalog, for seeding shard data
     * or inspecting the queries the connector sent.
     */
    public MaterializedResult executeOnRegion(String region, String sql)
    {
        return region(region).execute(sql);
    }

    /**
     * Runs the same SQL on every region cluster, for homogeneous DDL.
     */
    public void executeOnAllRegions(String sql)
    {
        for (DistributedQueryRunner region : regions.values()) {
            region.execute(sql);
        }
    }

    @Override
    public void close()
    {
        central.close();
        regions.values().forEach(DistributedQueryRunner::close);
    }

    public static final class Builder
    {
        private final Map<String, Optional<URI>> regions = new LinkedHashMap<>();
        private final Map<String, String> extraCatalogProperties = new LinkedHashMap<>();

        private Builder() {}

        /**
         * Adds an embedded region cluster with a memory catalog.
         */
        public Builder addRegion(String name)
        {
            addRegionEntry(name, Optional.empty());
            return this;
        }

        /**
         * Adds a region pointing at a fixed URI with no cluster behind it, for testing
         * unreachable regions.
         */
        public Builder addStaticRegion(String name, URI uri)
        {
            addRegionEntry(name, Optional.of(uri));
            return this;
        }

        /**
         * Adds a property to the federation catalog of the central cluster.
         */
        public Builder addCatalogProperty(String key, String value)
        {
            extraCatalogProperties.put(key, value);
            return this;
        }

        public FederationQueryRunner build()
                throws Exception
        {
            checkState(!regions.isEmpty(), "no regions defined");
            Map<String, DistributedQueryRunner> regionRunners = new LinkedHashMap<>();
            DistributedQueryRunner central = null;
            try {
                Map<String, URI> regionUris = new LinkedHashMap<>();
                for (Map.Entry<String, Optional<URI>> entry : regions.entrySet()) {
                    if (entry.getValue().isPresent()) {
                        regionUris.put(entry.getKey(), entry.getValue().get());
                        continue;
                    }
                    DistributedQueryRunner region = DistributedQueryRunner.builder(testSessionBuilder()
                                    .setCatalog(REMOTE_CATALOG)
                                    .setSchema(SCHEMA)
                                    .build())
                            .setWorkerCount(0)
                            .build();
                    regionRunners.put(entry.getKey(), region);
                    region.installPlugin(new MemoryPlugin());
                    region.createCatalog(REMOTE_CATALOG, "memory", ImmutableMap.of());
                    regionUris.put(entry.getKey(), region.getCoordinator().getBaseUrl());
                }

                central = DistributedQueryRunner.builder(testSessionBuilder()
                                .setCatalog(FEDERATION_CATALOG)
                                .setSchema(SCHEMA)
                                .build())
                        .setWorkerCount(0)
                        .build();
                central.installPlugin(new FederationPlugin());
                central.createCatalog(FEDERATION_CATALOG, "trino_federation", ImmutableMap.<String, String>builder()
                        .put("federation.regions", regionUris.entrySet().stream()
                                .map(entry -> entry.getKey() + "=" + entry.getValue())
                                .collect(joining(",")))
                        .put("federation.remote-catalog", REMOTE_CATALOG)
                        .putAll(extraCatalogProperties)
                        .buildOrThrow());
                return new FederationQueryRunner(regionRunners, central);
            }
            catch (Throwable e) {
                if (central != null) {
                    central.close();
                }
                regionRunners.values().forEach(DistributedQueryRunner::close);
                throw e;
            }
        }

        private void addRegionEntry(String name, Optional<URI> staticUri)
        {
            checkArgument(regions.put(name, staticUri) == null, "Duplicate region name: %s", name);
        }
    }
}
