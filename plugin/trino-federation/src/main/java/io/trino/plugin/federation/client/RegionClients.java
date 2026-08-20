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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.trino.plugin.federation.FederationConfig;
import jakarta.annotation.PreDestroy;
import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.OkHttpUtil.basicAuth;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Creates and owns one {@link RegionClient} per configured region, sharing a single HTTP
 * client across all of them.
 */
public class RegionClients
        implements Closeable
{
    private final OkHttpClient httpClient;
    private final List<RegionClient> clients;
    private final Map<String, RegionClient> clientsByName;

    @Inject
    public RegionClients(FederationConfig config)
    {
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(config.getConnectTimeout().toMillis(), MILLISECONDS)
                .readTimeout(config.getRequestTimeout().toMillis(), MILLISECONDS)
                .writeTimeout(config.getRequestTimeout().toMillis(), MILLISECONDS);
        config.getPassword().ifPresent(password -> builder.addInterceptor(basicAuth(config.getUser(), password)));
        this.httpClient = builder.build();
        this.clients = config.getRegions().stream()
                .map(region -> new RegionClient(httpClient, region, config.getRemoteCatalog(), config.getUser(), config.getRequestTimeout()))
                .collect(toImmutableList());
        this.clientsByName = Maps.uniqueIndex(clients, RegionClient::regionName);
    }

    /**
     * All region clients, in the configured region order.
     */
    public List<RegionClient> clients()
    {
        return ImmutableList.copyOf(clients);
    }

    public RegionClient client(String regionName)
    {
        RegionClient client = clientsByName.get(regionName);
        checkArgument(client != null, "Unknown region: %s", regionName);
        return client;
    }

    @PreDestroy
    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdownNow();
        httpClient.connectionPool().evictAll();
    }
}
