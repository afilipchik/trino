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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;

public class FederationConfig
{
    private static final Splitter REGION_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private List<Region> regions = ImmutableList.of();
    private String remoteCatalog;
    private String user = "federation";
    private Optional<String> password = Optional.empty();
    private Duration connectTimeout = new Duration(10, SECONDS);
    private Duration requestTimeout = new Duration(30, SECONDS);

    @NotEmpty(message = "must list at least one region")
    public List<Region> getRegions()
    {
        return regions;
    }

    @Config("federation.regions")
    @ConfigDescription("Ordered list of name=uri pairs of regional Trino clusters, for example us-east=http://trino-a:8080,eu-west=http://trino-b:8080")
    public FederationConfig setRegions(String regions)
    {
        this.regions = parseRegions(regions);
        return this;
    }

    @NotNull
    public String getRemoteCatalog()
    {
        return remoteCatalog;
    }

    @Config("federation.remote-catalog")
    @ConfigDescription("Name of the catalog to query on the regional clusters")
    public FederationConfig setRemoteCatalog(String remoteCatalog)
    {
        this.remoteCatalog = remoteCatalog;
        return this;
    }

    @NotNull
    public String getUser()
    {
        return user;
    }

    @Config("federation.user")
    @ConfigDescription("User sent to the regional clusters")
    public FederationConfig setUser(String user)
    {
        this.user = user;
        return this;
    }

    @NotNull
    public Optional<String> getPassword()
    {
        return password;
    }

    @Config("federation.password")
    @ConfigDescription("Password sent to the regional clusters")
    @ConfigSecuritySensitive
    public FederationConfig setPassword(String password)
    {
        this.password = Optional.ofNullable(password);
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("federation.connect-timeout")
    @ConfigDescription("Timeout for establishing a connection to a regional cluster")
    public FederationConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("federation.request-timeout")
    @ConfigDescription("Timeout for individual requests to a regional cluster")
    public FederationConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    private static List<Region> parseRegions(String regions)
    {
        ImmutableList.Builder<Region> parsed = ImmutableList.builder();
        Set<String> names = new HashSet<>();
        for (String entry : REGION_SPLITTER.split(regions)) {
            int separator = entry.indexOf('=');
            checkArgument(separator > 0, "Invalid region entry '%s', expected name=uri", entry);
            String name = entry.substring(0, separator).trim();
            String value = entry.substring(separator + 1).trim();
            checkArgument(!name.isEmpty(), "Region name is empty in entry '%s'", entry);
            checkArgument(!value.isEmpty(), "Region URI is empty for region '%s'", name);
            checkArgument(names.add(name), "Duplicate region name '%s'", name);
            URI uri = URI.create(value);
            checkArgument(
                    "http".equals(uri.getScheme()) || "https".equals(uri.getScheme()),
                    "Region URI '%s' for region '%s' must use http or https",
                    value,
                    name);
            parsed.add(new Region(name, uri));
        }
        return parsed.build();
    }
}
