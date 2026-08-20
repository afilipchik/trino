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
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestFederationConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FederationConfig.class)
                .setRegions("")
                .setRemoteCatalog(null)
                .setUser("federation")
                .setPassword(null)
                .setConnectTimeout(new Duration(10, SECONDS))
                .setRequestTimeout(new Duration(30, SECONDS))
                .setFanoutThreads(16));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("federation.regions", "us-east=http://trino-a:8080,eu-west=https://trino-b:8443")
                .put("federation.remote-catalog", "shard")
                .put("federation.user", "reporting")
                .put("federation.password", "secret")
                .put("federation.connect-timeout", "5s")
                .put("federation.request-timeout", "60s")
                .put("federation.fanout-threads", "4")
                .buildOrThrow();

        FederationConfig expected = new FederationConfig()
                .setRegions("us-east=http://trino-a:8080,eu-west=https://trino-b:8443")
                .setRemoteCatalog("shard")
                .setUser("reporting")
                .setPassword("secret")
                .setConnectTimeout(new Duration(5, SECONDS))
                .setRequestTimeout(new Duration(60, SECONDS))
                .setFanoutThreads(4);

        assertFullMapping(properties, expected);
    }

    @Test
    void testRegionsParsedInOrder()
    {
        FederationConfig config = new FederationConfig()
                .setRegions(" us-east = http://trino-a:8080 , eu-west=https://trino-b:8443,ap-south=http://trino-c:8080");

        assertThat(config.getRegions()).containsExactly(
                new Region("us-east", URI.create("http://trino-a:8080")),
                new Region("eu-west", URI.create("https://trino-b:8443")),
                new Region("ap-south", URI.create("http://trino-c:8080")));
        assertThat(config.getPassword()).isEqualTo(Optional.empty());
    }

    @Test
    void testDuplicateRegionNameRejected()
    {
        assertThatThrownBy(() -> new FederationConfig()
                .setRegions("us-east=http://trino-a:8080,us-east=http://trino-b:8080"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Duplicate region name 'us-east'");
    }

    @Test
    void testInvalidRegionEntryRejected()
    {
        assertThatThrownBy(() -> new FederationConfig().setRegions("us-east"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid region entry 'us-east', expected name=uri");

        assertThatThrownBy(() -> new FederationConfig().setRegions("=http://trino-a:8080"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid region entry '=http://trino-a:8080', expected name=uri");

        assertThatThrownBy(() -> new FederationConfig().setRegions("us-east="))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Region URI is empty for region 'us-east'");
    }

    @Test
    void testInvalidRegionUriRejected()
    {
        assertThatThrownBy(() -> new FederationConfig().setRegions("us-east=ftp://trino-a:21"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Region URI 'ftp://trino-a:21' for region 'us-east' must use http or https");

        assertThatThrownBy(() -> new FederationConfig().setRegions("us-east=trino-a:8080"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must use http or https");

        assertThatThrownBy(() -> new FederationConfig().setRegions("us-east=http://trino a:8080"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
