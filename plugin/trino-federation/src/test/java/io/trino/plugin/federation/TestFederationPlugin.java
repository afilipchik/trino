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
import com.google.common.collect.Iterables;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

final class TestFederationPlugin
{
    @Test
    void testCreateConnector()
    {
        ConnectorFactory factory = Iterables.getOnlyElement(new FederationPlugin().getConnectorFactories());
        Connector connector = factory.create(
                "test",
                ImmutableMap.of(
                        "federation.regions", "us-east=http://trino-a:8080,eu-west=http://trino-b:8080",
                        "federation.remote-catalog", "shard"),
                new TestingConnectorContext());
        connector.shutdown();
    }
}
