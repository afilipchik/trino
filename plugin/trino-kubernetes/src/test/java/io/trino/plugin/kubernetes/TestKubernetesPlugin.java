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
package io.trino.plugin.kubernetes;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

final class TestKubernetesPlugin
{
    @Test
    void testCreateConnector()
    {
        ConnectorFactory factory = Iterables.getOnlyElement(new KubernetesPlugin().getConnectorFactories());
        Connector connector = factory.create(
                "test",
                ImmutableMap.of(
                        "kubernetes.api-server-uri", "https://127.0.0.1:1",
                        "kubernetes.token", "unused",
                        "kubernetes.insecure-tls", "true"),
                new TestingConnectorContext());
        connector.shutdown();
    }
}
