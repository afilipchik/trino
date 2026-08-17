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

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class TestKubernetesClusterHarness
{
    @Test
    void testClusterLifecycle()
            throws Exception
    {
        assumeTrue(TestingKubernetesCluster.isAvailable(), "envtest binaries not available");
        try (TestingKubernetesCluster cluster = TestingKubernetesCluster.create()) {
            JsonNode created = cluster.create("/api/v1/namespaces/default/configmaps",
                    """
                    {"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"harness-probe"},"data":{"k":"v"}}
                    """);
            assertThat(created.path("metadata").path("resourceVersion").asText()).isNotEmpty();

            JsonNode listed = cluster.get("/api/v1/namespaces/default/configmaps");
            assertThat(listed.path("items")).anySatisfy(item ->
                    assertThat(item.path("metadata").path("name").asText()).isEqualTo("harness-probe"));

            JsonNode openApi = cluster.get("/openapi/v3");
            assertThat(openApi.path("paths").has("api/v1")).isTrue();
        }
    }
}
