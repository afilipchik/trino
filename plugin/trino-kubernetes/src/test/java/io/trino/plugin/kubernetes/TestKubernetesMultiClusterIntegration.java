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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Two real control planes served through one kubeconfig. The {@code kubernetes}
 * catalog runs in multi-cluster mode (synthetic {@code cluster} column, fan-out);
 * the {@code kubernetes_beta} catalog pins the {@code beta} context through
 * {@code kubernetes.kubeconfig-context}.
 */
final class TestKubernetesMultiClusterIntegration
        extends AbstractTestQueryFramework
{
    private TestingKubernetesCluster alpha;
    private TestingKubernetesCluster beta;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        assumeTrue(TestingKubernetesCluster.isAvailable(), "envtest binaries not available");
        alpha = closeAfterClass(TestingKubernetesCluster.create());
        beta = closeAfterClass(TestingKubernetesCluster.create());

        Path kubeconfig = Files.createTempFile("multi-kubeconfig", null);
        Files.writeString(kubeconfig,
                """
                apiVersion: v1
                kind: Config
                current-context: alpha
                clusters:
                  - name: alpha-cluster
                    cluster:
                      server: %s
                      insecure-skip-tls-verify: true
                  - name: beta-cluster
                    cluster:
                      server: %s
                      insecure-skip-tls-verify: true
                contexts:
                  - name: alpha
                    context:
                      cluster: alpha-cluster
                      user: alpha-user
                  - name: beta
                    context:
                      cluster: beta-cluster
                      user: beta-user
                users:
                  - name: alpha-user
                    user:
                      token: %s
                  - name: beta-user
                    user:
                      token: %s
                """.formatted(alpha.uri(), beta.uri(), alpha.token(), beta.token()));

        createConfigMap(alpha, "only-alpha", "alpha");
        createConfigMap(beta, "only-beta", "beta");

        DistributedQueryRunner queryRunner = KubernetesQueryRunner.builder()
                .addConnectorProperty("kubernetes.kubeconfig-path", kubeconfig.toString())
                .addConnectorProperty("kubernetes.multi-cluster.enabled", "true")
                .build();
        queryRunner.createCatalog("kubernetes_beta", "kubernetes", ImmutableMap.of(
                "kubernetes.kubeconfig-path", kubeconfig.toString(),
                "kubernetes.kubeconfig-context", "beta",
                "kubernetes.metadata-cache-ttl", "1s"));
        return queryRunner;
    }

    private static void createConfigMap(TestingKubernetesCluster cluster, String name, String origin)
    {
        cluster.create("/api/v1/namespaces/default/configmaps",
                """
                {"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"%s"},"data":{"origin":"%s"}}
                """.formatted(name, origin));
    }

    @Test
    void testClusterColumnExposed()
    {
        MaterializedResult result = computeActual("DESCRIBE core.configmaps");
        assertThat(result.getMaterializedRows().get(0).getField(0)).isEqualTo("cluster");
    }

    @Test
    void testFanOutAcrossClusters()
    {
        assertQuery(
                "SELECT cluster, name FROM core.configmaps WHERE name LIKE 'only-%'",
                "VALUES ('alpha', 'only-alpha'), ('beta', 'only-beta')");
    }

    @Test
    void testClusterPredicatePrunesFanOut()
    {
        assertQuery(
                "SELECT name FROM core.configmaps WHERE cluster = 'beta' AND name LIKE 'only-%'",
                "VALUES 'only-beta'");
        assertQueryReturnsEmptyResult("SELECT name FROM core.configmaps WHERE cluster = 'nonexistent'");
    }

    @Test
    void testInsertRoutedByClusterColumn()
    {
        assertUpdate(
                """
                INSERT INTO core.configmaps (cluster, name, namespace, data)
                VALUES ('beta', 'inserted-into-beta', 'default', MAP(ARRAY['k'], ARRAY['v']))
                """,
                1);
        assertThat(beta.get("/api/v1/namespaces/default/configmaps/inserted-into-beta")
                .path("data").path("k").asText()).isEqualTo("v");
        assertThat(alpha.statusCode("/api/v1/namespaces/default/configmaps/inserted-into-beta")).isEqualTo(404);
        assertUpdate("DELETE FROM core.configmaps WHERE name = 'inserted-into-beta' AND cluster = 'beta'", 1);
    }

    @Test
    void testInsertDefaultsToDefaultCluster()
    {
        assertUpdate(
                """
                INSERT INTO core.configmaps (name, namespace, data)
                VALUES ('inserted-default', 'default', MAP(ARRAY['k'], ARRAY['v']))
                """,
                1);
        assertThat(alpha.statusCode("/api/v1/namespaces/default/configmaps/inserted-default")).isEqualTo(200);
        assertThat(beta.statusCode("/api/v1/namespaces/default/configmaps/inserted-default")).isEqualTo(404);
        assertUpdate("DELETE FROM core.configmaps WHERE name = 'inserted-default' AND cluster = 'alpha'", 1);
    }

    @Test
    void testInsertIntoUnknownClusterFails()
    {
        assertQueryFails(
                """
                INSERT INTO core.configmaps (cluster, name, namespace, data)
                VALUES ('nonexistent', 'nowhere', 'default', MAP(ARRAY['k'], ARRAY['v']))
                """,
                ".*Unknown cluster 'nonexistent'.*");
    }

    @Test
    void testUpdateRoutedToOwningCluster()
    {
        createConfigMap(beta, "update-target", "beta");
        assertUpdate(
                "UPDATE core.configmaps SET data = MAP(ARRAY['origin'], ARRAY['updated']) WHERE name = 'update-target' AND cluster = 'beta'",
                1);
        assertThat(beta.get("/api/v1/namespaces/default/configmaps/update-target")
                .path("data").path("origin").asText()).isEqualTo("updated");
    }

    @Test
    void testDeleteRoutedToOwningCluster()
    {
        createConfigMap(alpha, "delete-twin", "alpha");
        createConfigMap(beta, "delete-twin", "beta");
        assertUpdate("DELETE FROM core.configmaps WHERE name = 'delete-twin' AND cluster = 'beta'", 1);
        assertThat(alpha.statusCode("/api/v1/namespaces/default/configmaps/delete-twin")).isEqualTo(200);
        assertThat(beta.statusCode("/api/v1/namespaces/default/configmaps/delete-twin")).isEqualTo(404);
    }

    @Test
    void testMovingAcrossClustersFails()
    {
        createConfigMap(alpha, "move-target", "alpha");
        assertQueryFails(
                "UPDATE core.configmaps SET cluster = 'beta' WHERE name = 'move-target' AND cluster = 'alpha'",
                ".*Moving Kubernetes objects across clusters is not supported.*");
    }

    @Test
    void testResourceMissingOnOtherClusterIsEmpty()
    {
        // a CRD registered only on the default cluster: the beta split finds the
        // resource unserved and contributes no rows instead of failing the query
        alpha.create("/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
                """
                {
                  "apiVersion": "apiextensions.k8s.io/v1",
                  "kind": "CustomResourceDefinition",
                  "metadata": {"name": "gadgets.fanout.trino.io"},
                  "spec": {
                    "group": "fanout.trino.io",
                    "names": {"plural": "gadgets", "singular": "gadget", "kind": "Gadget"},
                    "scope": "Namespaced",
                    "versions": [{
                      "name": "v1",
                      "served": true,
                      "storage": true,
                      "schema": {
                        "openAPIV3Schema": {
                          "type": "object",
                          "properties": {"spec": {"type": "object", "properties": {"color": {"type": "string"}}}}
                        }
                      }
                    }]
                  }
                }
                """);
        assertEventually(() -> {
            assertThat(alpha.statusCode("/apis/fanout.trino.io/v1")).isEqualTo(200);
            assertThat(alpha.get("/openapi/v3").path("paths").has("apis/fanout.trino.io/v1")).isTrue();
        });
        alpha.create("/apis/fanout.trino.io/v1/namespaces/default/gadgets",
                """
                {"apiVersion":"fanout.trino.io/v1","kind":"Gadget","metadata":{"name":"gadget-one"},"spec":{"color":"green"}}
                """);
        assertEventually(() ->
                assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains("fanout.trino.io"));

        assertQuery(
                "SELECT cluster, name FROM \"fanout.trino.io\".gadgets",
                "VALUES ('alpha', 'gadget-one')");
    }

    @Test
    void testKubeconfigContextCatalog()
    {
        // path 1: one catalog per cluster through kubernetes.kubeconfig-context
        assertQuery(
                "SELECT name FROM kubernetes_beta.core.configmaps WHERE name LIKE 'only-%'",
                "VALUES 'only-beta'");
        MaterializedResult result = computeActual("DESCRIBE kubernetes_beta.core.configmaps");
        assertThat(result.getMaterializedRows())
                .extracting(row -> row.getField(0))
                .doesNotContain("cluster");
    }
}
