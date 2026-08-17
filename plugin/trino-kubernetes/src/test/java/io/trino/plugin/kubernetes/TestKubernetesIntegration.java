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
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZonedDateTime;

import static io.trino.testing.assertions.Assert.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class TestKubernetesIntegration
        extends AbstractTestQueryFramework
{
    private static final String TEST_NAMESPACE = "trino-test-ns";
    private static final String COPY_NAMESPACE = "trino-copy-ns";

    private TestingKubernetesCluster cluster;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        assumeTrue(TestingKubernetesCluster.isAvailable(), "envtest binaries not available");
        cluster = closeAfterClass(TestingKubernetesCluster.create());

        cluster.create("/api/v1/namespaces",
                """
                {"apiVersion":"v1","kind":"Namespace","metadata":{"name":"%s"}}
                """.formatted(TEST_NAMESPACE));
        cluster.create("/api/v1/namespaces",
                """
                {"apiVersion":"v1","kind":"Namespace","metadata":{"name":"%s"}}
                """.formatted(COPY_NAMESPACE));

        createPod("default", "pod-nginx", "web", "nginx:1.25.3");
        createPod("default", "pod-redis", "cache", "redis:7.2");
        createPod(TEST_NAMESPACE, "pod-other", "batch", "busybox:1.36");

        return KubernetesQueryRunner.builder(cluster).build();
    }

    private void createPod(String namespace, String name, String app, String image)
    {
        cluster.create("/api/v1/namespaces/" + namespace + "/pods",
                """
                {
                  "apiVersion": "v1",
                  "kind": "Pod",
                  "metadata": {"name": "%s", "namespace": "%s", "labels": {"app": "%s"}},
                  "spec": {"containers": [{"name": "main", "image": "%s"}]}
                }
                """.formatted(name, namespace, app, image));
    }

    @Test
    void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertThat(result.getOnlyColumnAsSet())
                .contains("core", "apps", "batch", "networking.k8s.io", "rbac.authorization.k8s.io");
    }

    @Test
    void testShowTables()
    {
        assertThat(computeActual("SHOW TABLES FROM core").getOnlyColumnAsSet())
                .contains("pods", "configmaps", "services", "namespaces", "secrets");
        assertThat(computeActual("SHOW TABLES FROM apps").getOnlyColumnAsSet())
                .contains("deployments", "statefulsets", "daemonsets");
    }

    @Test
    void testDescribePods()
    {
        MaterializedResult result = computeActual("DESCRIBE core.pods");
        String specType = null;
        String metadataType = null;
        for (MaterializedRow row : result.getMaterializedRows()) {
            if (row.getField(0).equals("spec")) {
                specType = (String) row.getField(1);
            }
            if (row.getField(0).equals("metadata")) {
                metadataType = (String) row.getField(1);
            }
        }
        assertThat(specType).contains("\"containers\" array(row(");
        assertThat(specType).contains("\"image\" varchar");
        assertThat(metadataType).contains("\"creationTimestamp\" timestamp(3) with time zone");
        assertThat(metadataType).contains("\"labels\" map(varchar, varchar)");
        assertThat(result.getMaterializedRows())
                .extracting(row -> row.getField(0))
                .contains("name", "namespace", "apiversion", "kind", "spec", "status");
    }

    @Test
    void testSelectPodsByContainerImage()
    {
        assertQuery(
                """
                SELECT p.name, c.image
                FROM core.pods p
                CROSS JOIN UNNEST(p.spec.containers) AS c
                WHERE c.image LIKE 'nginx%' AND p.namespace = 'default'
                """,
                "VALUES ('pod-nginx', 'nginx:1.25.3')");
    }

    @Test
    void testNamespaceFilter()
    {
        assertQuery(
                "SELECT name FROM core.pods WHERE namespace = '%s'".formatted(TEST_NAMESPACE),
                "VALUES 'pod-other'");
    }

    @Test
    void testNameFilter()
    {
        assertQuery(
                "SELECT namespace FROM core.pods WHERE name = 'pod-redis'",
                "VALUES 'default'");
    }

    @Test
    void testLabelsMapPredicate()
    {
        assertQuery(
                "SELECT name FROM core.pods WHERE element_at(metadata.labels, 'app') = 'cache'",
                "VALUES 'pod-redis'");
    }

    @Test
    void testCreationTimestampIsTimestamp()
    {
        Object value = computeScalar("SELECT metadata.creationTimestamp FROM core.pods WHERE name = 'pod-nginx'");
        assertThat(value).isInstanceOf(ZonedDateTime.class);
        assertThat((ZonedDateTime) value).isAfter(ZonedDateTime.parse("2020-01-01T00:00:00Z"));
    }

    @Test
    void testClusterScopedTable()
    {
        assertQuery(
                "SELECT count(*) FROM core.namespaces WHERE name = '%s'".formatted(TEST_NAMESPACE),
                "VALUES 1");
    }

    @Test
    void testInsertConfigMap()
    {
        assertUpdate(
                """
                INSERT INTO core.configmaps (name, namespace, data)
                VALUES ('trino-inserted', '%s', MAP(ARRAY['greeting'], ARRAY['hello']))
                """.formatted(TEST_NAMESPACE),
                1);

        JsonNode created = cluster.get("/api/v1/namespaces/" + TEST_NAMESPACE + "/configmaps/trino-inserted");
        assertThat(created.path("data").path("greeting").asText()).isEqualTo("hello");

        assertQuery(
                "SELECT data['greeting'] FROM core.configmaps WHERE namespace = '%s' AND name = 'trino-inserted'".formatted(TEST_NAMESPACE),
                "VALUES 'hello'");
    }

    @Test
    void testInsertPodCopy()
    {
        // round-trip: read a typed pod spec and insert it back as a new object
        assertUpdate(
                """
                INSERT INTO core.pods (name, namespace, spec)
                SELECT 'pod-nginx-copy', '%s', spec
                FROM core.pods
                WHERE name = 'pod-nginx' AND namespace = 'default'
                """.formatted(COPY_NAMESPACE),
                1);

        JsonNode copied = cluster.get("/api/v1/namespaces/" + COPY_NAMESPACE + "/pods/pod-nginx-copy");
        assertThat(copied.path("spec").path("containers").get(0).path("image").asText()).isEqualTo("nginx:1.25.3");
    }

    @Test
    void testUpdateConfigMap()
    {
        cluster.create("/api/v1/namespaces/default/configmaps",
                """
                {"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"trino-update-target"},"data":{"version":"1"}}
                """);

        assertUpdate(
                "UPDATE core.configmaps SET data = MAP(ARRAY['version'], ARRAY['2']) WHERE name = 'trino-update-target'",
                1);

        JsonNode updated = cluster.get("/api/v1/namespaces/default/configmaps/trino-update-target");
        assertThat(updated.path("data").path("version").asText()).isEqualTo("2");
    }

    @Test
    void testUpdateRenameFails()
    {
        cluster.create("/api/v1/namespaces/default/configmaps",
                """
                {"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"trino-rename-target"},"data":{"a":"b"}}
                """);

        assertQueryFails(
                "UPDATE core.configmaps SET name = 'renamed' WHERE name = 'trino-rename-target'",
                ".*Renaming Kubernetes objects is not supported.*");
    }

    @Test
    void testDeleteConfigMap()
    {
        cluster.create("/api/v1/namespaces/default/configmaps",
                """
                {"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"trino-delete-a"},"data":{"owner":"trino"}}
                """);
        cluster.create("/api/v1/namespaces/default/configmaps",
                """
                {"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"trino-delete-b"},"data":{"owner":"other"}}
                """);

        // arbitrary (non-pushdown-able) predicate on the data map
        assertUpdate("DELETE FROM core.configmaps WHERE element_at(data, 'owner') = 'trino' AND name LIKE 'trino-delete-%'", 1);

        assertThat(cluster.statusCode("/api/v1/namespaces/default/configmaps/trino-delete-a")).isEqualTo(404);
        assertThat(cluster.statusCode("/api/v1/namespaces/default/configmaps/trino-delete-b")).isEqualTo(200);
    }

    @Test
    void testCustomResourceDefinition()
    {
        cluster.create("/apis/apiextensions.k8s.io/v1/customresourcedefinitions",
                """
                {
                  "apiVersion": "apiextensions.k8s.io/v1",
                  "kind": "CustomResourceDefinition",
                  "metadata": {"name": "widgets.example.trino.io"},
                  "spec": {
                    "group": "example.trino.io",
                    "names": {"plural": "widgets", "singular": "widget", "kind": "Widget"},
                    "scope": "Namespaced",
                    "versions": [{
                      "name": "v1",
                      "served": true,
                      "storage": true,
                      "schema": {
                        "openAPIV3Schema": {
                          "type": "object",
                          "properties": {
                            "spec": {
                              "type": "object",
                              "properties": {
                                "image": {"type": "string"},
                                "replicas": {"type": "integer"},
                                "size": {"x-kubernetes-int-or-string": true},
                                "activated": {"type": "string", "format": "date-time"},
                                "tags": {"type": "array", "items": {"type": "string"}},
                                "settings": {"type": "object", "additionalProperties": {"type": "string"}}
                              }
                            }
                          }
                        }
                      }
                    }]
                  }
                }
                """);

        // wait for the CRD to be served and its OpenAPI schema published
        assertEventually(() -> {
            assertThat(cluster.statusCode("/apis/example.trino.io/v1")).isEqualTo(200);
            assertThat(cluster.get("/openapi/v3").path("paths").has("apis/example.trino.io/v1")).isTrue();
        });

        cluster.create("/apis/example.trino.io/v1/namespaces/default/widgets",
                """
                {
                  "apiVersion": "example.trino.io/v1",
                  "kind": "Widget",
                  "metadata": {"name": "widget-one"},
                  "spec": {
                    "image": "widget:1.0",
                    "replicas": 3,
                    "size": 2,
                    "activated": "2026-01-02T03:04:05Z",
                    "tags": ["alpha", "beta"],
                    "settings": {"mode": "fast"}
                  }
                }
                """);

        assertEventually(() ->
                assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains("example.trino.io"));

        assertQuery(
                """
                SELECT w.name, w.spec.image, w.spec.replicas, w.spec.size, t.tag, w.spec.settings['mode']
                FROM "example.trino.io".widgets w
                CROSS JOIN UNNEST(w.spec.tags) AS t(tag)
                WHERE w.spec.replicas > 2 AND t.tag = 'alpha'
                """,
                "VALUES ('widget-one', 'widget:1.0', 3, '2', 'alpha', 'fast')");

        Object activated = computeScalar("SELECT w.spec.activated FROM \"example.trino.io\".widgets w WHERE w.name = 'widget-one'");
        assertThat(activated).isInstanceOf(ZonedDateTime.class);
        assertThat(((ZonedDateTime) activated).toInstant()).isEqualTo(Instant.parse("2026-01-02T03:04:05Z"));

        // full DML cycle on the custom resource
        assertUpdate(
                """
                INSERT INTO "example.trino.io".widgets (name, namespace, spec)
                SELECT 'widget-two', 'default', spec FROM "example.trino.io".widgets WHERE name = 'widget-one'
                """,
                1);
        assertThat(cluster.get("/apis/example.trino.io/v1/namespaces/default/widgets/widget-two")
                .path("spec").path("image").asText()).isEqualTo("widget:1.0");

        assertUpdate(
                """
                UPDATE "example.trino.io".widgets
                SET spec = CAST(ROW(TIMESTAMP '2026-05-06 07:08:09.000 UTC', 'widget:2.0', 5, MAP(ARRAY['mode'], ARRAY['slow']), '4', ARRAY['gamma'])
                    AS ROW(activated timestamp(3) with time zone, image varchar, replicas bigint, settings map(varchar, varchar), size varchar, tags array(varchar)))
                WHERE name = 'widget-two'
                """,
                1);
        JsonNode updatedWidget = cluster.get("/apis/example.trino.io/v1/namespaces/default/widgets/widget-two");
        assertThat(updatedWidget.path("spec").path("image").asText()).isEqualTo("widget:2.0");
        assertThat(updatedWidget.path("spec").path("replicas").asLong()).isEqualTo(5);

        assertUpdate("DELETE FROM \"example.trino.io\".widgets WHERE name = 'widget-two'", 1);
        assertThat(cluster.statusCode("/apis/example.trino.io/v1/namespaces/default/widgets/widget-two")).isEqualTo(404);
    }

    @Test
    void testUnknownTable()
    {
        assertQueryFails("SELECT * FROM core.nonexistent_things", ".*Table 'kubernetes.core.nonexistent_things' does not exist.*");
    }
}
