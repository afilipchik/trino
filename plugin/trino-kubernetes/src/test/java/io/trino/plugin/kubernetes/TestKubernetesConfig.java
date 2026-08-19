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
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestKubernetesConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(KubernetesConfig.class)
                .setKubeconfigPath(null)
                .setKubeconfigContext(null)
                .setMultiClusterEnabled(false)
                .setApiServerUri(null)
                .setToken(null)
                .setCaCertificatePath(null)
                .setInsecureTls(false)
                .setDefaultNamespace("default")
                .setMetadataCacheTtl(new Duration(1, TimeUnit.MINUTES))
                .setListPageSize(500));
    }

    @Test
    void testExplicitPropertyMappingsWithApiServer()
            throws IOException
    {
        Path caCertificate = Files.createTempFile("ca", ".pem");

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kubernetes.api-server-uri", "https://127.0.0.1:6443")
                .put("kubernetes.token", "secret-token")
                .put("kubernetes.ca-certificate-path", caCertificate.toString())
                .put("kubernetes.insecure-tls", "true")
                .put("kubernetes.default-namespace", "workloads")
                .put("kubernetes.metadata-cache-ttl", "5m")
                .put("kubernetes.list-page-size", "100")
                .buildOrThrow();

        KubernetesConfig config = new ConfigurationFactory(properties).build(KubernetesConfig.class);
        assertThat(config.getKubeconfigPath()).isEmpty();
        assertThat(config.getApiServerUri()).isEqualTo(Optional.of("https://127.0.0.1:6443"));
        assertThat(config.getToken()).isEqualTo(Optional.of("secret-token"));
        assertThat(config.getCaCertificatePath()).isEqualTo(Optional.of(caCertificate.toString()));
        assertThat(config.isInsecureTls()).isTrue();
        assertThat(config.getDefaultNamespace()).isEqualTo("workloads");
        assertThat(config.getMetadataCacheTtl()).isEqualTo(new Duration(5, TimeUnit.MINUTES));
        assertThat(config.getListPageSize()).isEqualTo(100);
    }

    @Test
    void testExplicitPropertyMappingsWithKubeconfig()
            throws IOException
    {
        Path kubeconfig = Files.createTempFile("kubeconfig", null);

        KubernetesConfig config = new ConfigurationFactory(ImmutableMap.of(
                "kubernetes.kubeconfig-path", kubeconfig.toString(),
                "kubernetes.kubeconfig-context", "staging"))
                .build(KubernetesConfig.class);
        assertThat(config.getKubeconfigPath()).isEqualTo(Optional.of(kubeconfig.toString()));
        assertThat(config.getKubeconfigContext()).isEqualTo(Optional.of("staging"));
        assertThat(config.isMultiClusterEnabled()).isFalse();
        assertThat(config.getApiServerUri()).isEmpty();
    }

    @Test
    void testMultiClusterMapping()
            throws IOException
    {
        Path kubeconfig = Files.createTempFile("kubeconfig", null);

        KubernetesConfig config = new ConfigurationFactory(ImmutableMap.of(
                "kubernetes.kubeconfig-path", kubeconfig.toString(),
                "kubernetes.multi-cluster.enabled", "true"))
                .build(KubernetesConfig.class);
        assertThat(config.isMultiClusterEnabled()).isTrue();
    }

    @Test
    void testConnectionSourceIsExclusive()
            throws IOException
    {
        Path kubeconfig = Files.createTempFile("kubeconfig", null);

        assertThatThrownBy(() -> new ConfigurationFactory(ImmutableMap.of(
                "kubernetes.kubeconfig-path", kubeconfig.toString(),
                "kubernetes.api-server-uri", "https://127.0.0.1:6443"))
                .build(KubernetesConfig.class))
                .hasMessageContaining("Exactly one of 'kubernetes.kubeconfig-path' or 'kubernetes.api-server-uri' must be specified");

        assertThatThrownBy(() -> new ConfigurationFactory(ImmutableMap.<String, String>of())
                .build(KubernetesConfig.class))
                .hasMessageContaining("Exactly one of 'kubernetes.kubeconfig-path' or 'kubernetes.api-server-uri' must be specified");
    }

    @Test
    void testKubeconfigContextRequiresKubeconfig()
    {
        assertThatThrownBy(() -> new ConfigurationFactory(ImmutableMap.of(
                "kubernetes.api-server-uri", "https://127.0.0.1:6443",
                "kubernetes.kubeconfig-context", "staging"))
                .build(KubernetesConfig.class))
                .hasMessageContaining("'kubernetes.kubeconfig-context' requires 'kubernetes.kubeconfig-path'");
    }

    @Test
    void testMultiClusterRequiresKubeconfig()
            throws IOException
    {
        Path kubeconfig = Files.createTempFile("kubeconfig", null);

        assertThatThrownBy(() -> new ConfigurationFactory(ImmutableMap.of(
                "kubernetes.api-server-uri", "https://127.0.0.1:6443",
                "kubernetes.multi-cluster.enabled", "true"))
                .build(KubernetesConfig.class))
                .hasMessageContaining("'kubernetes.multi-cluster.enabled' requires 'kubernetes.kubeconfig-path'");

        assertThatThrownBy(() -> new ConfigurationFactory(ImmutableMap.of(
                "kubernetes.kubeconfig-path", kubeconfig.toString(),
                "kubernetes.kubeconfig-context", "staging",
                "kubernetes.multi-cluster.enabled", "true"))
                .build(KubernetesConfig.class))
                .hasMessageContaining("cannot be combined with 'kubernetes.kubeconfig-context'");
    }
}
