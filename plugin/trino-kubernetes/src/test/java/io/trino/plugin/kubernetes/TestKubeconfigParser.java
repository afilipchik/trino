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

import io.trino.plugin.kubernetes.client.KubeconfigParser;
import io.trino.plugin.kubernetes.client.KubernetesAuth;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestKubeconfigParser
{
    @TempDir
    Path directory;

    private Path writeKubeconfig()
            throws IOException
    {
        Path kubeconfig = directory.resolve("kubeconfig");
        Files.writeString(kubeconfig,
                """
                apiVersion: v1
                kind: Config
                current-context: beta
                clusters:
                  - name: alpha-cluster
                    cluster:
                      server: https://alpha.example.com:6443
                      insecure-skip-tls-verify: true
                  - name: beta-cluster
                    cluster:
                      server: https://beta.example.com:6443
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
                      token: alpha-token
                  - name: beta-user
                    user:
                      token: beta-token
                """);
        return kubeconfig;
    }

    @Test
    void testContextNames()
            throws IOException
    {
        assertThat(KubeconfigParser.contextNames(writeKubeconfig())).containsExactly("alpha", "beta");
    }

    @Test
    void testDefaultContextName()
            throws IOException
    {
        assertThat(KubeconfigParser.defaultContextName(writeKubeconfig())).isEqualTo("beta");
    }

    @Test
    void testParseCurrentContext()
            throws IOException
    {
        KubernetesAuth auth = KubeconfigParser.parse(writeKubeconfig());
        assertThat(auth.serverUri().toString()).isEqualTo("https://beta.example.com:6443");
        assertThat(auth.token()).isEqualTo(Optional.of("beta-token"));
        assertThat(auth.insecureTls()).isTrue();
    }

    @Test
    void testParseNamedContext()
            throws IOException
    {
        KubernetesAuth auth = KubeconfigParser.parse(writeKubeconfig(), Optional.of("alpha"));
        assertThat(auth.serverUri().toString()).isEqualTo("https://alpha.example.com:6443");
        assertThat(auth.token()).isEqualTo(Optional.of("alpha-token"));
    }

    @Test
    void testParseUnknownContext()
            throws IOException
    {
        Path kubeconfig = writeKubeconfig();
        assertThatThrownBy(() -> KubeconfigParser.parse(kubeconfig, Optional.of("gamma")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Kubeconfig has no context named 'gamma'");
    }
}
