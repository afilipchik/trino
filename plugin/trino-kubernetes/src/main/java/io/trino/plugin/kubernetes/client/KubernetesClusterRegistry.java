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
package io.trino.plugin.kubernetes.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.security.pem.PemReader;
import io.trino.plugin.kubernetes.KubernetesConfig;
import io.trino.spi.TrinoException;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_AUTHENTICATION_ERROR;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_INVALID_WRITE;

/**
 * The clusters served by a catalog, each with its own {@link KubernetesClient}.
 *
 * <p>With {@code kubernetes.multi-cluster.enabled} every kubeconfig context becomes a
 * cluster, named by its context name; the default cluster (the kubeconfig current
 * context) provides table metadata. Otherwise the registry holds the single configured
 * cluster: the selected kubeconfig context, or the directly configured API server
 * under the name {@code default}.
 */
public class KubernetesClusterRegistry
        implements AutoCloseable
{
    private static final String DIRECT_CLUSTER_NAME = "default";

    private final Map<String, KubernetesClient> clients;
    private final String defaultClusterName;
    private final boolean clusterColumnEnabled;

    @Inject
    public KubernetesClusterRegistry(KubernetesConfig config)
    {
        this.clusterColumnEnabled = config.isMultiClusterEnabled();
        if (config.getKubeconfigPath().isEmpty()) {
            this.defaultClusterName = DIRECT_CLUSTER_NAME;
            this.clients = ImmutableMap.of(DIRECT_CLUSTER_NAME, new KubernetesClient(directAuth(config), config));
            return;
        }

        Path kubeconfigPath = Path.of(config.getKubeconfigPath().get());
        if (config.isMultiClusterEnabled()) {
            List<String> contextNames = KubeconfigParser.contextNames(kubeconfigPath);
            if (contextNames.isEmpty()) {
                throw new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Kubeconfig has no usable context: " + kubeconfigPath);
            }
            ImmutableMap.Builder<String, KubernetesClient> builder = ImmutableMap.builder();
            for (String contextName : contextNames) {
                builder.put(contextName, new KubernetesClient(KubeconfigParser.parse(kubeconfigPath, Optional.of(contextName)), config));
            }
            this.clients = builder.buildOrThrow();
            this.defaultClusterName = KubeconfigParser.defaultContextName(kubeconfigPath);
        }
        else {
            String contextName = config.getKubeconfigContext()
                    .orElseGet(() -> KubeconfigParser.defaultContextName(kubeconfigPath));
            this.defaultClusterName = contextName;
            this.clients = ImmutableMap.of(contextName, new KubernetesClient(KubeconfigParser.parse(kubeconfigPath, Optional.of(contextName)), config));
        }
    }

    private static KubernetesAuth directAuth(KubernetesConfig config)
    {
        Optional<List<X509Certificate>> caCertificates = Optional.empty();
        if (config.getCaCertificatePath().isPresent()) {
            try {
                caCertificates = Optional.of(PemReader.readCertificateChain(new File(config.getCaCertificatePath().get())));
            }
            catch (IOException | GeneralSecurityException e) {
                throw new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Failed to load CA certificate: " + config.getCaCertificatePath().get(), e);
            }
        }
        return new KubernetesAuth(
                URI.create(config.getApiServerUri().orElseThrow()),
                config.getToken(),
                Optional.empty(),
                Optional.empty(),
                caCertificates,
                config.isInsecureTls());
    }

    /**
     * Whether every table carries the synthetic {@code cluster} column and reads fan
     * out to all clusters.
     */
    public boolean isClusterColumnEnabled()
    {
        return clusterColumnEnabled;
    }

    public String defaultClusterName()
    {
        return defaultClusterName;
    }

    public KubernetesClient defaultClient()
    {
        return clients.get(defaultClusterName);
    }

    public List<String> clusterNames()
    {
        return ImmutableList.copyOf(clients.keySet());
    }

    public KubernetesClient client(String clusterName)
    {
        KubernetesClient client = clients.get(clusterName);
        if (client == null) {
            throw new TrinoException(KUBERNETES_INVALID_WRITE, "Unknown cluster '%s'; available clusters: %s".formatted(clusterName, clusterNames()));
        }
        return client;
    }

    @PreDestroy
    @Override
    public void close()
    {
        clients.values().forEach(KubernetesClient::close);
    }
}
