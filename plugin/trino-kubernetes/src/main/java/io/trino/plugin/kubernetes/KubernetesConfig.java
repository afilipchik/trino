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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class KubernetesConfig
{
    private Optional<String> kubeconfigPath = Optional.empty();
    private Optional<String> kubeconfigContext = Optional.empty();
    private boolean multiClusterEnabled;
    private Optional<String> apiServerUri = Optional.empty();
    private Optional<String> token = Optional.empty();
    private Optional<String> caCertificatePath = Optional.empty();
    private boolean insecureTls;
    private String defaultNamespace = "default";
    private Duration metadataCacheTtl = new Duration(1, TimeUnit.MINUTES);
    private int listPageSize = 500;

    @NotNull
    public Optional<@FileExists String> getKubeconfigPath()
    {
        return kubeconfigPath;
    }

    @Config("kubernetes.kubeconfig-path")
    @ConfigDescription("Path to a kubeconfig file used to locate and authenticate to the cluster")
    public KubernetesConfig setKubeconfigPath(String kubeconfigPath)
    {
        this.kubeconfigPath = Optional.ofNullable(kubeconfigPath);
        return this;
    }

    @NotNull
    public Optional<String> getKubeconfigContext()
    {
        return kubeconfigContext;
    }

    @Config("kubernetes.kubeconfig-context")
    @ConfigDescription("Kubeconfig context to use instead of the current context")
    public KubernetesConfig setKubeconfigContext(String kubeconfigContext)
    {
        this.kubeconfigContext = Optional.ofNullable(kubeconfigContext);
        return this;
    }

    public boolean isMultiClusterEnabled()
    {
        return multiClusterEnabled;
    }

    @Config("kubernetes.multi-cluster.enabled")
    @ConfigDescription("Serve all kubeconfig contexts through one catalog with a synthetic cluster column and query-time fan-out")
    public KubernetesConfig setMultiClusterEnabled(boolean multiClusterEnabled)
    {
        this.multiClusterEnabled = multiClusterEnabled;
        return this;
    }

    @NotNull
    public Optional<String> getApiServerUri()
    {
        return apiServerUri;
    }

    @Config("kubernetes.api-server-uri")
    @ConfigDescription("URI of the Kubernetes API server, for example https://127.0.0.1:6443")
    public KubernetesConfig setApiServerUri(String apiServerUri)
    {
        this.apiServerUri = Optional.ofNullable(apiServerUri);
        return this;
    }

    @NotNull
    public Optional<String> getToken()
    {
        return token;
    }

    @Config("kubernetes.token")
    @ConfigDescription("Bearer token used to authenticate to the API server")
    @ConfigSecuritySensitive
    public KubernetesConfig setToken(String token)
    {
        this.token = Optional.ofNullable(token);
        return this;
    }

    @NotNull
    public Optional<@FileExists String> getCaCertificatePath()
    {
        return caCertificatePath;
    }

    @Config("kubernetes.ca-certificate-path")
    @ConfigDescription("Path to a PEM file with the certificate authority of the API server")
    public KubernetesConfig setCaCertificatePath(String caCertificatePath)
    {
        this.caCertificatePath = Optional.ofNullable(caCertificatePath);
        return this;
    }

    public boolean isInsecureTls()
    {
        return insecureTls;
    }

    @Config("kubernetes.insecure-tls")
    @ConfigDescription("Skip verification of the API server TLS certificate")
    public KubernetesConfig setInsecureTls(boolean insecureTls)
    {
        this.insecureTls = insecureTls;
        return this;
    }

    @NotNull
    public String getDefaultNamespace()
    {
        return defaultNamespace;
    }

    @Config("kubernetes.default-namespace")
    @ConfigDescription("Namespace used for inserted objects that do not specify one")
    public KubernetesConfig setDefaultNamespace(String defaultNamespace)
    {
        this.defaultNamespace = defaultNamespace;
        return this;
    }

    @NotNull
    @MinDuration("0s")
    public Duration getMetadataCacheTtl()
    {
        return metadataCacheTtl;
    }

    @Config("kubernetes.metadata-cache-ttl")
    @ConfigDescription("How long to cache API discovery and OpenAPI schema information")
    public KubernetesConfig setMetadataCacheTtl(Duration metadataCacheTtl)
    {
        this.metadataCacheTtl = metadataCacheTtl;
        return this;
    }

    @Min(1)
    public int getListPageSize()
    {
        return listPageSize;
    }

    @Config("kubernetes.list-page-size")
    @ConfigDescription("Number of objects requested per page from the API server list endpoints")
    public KubernetesConfig setListPageSize(int listPageSize)
    {
        this.listPageSize = listPageSize;
        return this;
    }

    @AssertTrue(message = "Exactly one of 'kubernetes.kubeconfig-path' or 'kubernetes.api-server-uri' must be specified")
    public boolean isConnectionConfigurationValid()
    {
        return kubeconfigPath.isPresent() ^ apiServerUri.isPresent();
    }

    @AssertTrue(message = "'kubernetes.kubeconfig-context' requires 'kubernetes.kubeconfig-path'")
    public boolean isKubeconfigContextValid()
    {
        return kubeconfigContext.isEmpty() || kubeconfigPath.isPresent();
    }

    @AssertTrue(message = "'kubernetes.multi-cluster.enabled' requires 'kubernetes.kubeconfig-path' and cannot be combined with 'kubernetes.kubeconfig-context'")
    public boolean isMultiClusterConfigurationValid()
    {
        return !multiClusterEnabled || (kubeconfigPath.isPresent() && kubeconfigContext.isEmpty());
    }
}
