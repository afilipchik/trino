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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.airlift.security.pem.PemReader;
import io.trino.spi.TrinoException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_AUTHENTICATION_ERROR;

/**
 * Parses the subset of kubeconfig used for direct API server access: the current context's
 * cluster address, certificate authority, bearer token, and client certificate credentials.
 */
public final class KubeconfigParser
{
    private KubeconfigParser() {}

    public static KubernetesAuth parse(Path kubeconfigPath)
    {
        JsonNode config;
        try {
            config = new ObjectMapper(new YAMLFactory()).readTree(Files.readString(kubeconfigPath, StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Failed to read kubeconfig: " + kubeconfigPath, e);
        }

        String contextName = config.path("current-context").asText("");
        JsonNode context = namedEntry(config, "contexts", contextName)
                .orElseThrow(() -> new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Kubeconfig has no usable context: " + kubeconfigPath))
                .path("context");

        JsonNode cluster = namedEntry(config, "clusters", context.path("cluster").asText())
                .orElseThrow(() -> new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Kubeconfig context references unknown cluster: " + context.path("cluster").asText()))
                .path("cluster");
        JsonNode user = namedEntry(config, "users", context.path("user").asText())
                .map(entry -> entry.path("user"))
                .orElse(MissingNode.getInstance());

        String server = cluster.path("server").asText("");
        if (server.isEmpty()) {
            throw new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Kubeconfig cluster has no server address");
        }

        Path baseDirectory = kubeconfigPath.toAbsolutePath().getParent();

        Optional<List<X509Certificate>> caCertificates = readCertificates(cluster, "certificate-authority", baseDirectory);
        boolean insecure = cluster.path("insecure-skip-tls-verify").asBoolean(false);

        Optional<String> token = textValue(user, "token");
        if (token.isEmpty()) {
            Optional<String> tokenFile = textValue(user, "tokenFile");
            if (tokenFile.isPresent()) {
                token = Optional.of(readFile(resolve(baseDirectory, tokenFile.get())).strip());
            }
        }

        Optional<List<X509Certificate>> clientCertificates = readCertificates(user, "client-certificate", baseDirectory);
        Optional<PrivateKey> clientKey = readPrivateKey(user, baseDirectory);

        return new KubernetesAuth(URI.create(server), token, clientCertificates, clientKey, caCertificates, insecure);
    }

    private static Optional<JsonNode> namedEntry(JsonNode config, String section, String name)
    {
        JsonNode entries = config.path(section);
        if (!entries.isArray() || entries.isEmpty()) {
            return Optional.empty();
        }
        if (name.isEmpty()) {
            return Optional.of(entries.get(0));
        }
        for (JsonNode entry : entries) {
            if (name.equals(entry.path("name").asText())) {
                return Optional.of(entry);
            }
        }
        return Optional.empty();
    }

    private static Optional<List<X509Certificate>> readCertificates(JsonNode node, String field, Path baseDirectory)
    {
        try {
            Optional<String> data = textValue(node, field + "-data");
            if (data.isPresent()) {
                String pem = new String(Base64.getDecoder().decode(data.get()), StandardCharsets.US_ASCII);
                return Optional.of(PemReader.readCertificateChain(pem));
            }
            Optional<String> path = textValue(node, field);
            if (path.isPresent()) {
                return Optional.of(PemReader.readCertificateChain(new File(resolve(baseDirectory, path.get()).toString())));
            }
            return Optional.empty();
        }
        catch (IOException | GeneralSecurityException e) {
            throw new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Failed to load certificates from kubeconfig field: " + field, e);
        }
    }

    private static Optional<PrivateKey> readPrivateKey(JsonNode user, Path baseDirectory)
    {
        try {
            Optional<String> data = textValue(user, "client-key-data");
            if (data.isPresent()) {
                String pem = new String(Base64.getDecoder().decode(data.get()), StandardCharsets.US_ASCII);
                return Optional.of(PemReader.loadPrivateKey(pem, Optional.empty()));
            }
            Optional<String> path = textValue(user, "client-key");
            if (path.isPresent()) {
                return Optional.of(PemReader.loadPrivateKey(new File(resolve(baseDirectory, path.get()).toString()), Optional.empty()));
            }
            return Optional.empty();
        }
        catch (IOException | GeneralSecurityException e) {
            throw new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Failed to load client key from kubeconfig", e);
        }
    }

    private static Optional<String> textValue(JsonNode node, String field)
    {
        JsonNode value = node.path(field);
        if (value.isTextual() && !value.asText().isEmpty()) {
            return Optional.of(value.asText());
        }
        return Optional.empty();
    }

    private static Path resolve(Path baseDirectory, String path)
    {
        Path resolved = Path.of(path);
        if (!resolved.isAbsolute() && baseDirectory != null) {
            resolved = baseDirectory.resolve(path);
        }
        return resolved;
    }

    private static String readFile(Path path)
    {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new TrinoException(KUBERNETES_AUTHENTICATION_ERROR, "Failed to read file referenced by kubeconfig: " + path, e);
        }
    }
}
