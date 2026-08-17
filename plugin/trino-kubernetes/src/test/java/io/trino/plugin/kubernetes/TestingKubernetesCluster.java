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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.lang.String.join;

/**
 * A real Kubernetes control plane (etcd + kube-apiserver) started as local child
 * processes from envtest binaries — no containers required. Authentication uses a
 * static bearer token with cluster-admin rights.
 *
 * <p>Binaries are located through the {@code TESTING_KUBERNETES_ASSETS} or
 * {@code KUBEBUILDER_ASSETS} environment variable (or the
 * {@code testing.kubernetes.assets} system property) pointing at a directory
 * containing {@code etcd} and {@code kube-apiserver}.
 */
public final class TestingKubernetesCluster
        implements Closeable
{
    private static final Logger log = Logger.get(TestingKubernetesCluster.class);
    private static final ObjectMapper MAPPER = new JsonMapperProvider().get();

    private final Path workDirectory;
    private final Process etcd;
    private final Process apiServer;
    private final URI uri;
    private final String token;
    private final HttpClient httpClient;

    public static Optional<Path> assetsDirectory()
    {
        for (String value : new String[] {
                System.getProperty("testing.kubernetes.assets"),
                System.getenv("TESTING_KUBERNETES_ASSETS"),
                System.getenv("KUBEBUILDER_ASSETS")}) {
            if (value != null && !value.isEmpty()) {
                Path path = Path.of(value);
                if (Files.isExecutable(path.resolve("kube-apiserver")) && Files.isExecutable(path.resolve("etcd"))) {
                    return Optional.of(path);
                }
            }
        }
        return Optional.empty();
    }

    public static boolean isAvailable()
    {
        return assetsDirectory().isPresent();
    }

    public static TestingKubernetesCluster create()
    {
        Path assets = assetsDirectory().orElseThrow(() ->
                new IllegalStateException("Kubernetes envtest binaries not found; set TESTING_KUBERNETES_ASSETS to a directory with etcd and kube-apiserver"));
        try {
            return new TestingKubernetesCluster(assets);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private TestingKubernetesCluster(Path assets)
            throws IOException
    {
        workDirectory = Files.createTempDirectory("trino-kubernetes-test");
        Path certsDirectory = Files.createDirectory(workDirectory.resolve("certs"));
        Path etcdData = Files.createDirectory(workDirectory.resolve("etcd-data"));

        token = "testing-" + UUID.randomUUID();
        Path tokenFile = workDirectory.resolve("tokens.csv");
        Files.writeString(tokenFile, "%s,testing,testing-id,\"system:masters\"\n".formatted(token), StandardCharsets.UTF_8);

        writeServiceAccountKeys(certsDirectory);

        int etcdClientPort = freePort();
        int etcdPeerPort = freePort();
        int apiServerPort = freePort();
        uri = URI.create("https://127.0.0.1:" + apiServerPort);

        etcd = start(
                workDirectory.resolve("etcd.log"),
                assets.resolve("etcd").toString(),
                "--data-dir", etcdData.toString(),
                "--listen-client-urls", "http://127.0.0.1:" + etcdClientPort,
                "--advertise-client-urls", "http://127.0.0.1:" + etcdClientPort,
                "--listen-peer-urls", "http://127.0.0.1:" + etcdPeerPort);

        apiServer = start(
                workDirectory.resolve("kube-apiserver.log"),
                assets.resolve("kube-apiserver").toString(),
                "--etcd-servers=http://127.0.0.1:" + etcdClientPort,
                "--secure-port=" + apiServerPort,
                "--bind-address=127.0.0.1",
                "--cert-dir=" + certsDirectory,
                "--token-auth-file=" + tokenFile,
                "--authorization-mode=AlwaysAllow",
                "--service-account-issuer=https://kubernetes.default.svc",
                "--service-account-key-file=" + certsDirectory.resolve("sa.pub"),
                "--service-account-signing-key-file=" + certsDirectory.resolve("sa.key"),
                "--disable-admission-plugins=ServiceAccount",
                "--allow-privileged=true");

        httpClient = HttpClient.newBuilder()
                .sslContext(insecureSslContext())
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        awaitReady();
    }

    private static void writeServiceAccountKeys(Path certsDirectory)
            throws IOException
    {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            KeyPair keyPair = generator.generateKeyPair();
            Base64.Encoder encoder = Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII));
            Files.writeString(certsDirectory.resolve("sa.key"),
                    "-----BEGIN PRIVATE KEY-----\n" + encoder.encodeToString(keyPair.getPrivate().getEncoded()) + "\n-----END PRIVATE KEY-----\n");
            Files.writeString(certsDirectory.resolve("sa.pub"),
                    "-----BEGIN PUBLIC KEY-----\n" + encoder.encodeToString(keyPair.getPublic().getEncoded()) + "\n-----END PUBLIC KEY-----\n");
        }
        catch (GeneralSecurityException e) {
            throw new IOException("Failed to generate service account keys", e);
        }
    }

    private static Process start(Path logFile, String... command)
            throws IOException
    {
        return new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(logFile.toFile())
                .start();
    }

    private static int freePort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void awaitReady()
            throws IOException
    {
        Instant deadline = Instant.now().plusSeconds(90);
        IOException lastFailure = null;
        while (Instant.now().isBefore(deadline)) {
            if (!apiServer.isAlive()) {
                throw new IOException("kube-apiserver exited with code " + apiServer.exitValue() + "\n" + logTail("kube-apiserver.log"));
            }
            try {
                HttpResponse<String> response = send("GET", "/readyz", Optional.empty());
                if (response.statusCode() == 200) {
                    return;
                }
            }
            catch (IOException e) {
                lastFailure = e;
            }
            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for kube-apiserver", e);
            }
        }
        throw new IOException("kube-apiserver did not become ready\n" + logTail("kube-apiserver.log"), lastFailure);
    }

    private String logTail(String name)
    {
        try {
            List<String> lines = Files.readAllLines(workDirectory.resolve(name), StandardCharsets.UTF_8);
            return join("\n", lines.subList(Math.max(0, lines.size() - 30), lines.size()));
        }
        catch (IOException e) {
            return "(no log available)";
        }
    }

    public URI uri()
    {
        return uri;
    }

    public String token()
    {
        return token;
    }

    public JsonNode get(String path)
    {
        return parse(expectSuccess(sendUnchecked("GET", path, Optional.empty())));
    }

    public int statusCode(String path)
    {
        return sendUnchecked("GET", path, Optional.empty()).statusCode();
    }

    public JsonNode create(String path, String json)
    {
        return parse(expectSuccess(sendUnchecked("POST", path, Optional.of(json))));
    }

    public JsonNode replace(String path, String json)
    {
        return parse(expectSuccess(sendUnchecked("PUT", path, Optional.of(json))));
    }

    public void delete(String path)
    {
        expectSuccess(sendUnchecked("DELETE", path, Optional.empty()));
    }

    private static HttpResponse<String> expectSuccess(HttpResponse<String> response)
    {
        if (response.statusCode() >= 400) {
            throw new IllegalStateException("Kubernetes API error %s for %s: %s".formatted(response.statusCode(), response.request().uri(), response.body()));
        }
        return response;
    }

    private static JsonNode parse(HttpResponse<String> response)
    {
        try {
            return MAPPER.readTree(response.body());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private HttpResponse<String> sendUnchecked(String method, String path, Optional<String> body)
    {
        try {
            return send(method, path, body);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private HttpResponse<String> send(String method, String path, Optional<String> body)
            throws IOException
    {
        HttpRequest.Builder request = HttpRequest.newBuilder(uri.resolve(path))
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", "Bearer " + token)
                .header("Accept", "application/json");
        if (body.isPresent()) {
            request.header("Content-Type", "application/json");
            request.method(method, HttpRequest.BodyPublishers.ofString(body.get(), StandardCharsets.UTF_8));
        }
        else {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        }
        try {
            return httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }

    private static SSLContext insecureSslContext()
    {
        try {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {new TrustAllManager()}, null);
            return sslContext;
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        stop(apiServer);
        stop(etcd);
        try (var files = Files.walk(workDirectory)) {
            files.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                }
                catch (IOException e) {
                    log.warn("Failed to delete %s", path);
                }
            });
        }
        catch (IOException e) {
            log.warn(e, "Failed to clean up %s", workDirectory);
        }
    }

    private static void stop(Process process)
    {
        process.destroy();
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private static final class TrustAllManager
            extends X509ExtendedTrustManager
    {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {}

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {}

        @Override
        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }
    }
}
