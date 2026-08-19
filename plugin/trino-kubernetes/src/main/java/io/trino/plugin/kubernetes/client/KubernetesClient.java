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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.json.JsonMapperProvider;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.kubernetes.KubernetesConfig;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_CLIENT_ERROR;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_INVALID_WRITE;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_RESOURCE_NOT_FOUND;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_SCHEMA_ERROR;
import static io.trino.plugin.kubernetes.KubernetesErrorCode.KUBERNETES_WRITE_CONFLICT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Minimal Kubernetes API client for a single cluster: discovery, OpenAPI v3 schemas,
 * list/create/replace/delete. All payloads are raw JSON trees; the connector is fully
 * dynamic over resource types. Instances are created and owned by
 * {@link KubernetesClusterRegistry}.
 */
public class KubernetesClient
        implements AutoCloseable
{
    private static final String CORE_SCHEMA_NAME = "core";
    private static final Set<String> REQUIRED_VERBS = Set.of("get", "list");

    private final HttpClient httpClient;
    private final URI baseUri;
    private final Optional<String> token;
    private final ObjectMapper mapper = new JsonMapperProvider().get();
    private final Supplier<Map<String, List<ResourceDescriptor>>> discovery;
    private final Cache<String, JsonNode> openApiDocuments;

    public KubernetesClient(KubernetesAuth auth, KubernetesConfig config)
    {
        requireNonNull(auth, "auth is null");
        requireNonNull(config, "config is null");
        this.baseUri = auth.serverUri();
        this.token = auth.token();
        this.httpClient = HttpClient.newBuilder()
                .sslContext(auth.createSslContext())
                .connectTimeout(Duration.ofSeconds(30))
                .build();
        long ttlMillis = config.getMetadataCacheTtl().toMillis();
        this.discovery = Suppliers.memoizeWithExpiration(this::loadDiscovery, ttlMillis, MILLISECONDS);
        this.openApiDocuments = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(ttlMillis, MILLISECONDS)
                .maximumSize(1000)
                .build();
    }

    public Map<String, List<ResourceDescriptor>> resourcesBySchema()
    {
        return discovery.get();
    }

    public Optional<ResourceDescriptor> resource(String schemaName, String tableName)
    {
        return resourcesBySchema().getOrDefault(schemaName, ImmutableList.of()).stream()
                .filter(resource -> resource.tableName().equals(tableName))
                .findFirst();
    }

    public JsonNode openApiDocument(ResourceDescriptor resource)
    {
        String key = resource.groupVersionPath();
        try {
            return openApiDocuments.get(key, () -> loadOpenApiDocument(key));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), TrinoException.class);
            throw new TrinoException(KUBERNETES_SCHEMA_ERROR, "Failed to load OpenAPI schema for " + key, e.getCause());
        }
    }

    private JsonNode loadOpenApiDocument(String groupVersionPath)
    {
        JsonNode index = get("/openapi/v3");
        JsonNode entry = index.path("paths").path(groupVersionPath).path("serverRelativeURL");
        if (!entry.isTextual()) {
            throw new TrinoException(KUBERNETES_SCHEMA_ERROR, "API server has no OpenAPI v3 document for " + groupVersionPath);
        }
        return get(entry.asText());
    }

    public ObjectListPage listObjects(ResourceDescriptor resource, Optional<String> namespace, Optional<String> nameSelector, int limit, Optional<String> continueToken)
    {
        StringBuilder query = new StringBuilder("?limit=").append(limit);
        continueToken.ifPresent(value -> query.append("&continue=").append(urlEncode(value)));
        nameSelector.ifPresent(value -> query.append("&fieldSelector=").append(urlEncode("metadata.name=" + value)));

        JsonNode response = get(collectionPath(resource, namespace) + query);
        ImmutableList.Builder<JsonNode> items = ImmutableList.builder();
        for (JsonNode item : response.path("items")) {
            items.add(item);
        }
        String nextToken = response.path("metadata").path("continue").asText("");
        return new ObjectListPage(items.build(), nextToken.isEmpty() ? Optional.empty() : Optional.of(nextToken));
    }

    public JsonNode createObject(ResourceDescriptor resource, Optional<String> namespace, JsonNode object)
    {
        return send("POST", collectionPath(resource, namespace), Optional.of(object));
    }

    public JsonNode replaceObject(ResourceDescriptor resource, Optional<String> namespace, String name, JsonNode object)
    {
        return send("PUT", collectionPath(resource, namespace) + "/" + urlEncode(name), Optional.of(object));
    }

    public void deleteObject(ResourceDescriptor resource, Optional<String> namespace, String name)
    {
        send("DELETE", collectionPath(resource, namespace) + "/" + urlEncode(name), Optional.empty());
    }

    private static String collectionPath(ResourceDescriptor resource, Optional<String> namespace)
    {
        StringBuilder path = new StringBuilder("/").append(resource.groupVersionPath());
        if (resource.namespaced() && namespace.isPresent()) {
            path.append("/namespaces/").append(urlEncode(namespace.get()));
        }
        return path.append("/").append(resource.tableName()).toString();
    }

    private Map<String, List<ResourceDescriptor>> loadDiscovery()
    {
        ImmutableMap.Builder<String, List<ResourceDescriptor>> schemas = ImmutableMap.builder();
        schemas.put(CORE_SCHEMA_NAME, loadResources("", "v1"));

        for (JsonNode group : get("/apis").path("groups")) {
            String groupName = group.path("name").asText();
            String version = group.path("preferredVersion").path("version").asText();
            if (groupName.isEmpty() || version.isEmpty() || groupName.equals(CORE_SCHEMA_NAME)) {
                continue;
            }
            List<ResourceDescriptor> resources = loadResources(groupName, version);
            if (!resources.isEmpty()) {
                schemas.put(groupName.toLowerCase(Locale.ENGLISH), resources);
            }
        }
        return schemas.buildKeepingLast();
    }

    private List<ResourceDescriptor> loadResources(String group, String version)
    {
        String schemaName = group.isEmpty() ? CORE_SCHEMA_NAME : group.toLowerCase(Locale.ENGLISH);
        String path = group.isEmpty() ? "/api/" + version : "/apis/" + group + "/" + version;
        ImmutableList.Builder<ResourceDescriptor> resources = ImmutableList.builder();
        for (JsonNode resource : get(path).path("resources")) {
            String name = resource.path("name").asText();
            if (name.isEmpty() || name.contains("/")) {
                continue;
            }
            Set<String> verbs = new HashSet<>();
            for (JsonNode verb : resource.path("verbs")) {
                verbs.add(verb.asText());
            }
            if (!verbs.containsAll(REQUIRED_VERBS)) {
                continue;
            }
            resources.add(new ResourceDescriptor(
                    schemaName,
                    name.toLowerCase(Locale.ENGLISH),
                    group,
                    version,
                    resource.path("kind").asText(),
                    resource.path("namespaced").asBoolean()));
        }
        return resources.build();
    }

    private JsonNode get(String pathAndQuery)
    {
        return send("GET", pathAndQuery, Optional.empty());
    }

    private JsonNode send(String method, String pathAndQuery, Optional<JsonNode> body)
    {
        HttpRequest.Builder request = HttpRequest.newBuilder(baseUri.resolve(pathAndQuery))
                .timeout(Duration.ofMinutes(2))
                .header("Accept", "application/json");
        token.ifPresent(value -> request.header("Authorization", "Bearer " + value));
        if (body.isPresent()) {
            request.header("Content-Type", "application/json");
            request.method(method, HttpRequest.BodyPublishers.ofString(body.get().toString(), StandardCharsets.UTF_8));
        }
        else {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        }

        HttpResponse<String> response;
        try {
            response = httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            throw new TrinoException(KUBERNETES_CLIENT_ERROR, "Kubernetes API request failed: %s %s".formatted(method, pathAndQuery), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(KUBERNETES_CLIENT_ERROR, "Kubernetes API request interrupted", e);
        }

        if (response.statusCode() >= 400) {
            throw statusException(method, pathAndQuery, response);
        }
        try {
            return mapper.readTree(response.body());
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(KUBERNETES_CLIENT_ERROR, "Kubernetes API returned malformed JSON for %s %s".formatted(method, pathAndQuery), e);
        }
    }

    private TrinoException statusException(String method, String pathAndQuery, HttpResponse<String> response)
    {
        String message = "";
        try {
            message = mapper.readTree(response.body()).path("message").asText("");
        }
        catch (JsonProcessingException _) {
        }
        if (message.isEmpty()) {
            message = response.body();
        }
        String description = "Kubernetes API error %s for %s %s: %s".formatted(response.statusCode(), method, pathAndQuery, message);
        return switch (response.statusCode()) {
            case 404 -> new TrinoException(KUBERNETES_RESOURCE_NOT_FOUND, description);
            case 409 -> new TrinoException(KUBERNETES_WRITE_CONFLICT, description);
            case 400, 422 -> new TrinoException(KUBERNETES_INVALID_WRITE, description);
            default -> new TrinoException(KUBERNETES_CLIENT_ERROR, description);
        };
    }

    private static String urlEncode(String value)
    {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    @Override
    public void close()
    {
        httpClient.close();
    }

    public record ObjectListPage(List<JsonNode> items, Optional<String> continueToken)
    {
        public ObjectListPage
        {
            items = ImmutableList.copyOf(items);
            requireNonNull(continueToken, "continueToken is null");
        }
    }
}
