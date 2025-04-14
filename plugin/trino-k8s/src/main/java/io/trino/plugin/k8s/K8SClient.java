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
package io.trino.plugin.k8s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.openapi.apis.ApiextensionsV1Api;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinitionList;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1ServiceStatus;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.models.V1DeploymentList;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1Status;

import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.StandardTypes.JSON;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.Iterator;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

import com.google.gson.annotations.SerializedName;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Method;
import java.util.function.Function;
import java.util.ArrayList;
import java.util.function.Supplier;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.models.V1JobList;
import io.kubernetes.client.openapi.models.V1CronJobList;
import io.kubernetes.client.openapi.models.V1ConfigMapList;
import io.kubernetes.client.openapi.models.V1SecretList;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimList;
import io.kubernetes.client.openapi.apis.AutoscalingV1Api;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscalerList;
import io.kubernetes.client.openapi.apis.NetworkingV1Api;
import io.kubernetes.client.openapi.models.V1IngressList;
import io.kubernetes.client.openapi.models.V1NetworkPolicyList;
import io.kubernetes.client.openapi.apis.RbacAuthorizationV1Api;
import io.kubernetes.client.openapi.models.V1RoleBindingList;
import io.kubernetes.client.openapi.models.V1RoleList;
import io.kubernetes.client.openapi.models.V1ServiceAccountList;
import io.kubernetes.client.openapi.models.V1EndpointsList;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1PersistentVolumeList;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.CoreV1EventList;
import io.kubernetes.client.openapi.models.V1ResourceQuotaList;
import io.kubernetes.client.openapi.models.V1LimitRangeList;
import io.kubernetes.client.openapi.models.V1ReplicaSet;
import io.kubernetes.client.openapi.models.V1ReplicaSetList;
import io.kubernetes.client.openapi.models.V1StatefulSet;
import io.kubernetes.client.openapi.models.V1StatefulSetList;
import io.kubernetes.client.openapi.models.V1DaemonSet;
import io.kubernetes.client.openapi.models.V1DaemonSetList;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1CronJob;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1HorizontalPodAutoscaler;
import io.kubernetes.client.openapi.models.V1Ingress;
import io.kubernetes.client.openapi.models.V1NetworkPolicy;
import io.kubernetes.client.openapi.models.V1RoleBinding;
import io.kubernetes.client.openapi.models.V1Role;
import io.kubernetes.client.openapi.models.V1ServiceAccount;
import io.kubernetes.client.openapi.models.V1Endpoints;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1ResourceQuota;
import io.kubernetes.client.openapi.models.V1LimitRange;
import java.util.Date;
import java.util.Arrays;
import io.kubernetes.client.openapi.models.V1JSONSchemaProps;
import io.trino.spi.type.JsonType;
import static java.lang.String.format;

public class K8SClient
{
    private final K8SConfig config;
    private final TypeManager typeManager;
    private final Map<String, RowType> resourceTypes = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private static final Set<String> NAMESPACED_RESOURCES = ImmutableSet.of(
            "pods", "services", "deployments", "replicasets", "statefulsets",
            "daemonsets", "jobs", "cronjobs", "configmaps", "secrets",
            "persistentvolumeclaims", "horizontalpodautoscalers", "ingresses",
            "networkpolicies", "rolebindings", "roles", "serviceaccounts",
            "endpoints", "resourcequotas", "limitranges");

    private static final Set<String> CLUSTER_SCOPED_RESOURCES = ImmutableSet.of(
            "nodes",
            "persistentvolumes",
            "namespaces",
            "events",
            "resourcequotas",
            "limitranges");

    private final Map<String, ApiClient> contextClients = new ConcurrentHashMap<>();
    private final Map<String, KubeConfig> contextConfigs = new ConcurrentHashMap<>();
    private final Map<String, Map<String, K8STable>> schemas = new ConcurrentHashMap<>();
    private final Set<String> availableContexts = new HashSet<>();
    private final JsonCodec<Map<String, Object>> jsonCodec = JsonCodec.mapJsonCodec(String.class, Object.class);

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

    @Inject
    public K8SClient(K8SConfig config, TypeManager typeManager)
    {
        this.config = requireNonNull(config, "config is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        // Initialize ObjectMapper with JSR310 module for Java 8 date/time support
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());

        try {
            KubeConfig baseConfig = KubeConfig.loadKubeConfig(Files.newBufferedReader(Paths.get(System.getProperty("user.home"), ".kube", "config")));
            // Only load context names initially
            baseConfig.getContexts().stream()
                    .map(context -> ((Map<String, Object>) context).get("name").toString())
                    .forEach(availableContexts::add);
        }
        catch (Exception e) {
            throw new UncheckedIOException("Failed to load Kubernetes config", new IOException(e));
        }
    }

    private Map<String, K8STable> initializeSchema(String context)
    {
        ImmutableMap.Builder<String, K8STable> tables = ImmutableMap.builder();

        // Add standard Kubernetes resources
        for (String resource : NAMESPACED_RESOURCES) {
            tables.put(resource, new K8STable(resource, getTableColumns(context, resource), ImmutableList.of()));
        }

        // Add CRDs
        try {
            ApiClient client = getClientForContext(context);
            ApiextensionsV1Api apiExtensionsApi = new ApiextensionsV1Api(client);
            V1CustomResourceDefinitionList crdList = apiExtensionsApi.listCustomResourceDefinition(null, null, null, null, null, null, null, null, null, null);
            
            for (V1CustomResourceDefinition crd : crdList.getItems()) {
                K8sOpenApiSchema schema = extractSchemaFromCrd(crd);
                if (schema != null) {
                    K8STable table = K8STable.fromCrd(crd, schema);
                    tables.put(table.getName(), table);
                }
            }
        }
        catch (ApiException e) {
            handleApiException(e, "list CRDs");
        }

        return tables.buildOrThrow();
    }

    private List<K8SColumn> getTableColumns(String schema, String table)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");

        ImmutableList.Builder<K8SColumn> columns = ImmutableList.builder();
        Set<String> addedColumns = new HashSet<>();

        // Add standard Kubernetes resource fields
        if (isNamespacedResource(table)) {
            columns.add(new K8SColumn("namespace", VARCHAR));
            addedColumns.add("namespace");
        }

        if (!addedColumns.contains("name")) {
            columns.add(new K8SColumn("name", VARCHAR));
            addedColumns.add("name");
        }

        // Get the resource type using reflection
        RowType resourceType = getResourceType(table);

        // Add fields from the resource type
        for (RowType.Field field : resourceType.getFields()) {
            String fieldName = field.getName().orElse("field");
            if (!addedColumns.contains(fieldName)) {
                columns.add(new K8SColumn(fieldName, field.getType()));
                addedColumns.add(fieldName);
            }
        }

        return columns.build();
    }

    private ApiClient getClientForContext(String context)
    {
        return contextClients.computeIfAbsent(context, ctx -> {
            try {
                KubeConfig contextConfig = contextConfigs.computeIfAbsent(ctx, k -> {
                    try {
                        KubeConfig config = KubeConfig.loadKubeConfig(Files.newBufferedReader(Paths.get(System.getProperty("user.home"), ".kube", "config")));
                        config.setContext(k);
                        return config;
                    }
                    catch (UncheckedIOException e) {
                        throw e;
                    }
                    catch (Exception e) {
                        throw new UncheckedIOException("Failed to load config for context: " + k, new IOException(e));
                    }
                });
                ApiClient client = Config.fromConfig(contextConfig);
                // Set a reasonable timeout
                client.setConnectTimeout(30000);
                client.setReadTimeout(30000);
                client.setWriteTimeout(30000);
                return client;
            }
            catch (UncheckedIOException e) {
                throw e;
            }
            catch (Exception e) {
                throw new UncheckedIOException("Failed to create API client for context: " + ctx, new IOException(e));
            }
        });
    }

    private K8sOpenApiSchema extractSchemaFromCrd(V1CustomResourceDefinition crd) {
        try {
            // Get the OpenAPI v3 schema from the CRD
            V1JSONSchemaProps schema = crd.getSpec().getVersions().get(0).getSchema().getOpenAPIV3Schema();
            if (schema == null) {
                return null;
            }

            // Convert the schema to JSON string
            String schemaJson = objectMapper.writeValueAsString(schema);
            
            // Parse it into our schema class
            return objectMapper.readValue(schemaJson, K8sOpenApiSchema.class);
        }
        catch (Exception e) {
            // Log warning but don't fail - we'll skip this CRD
            System.err.println("Failed to extract schema from CRD " + crd.getMetadata().getName() + ": " + e.getMessage());
            return null;
        }
    }

    public List<Map<String, Object>> getCustomResourceAsList(String context, K8sCustomResourceTable table) {
        try {
            ApiClient client = getClientForContext(context);
            CustomObjectsApi customApi = new CustomObjectsApi(client);

            Object result;
            if (table.isNamespaced()) {
                result = customApi.listClusterCustomObject(
                    table.getGroup(),
                    table.getVersion(),
                    table.getPlural(),
                    null, false, null, null, null, null, null);
            }
            else {
                result = customApi.listNamespacedCustomObject(
                    table.getGroup(),
                    table.getVersion(),
                    "", // all namespaces
                    table.getPlural(),
                    null, false, null, null, null, null, null, null);
            }

            // Convert the result to a list of maps
            Map<String, Object> resultMap = (Map<String, Object>) result;
            List<Map<String, Object>> items = (List<Map<String, Object>>) resultMap.get("items");

            return items.stream()
                .map(item -> {
                    Map<String, Object> row = new HashMap<>();
                    
                    // Extract metadata fields
                    Map<String, Object> metadata = (Map<String, Object>) item.get("metadata");
                    if (metadata != null) {
                        row.put("name", metadata.get("name"));
                        if (table.isNamespaced()) {
                            row.put("namespace", metadata.get("namespace"));
                        }
                    }

                    // Extract spec fields based on schema
                    Map<String, Object> spec = (Map<String, Object>) item.get("spec");
                    if (spec != null) {
                        for (Map.Entry<String, K8sOpenApiSchema> entry : table.getSchema().getProperties().entrySet()) {
                            row.put(entry.getKey(), spec.get(entry.getKey()));
                        }
                    }

                    return row;
                })
                .collect(Collectors.toList());
        }
        catch (ApiException e) {
            handleApiException(e, "list custom resources");
            return ImmutableList.of(); // Never reached due to exception
        }
    }

    public List<Map<String, Object>> getResourceAsList(String schema, String table) {
        ApiClient client = getClientForContext(schema);
        K8STable k8sTable = getTable(schema, table);

        if (k8sTable.isCustomResource()) {
            try {
                CustomObjectsApi customApi = new CustomObjectsApi(client);
                Object result;
                
                if (k8sTable.isNamespaced().orElse(false)) {
                    result = customApi.listNamespacedCustomObject(
                        k8sTable.getGroup().get(),
                        k8sTable.getVersion().get(),
                        "", // all namespaces
                        k8sTable.getPlural().get(),
                        null, null, null, null, null, null, null, null, null, null);
                }
                else {
                    result = customApi.listClusterCustomObject(
                        k8sTable.getGroup().get(),
                        k8sTable.getVersion().get(),
                        k8sTable.getPlural().get(),
                        null, null, null, null, null, null, null, null, null);
                }

                // Convert the result to a list of maps
                Map<String, Object> resultMap = (Map<String, Object>) result;
                List<Map<String, Object>> items = (List<Map<String, Object>>) resultMap.get("items");

                return items.stream()
                    .map(item -> {
                        Map<String, Object> row = new HashMap<>();
                        
                        // Extract metadata fields
                        Map<String, Object> metadata = (Map<String, Object>) item.get("metadata");
                        if (metadata != null) {
                            row.put("name", metadata.get("name"));
                            if (k8sTable.isNamespaced().orElse(false)) {
                                row.put("namespace", metadata.get("namespace"));
                            }
                        }

                        // Extract spec fields based on schema
                        Map<String, Object> spec = (Map<String, Object>) item.get("spec");
                        if (spec != null && k8sTable.getSchema().isPresent()) {
                            for (Map.Entry<String, K8sOpenApiSchema> entry : k8sTable.getSchema().get().getProperties().entrySet()) {
                                row.put(entry.getKey(), spec.get(entry.getKey()));
                            }
                        }

                        return row;
                    })
                    .collect(Collectors.toList());
            }
            catch (ApiException e) {
                handleApiException(e, "list custom resources");
                return ImmutableList.of(); // Never reached due to exception
            }
        }

        // Use the appropriate API class based on the resource type
        switch (table.toLowerCase()) {
            case "pods":
                return getPods(schema);
            case "services":
                return getServices(schema);
            case "deployments":
                return getDeployments(schema);
            case "replicasets":
                return getReplicaSets(schema);
            case "statefulsets":
                return getStatefulSets(schema);
            case "daemonsets":
                return getDaemonSets(schema);
            case "jobs":
                return getJobs(schema);
            case "cronjobs":
                return getCronJobs(schema);
            case "configmaps":
                return getConfigMaps(schema);
            case "secrets":
                return getSecrets(schema);
            case "persistentvolumeclaims":
                return getPersistentVolumeClaims(schema);
            case "horizontalpodautoscalers":
                return getHorizontalPodAutoscalers(schema);
            case "ingresses":
                return getIngresses(schema);
            case "networkpolicies":
                return getNetworkPolicies(schema);
            case "rolebindings":
                return getRoleBindings(schema);
            case "roles":
                return getRoles(schema);
            case "serviceaccounts":
                return getServiceAccounts(schema);
            case "endpoints":
                return getEndpoints(schema);
            case "nodes":
                return getNodes(schema);
            case "persistentvolumes":
                return getPersistentVolumes(schema);
            case "namespaces":
                return getNamespaces(schema);
            case "events":
                return getEvents(schema);
            case "resourcequotas":
                return getResourceQuotas(schema);
            case "limitranges":
                return getLimitRanges(schema);
            default:
                return ImmutableList.of();
        }
    }

    private <T> List<Map<String, Object>> getResourceList(String context, Class<T> resourceClass, Function<ApiClient, List<T>> listFunction) {
        try {
            ApiClient client = getClientForContext(context);
            List<T> items = listFunction.apply(client);

            return items.stream()
                    .map(item -> {
                        Map<String, Object> row = (Map<String, Object>) convertToMap(item);

                        // Add standard fields if available
                        try {
                            Method getMetadata = item.getClass().getMethod("getMetadata");
                            Object metadata = getMetadata.invoke(item);
                            if (metadata != null) {
                                Method getName = metadata.getClass().getMethod("getName");
                                Method getNamespace = metadata.getClass().getMethod("getNamespace");

                                Object name = getName.invoke(metadata);
                                Object namespace = getNamespace.invoke(metadata);

                                if (name != null) {
                                    row.put("name", name.toString());
                                }
                                if (namespace != null) {
                                    row.put("namespace", namespace.toString());
                                }
                            }
                        }
                        catch (Exception e) {
                            // Ignore if metadata methods are not available
                        }

                        return row;
                    })
                    .collect(Collectors.toList());
        }
        catch (UncheckedIOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new UncheckedIOException("Failed to get resources for class: " + resourceClass.getName(), new IOException(e));
        }
    }

    private RowType getResourceType(String resourceKind) {
        return resourceTypes.computeIfAbsent(resourceKind, kind -> {
            try {
                // Use the built-in API classes based on the resource kind
                Class<?> resourceClass = getResourceClass(kind);
                if (resourceClass != null) {
                    return createRowTypeFromClass(resourceClass);
                }

                // For unknown resources, return a generic row type
                return createGenericRowType();
            } catch (Exception e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Failed to get type information for resource: " + kind, e);
            }
        });
    }

    private Class<?> getResourceClass(String resourceKind) {
        switch (resourceKind.toLowerCase()) {
            case "pods":
                return io.kubernetes.client.openapi.models.V1Pod.class;
            case "services":
                return io.kubernetes.client.openapi.models.V1Service.class;
            case "deployments":
                return io.kubernetes.client.openapi.models.V1Deployment.class;
            case "replicasets":
                return io.kubernetes.client.openapi.models.V1ReplicaSet.class;
            case "statefulsets":
                return io.kubernetes.client.openapi.models.V1StatefulSet.class;
            case "daemonsets":
                return io.kubernetes.client.openapi.models.V1DaemonSet.class;
            case "jobs":
                return io.kubernetes.client.openapi.models.V1Job.class;
            case "cronjobs":
                return io.kubernetes.client.openapi.models.V1CronJob.class;
            case "configmaps":
                return io.kubernetes.client.openapi.models.V1ConfigMap.class;
            case "secrets":
                return io.kubernetes.client.openapi.models.V1Secret.class;
            case "persistentvolumeclaims":
                return io.kubernetes.client.openapi.models.V1PersistentVolumeClaim.class;
            case "horizontalpodautoscalers":
                return io.kubernetes.client.openapi.models.V1HorizontalPodAutoscaler.class;
            case "ingresses":
                return io.kubernetes.client.openapi.models.V1Ingress.class;
            case "networkpolicies":
                return io.kubernetes.client.openapi.models.V1NetworkPolicy.class;
            case "rolebindings":
                return io.kubernetes.client.openapi.models.V1RoleBinding.class;
            case "roles":
                return io.kubernetes.client.openapi.models.V1Role.class;
            case "serviceaccounts":
                return io.kubernetes.client.openapi.models.V1ServiceAccount.class;
            case "endpoints":
                return io.kubernetes.client.openapi.models.V1Endpoints.class;
            case "nodes":
                return io.kubernetes.client.openapi.models.V1Node.class;
            case "persistentvolumes":
                return io.kubernetes.client.openapi.models.V1PersistentVolume.class;
            case "namespaces":
                return io.kubernetes.client.openapi.models.V1Namespace.class;
            case "resourcequotas":
                return io.kubernetes.client.openapi.models.V1ResourceQuota.class;
            case "limitranges":
                return io.kubernetes.client.openapi.models.V1LimitRange.class;
            default:
                return null;
        }
    }

    private <T> RowType createRowTypeFromClass(Class<T> clazz) {
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();

        // If it's a primitive type or timestamp, return a simple row type with a single value field
        if (isPrimitiveOrTimestamp(clazz)) {
            Type type = getSimpleType(clazz);
            fields.add(new RowType.Field(Optional.of("value"), type));
            return RowType.from(fields.build());
        }

        // Get all fields with @SerializedName annotation
        for (Field field : clazz.getDeclaredFields()) {
            SerializedName annotation = field.getAnnotation(SerializedName.class);
            if (annotation != null) {
                String fieldName = annotation.value();
                Type fieldType = getTypeFromField(field);
                fields.add(new RowType.Field(Optional.of(fieldName), fieldType));
            }
        }

        // If no fields were found, return a generic JSON type
        if (fields.build().isEmpty()) {
            return createGenericRowType();
        }

        return RowType.from(fields.build());
    }

    private Type getSimpleType(Class<?> clazz) {
        if (clazz == String.class) {
            return VARCHAR;
        }
        if (clazz == Integer.class || clazz == int.class ||
            clazz == Long.class || clazz == long.class) {
            return INTEGER;
        }
        if (clazz == Double.class || clazz == double.class ||
            clazz == Float.class || clazz == float.class) {
            return DOUBLE;
        }
        if (clazz == Boolean.class || clazz == boolean.class) {
            return BOOLEAN;
        }
        if (clazz == Date.class ||
            clazz == java.sql.Timestamp.class ||
            clazz == java.time.OffsetDateTime.class ||
            clazz == Number.class) {  // For epoch timestamps
            return typeManager.getType(new TypeSignature("timestamp"));
        }
        // Default to VARCHAR for unknown types
        return VARCHAR;
    }

    private Type getTypeFromField(Field field) {
        Class<?> fieldType = field.getType();

        // Handle timestamps first
        if (fieldType == java.time.OffsetDateTime.class ||
            fieldType == Date.class ||
            fieldType == java.sql.Timestamp.class ||
            field.getName().toLowerCase().contains("timestamp")) {
            return typeManager.getType(new TypeSignature("timestamp"));
        }

        // Handle primitive types
        if (isPrimitiveOrTimestamp(fieldType)) {
            return getSimpleType(fieldType);
        }

        // Handle Lists
        if (List.class.isAssignableFrom(fieldType)) {
            Type elementType = getElementTypeFromField(field);
            return new ArrayType(elementType);
        }

        // Handle Maps
        if (Map.class.isAssignableFrom(fieldType)) {
            return getJsonMapType();
        }

        // For complex objects, create a RowType
        return createRowTypeFromClass(fieldType);
    }

    private Type getElementTypeFromField(Field field) {
        // Try to get the generic type of the List
        java.lang.reflect.Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType) {
            ParameterizedType paramType = (ParameterizedType) genericType;
            java.lang.reflect.Type[] typeArguments = paramType.getActualTypeArguments();
            if (typeArguments.length > 0) {
                java.lang.reflect.Type elementType = typeArguments[0];
                if (elementType instanceof Class) {
                    Class<?> elementClass = (Class<?>) elementType;
                    return isPrimitiveOrTimestamp(elementClass) ?
                           getSimpleType(elementClass) :
                           getJsonMapType();
                }
            }
        }
        // Default to JSON type for complex elements
        return getJsonMapType();
    }

    private RowType createGenericRowType() {
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        fields.add(new RowType.Field(Optional.of("value"), getJsonMapType()));
        return RowType.from(fields.build());
    }

    private boolean isPrimitiveOrTimestamp(Class<?> clazz) {
        return clazz.isPrimitive() ||
                clazz == String.class ||
                clazz == Integer.class ||
                clazz == Long.class ||
                clazz == Double.class ||
                clazz == Float.class ||
                clazz == Boolean.class ||
                clazz == Character.class ||
                clazz == Date.class ||
                clazz == java.sql.Timestamp.class ||
                clazz == java.time.OffsetDateTime.class;
    }

    private List<Map<String, Object>> getPods(String context)
    {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Pod.class,
            client -> withRetry("list pods", () -> {
                try {
                    CoreV1Api api = new CoreV1Api(client);
                    V1PodList podList = api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return podList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list pods");
                    return null; // This line will never be reached due to exception
                }
            })
        );
    }

    private List<Map<String, Object>> getServices(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Service.class,
            client -> {
                try {
                    CoreV1Api api = new CoreV1Api(client);
                    V1ServiceList serviceList = api.listServiceForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return serviceList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list services");
                    return null; // This line will never be reached due to exception
                }
            }
        );
    }

    public List<Map<String, Object>> getTableData(String schema, String table)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(table, "table is null");

        if (!availableContexts.contains(schema)) {
            return ImmutableList.of();
        }

        switch (table.toLowerCase()) {
            case "pods":
                return getPods(schema);
            default:
                return ImmutableList.of();
        }
    }

    private List<Map<String, Object>> getDeployments(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Deployment.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    AppsV1Api api = new AppsV1Api(refreshedClient);
                    V1DeploymentList deploymentList = api.listDeploymentForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return deploymentList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list deployments");
                    return null; // This line will never be reached due to exception
                }
            })
        );
    }

    private List<Map<String, Object>> getReplicaSets(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1ReplicaSet.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    AppsV1Api api = new AppsV1Api(refreshedClient);
                    V1ReplicaSetList replicaSetList = api.listReplicaSetForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return replicaSetList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list replica sets");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getStatefulSets(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1StatefulSet.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    AppsV1Api api = new AppsV1Api(refreshedClient);
                    V1StatefulSetList statefulSetList = api.listStatefulSetForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return statefulSetList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list stateful sets");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getDaemonSets(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1DaemonSet.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    AppsV1Api api = new AppsV1Api(refreshedClient);
                    V1DaemonSetList daemonSetList = api.listDaemonSetForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return daemonSetList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list daemon sets");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getJobs(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Job.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    BatchV1Api api = new BatchV1Api(refreshedClient);
                    V1JobList jobList = api.listJobForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return jobList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list jobs");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getCronJobs(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1CronJob.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    BatchV1Api api = new BatchV1Api(refreshedClient);
                    V1CronJobList cronJobList = api.listCronJobForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return cronJobList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list cron jobs");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getConfigMaps(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1ConfigMap.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1ConfigMapList configMapList = api.listConfigMapForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return configMapList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list config maps");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getSecrets(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Secret.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1SecretList secretList = api.listSecretForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return secretList.getItems();
                }
                catch (ApiException e) {
                    handleApiException(e, "list secrets");
                    return null; // This line will never be reached due to exception
                }
            })
        );
    }

    private List<Map<String, Object>> getPersistentVolumeClaims(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1PersistentVolumeClaim.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1PersistentVolumeClaimList pvcList = api.listPersistentVolumeClaimForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return pvcList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list persistent volume claims");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getHorizontalPodAutoscalers(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1HorizontalPodAutoscaler.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    AutoscalingV1Api api = new AutoscalingV1Api(refreshedClient);
                    V1HorizontalPodAutoscalerList hpaList = api.listHorizontalPodAutoscalerForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return hpaList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list horizontal pod autoscalers");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getIngresses(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Ingress.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    NetworkingV1Api api = new NetworkingV1Api(refreshedClient);
                    V1IngressList ingressList = api.listIngressForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return ingressList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list ingresses");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getNetworkPolicies(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1NetworkPolicy.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    NetworkingV1Api api = new NetworkingV1Api(refreshedClient);
                    V1NetworkPolicyList networkPolicyList = api.listNetworkPolicyForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return networkPolicyList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list network policies");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getRoleBindings(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1RoleBinding.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    RbacAuthorizationV1Api api = new RbacAuthorizationV1Api(refreshedClient);
                    V1RoleBindingList roleBindingList = api.listRoleBindingForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return roleBindingList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list role bindings");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getRoles(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Role.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    RbacAuthorizationV1Api api = new RbacAuthorizationV1Api(refreshedClient);
                    V1RoleList roleList = api.listRoleForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return roleList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list roles");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getServiceAccounts(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1ServiceAccount.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1ServiceAccountList serviceAccountList = api.listServiceAccountForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return serviceAccountList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list service accounts");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getEndpoints(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Endpoints.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1EndpointsList endpointsList = api.listEndpointsForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return endpointsList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list endpoints");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getNodes(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Node.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1NodeList nodeList = api.listNode(null, null, null, null, null, null, null, null, null, null);
                    return nodeList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list nodes");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getPersistentVolumes(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1PersistentVolume.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1PersistentVolumeList pvList = api.listPersistentVolume(null, null, null, null, null, null, null, null, null, null);
                    return pvList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list persistent volumes");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getNamespaces(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1Namespace.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1NamespaceList namespaceList = api.listNamespace(null, null, null, null, null, null, null, null, null, null);
                    return namespaceList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list namespaces");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getEvents(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.CoreV1Event.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    CoreV1EventList eventList = api.listEventForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return eventList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list events");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getResourceQuotas(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1ResourceQuota.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1ResourceQuotaList resourceQuotaList = api.listResourceQuotaForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return resourceQuotaList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list resource quotas");
                    return null;
                }
            })
        );
    }

    private List<Map<String, Object>> getLimitRanges(String context) {
        return getResourceList(
            context,
            io.kubernetes.client.openapi.models.V1LimitRange.class,
            client -> executeWithClientRefresh(context, refreshedClient -> {
                try {
                    CoreV1Api api = new CoreV1Api(refreshedClient);
                    V1LimitRangeList limitRangeList = api.listLimitRangeForAllNamespaces(null, null, null, null, null, null, null, null, null, null);
                    return limitRangeList.getItems();
                } catch (ApiException e) {
                    handleApiException(e, "list limit ranges");
                    return null;
                }
            })
        );
    }

    private <T> List<T> withRetry(String operation, Supplier<List<T>> action) {
        int attempts = 0;
        while (true) {
            try {
                return action.get();
            }
            catch (UncheckedIOException e) {
                throw e;
            }
            catch (Exception e) {
                attempts++;
                if (attempts >= MAX_RETRIES) {
                    throw new UncheckedIOException("Failed to " + operation + " after " + MAX_RETRIES + " attempts", new IOException(e));
                }
                try {
                    Thread.sleep(RETRY_DELAY_MS * attempts);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new UncheckedIOException("Operation interrupted", new IOException(ie));
                }
                // Refresh the client for the next attempt
                refreshClient();
            }
        }
    }

    private void refreshClient() {
        try {
            // Clear the cached clients to force recreation
            contextClients.clear();
            contextConfigs.clear();

            // Reload the base config
            KubeConfig baseConfig = KubeConfig.loadKubeConfig(Files.newBufferedReader(Paths.get(System.getProperty("user.home"), ".kube", "config")));
            availableContexts.clear();
            baseConfig.getContexts().stream()
                    .map(context -> ((Map<String, Object>) context).get("name").toString())
                    .forEach(availableContexts::add);
        }
        catch (UncheckedIOException e) {
            throw e;
        }
        catch (Exception e) {
            throw new UncheckedIOException("Failed to refresh Kubernetes client", new IOException(e));
        }
    }

    @SuppressWarnings("unchecked")
    public Object convertToMap(Object value) {
        if (value == null) {
            return null;
        }

        // Handle OffsetDateTime (timestamps)
        if (value instanceof java.time.OffsetDateTime) {
            return java.sql.Timestamp.valueOf(((java.time.OffsetDateTime) value).toLocalDateTime());
        }

        // Handle Date objects (timestamps)
        if (value instanceof Date) {
            return new java.sql.Timestamp(((Date) value).getTime());
        }

        // Handle primitive types
        if (value instanceof String) {
            return value.toString();
        }
        if (value instanceof Number) {
            return value;
        }
        if (value instanceof Boolean) {
            return value;
        }
        if (value.getClass().isPrimitive()) {
            return value;
        }

        // Handle Lists
        if (value instanceof List) {
            return ((List<?>) value).stream()
                    .map(this::convertToMap)
                    .collect(Collectors.toList());
        }

        // Handle Maps
        if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            if (map.isEmpty()) {
                return ImmutableMap.of();
            }

            // Check if this is a special case with a 'value' field
            if (map.containsKey("value")) {
                Object mapValue = map.get("value");
                // If the value is null, return null
                if (mapValue == null) {
                    return null;
                }
                // Otherwise, convert the value
                return convertToMap(mapValue);
            }

            return map.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().toString(),
                            e -> convertToMap(e.getValue())));
        }

        // Handle IntOrString values
        if (value instanceof io.kubernetes.client.custom.IntOrString) {
            io.kubernetes.client.custom.IntOrString intOrString = (io.kubernetes.client.custom.IntOrString) value;
            try {
                return intOrString.isInteger() ? intOrString.getIntValue() : intOrString.getStrValue();
            }
            catch (UncheckedIOException e) {
                throw e;
            }
            catch (Exception e) {
                return value.toString();
            }
        }

        // Handle all other objects using reflection
        try {
            Map<String, Object> result = new HashMap<>();
            // First, get all declared fields including those from superclasses
            List<Field> allFields = new ArrayList<>();
            Class<?> currentClass = value.getClass();
            while (currentClass != null) {
                allFields.addAll(Arrays.asList(currentClass.getDeclaredFields()));
                currentClass = currentClass.getSuperclass();
            }

            // Process all fields
            for (Field field : allFields) {
                SerializedName annotation = field.getAnnotation(SerializedName.class);
                if (annotation != null) {
                    String fieldName = annotation.value();
                    try {
                        field.setAccessible(true);
                        Object fieldValue = field.get(value);
                        if (fieldValue != null) {
                            result.put(fieldName, convertToMap(fieldValue));
                        }
                    }
                    catch (UncheckedIOException e) {
                        throw e;
                    }
                    catch (IllegalAccessException e) {
                        // Skip fields that can't be accessed
                    }
                }
            }
            return result;
        }
        catch (UncheckedIOException e) {
            throw e;
        }
        catch (Exception e) {
            // If reflection fails, return the string representation
            return value.toString();
        }
    }

    private boolean isPrimitiveType(Object value) {
        return value instanceof Number ||
                value instanceof String ||
                value instanceof Boolean ||
                value instanceof Character ||
                value.getClass().isPrimitive();
    }

    private void handleApiException(ApiException e, String operation) {
        String message;
        if (e.getCode() == 403) {
            message = "You don't have access to list objects";
        }
        else {
            message = String.format("Failed to %s. Status code: %d, Message: %s",
                    operation,
                    e.getCode(),
                    e.getMessage() != null ? e.getMessage() : "No message");
        }
        throw new UncheckedIOException(message, new IOException(e));
    }

    private boolean isNamespacedResource(String resourceKind) {
        return NAMESPACED_RESOURCES.contains(resourceKind.toLowerCase());
    }

    private K8STable getTable(String schema, String table) {
        Map<String, K8STable> tables = schemas.get(schema);
        if (tables == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Schema not found: " + schema);
        }
        K8STable k8sTable = tables.get(table);
        if (k8sTable == null) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Table not found: " + table);
        }
        return k8sTable;
    }

    private Type getJsonMapType() {
        return typeManager.getType(new TypeSignature(JSON));
    }

    private <T> T executeWithClientRefresh(String context, Function<ApiClient, T> operation) {
        try {
            ApiClient client = getClientForContext(context);
            return operation.apply(client);
        }
        catch (ApiException e) {
            if (e.getCode() == 401) {
                // Token might be expired, refresh the client
                contextClients.remove(context);
                ApiClient refreshedClient = getClientForContext(context);
                return operation.apply(refreshedClient);
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                format("Kubernetes API request failed: %s", e.getMessage()), e);
        }
    }
}
