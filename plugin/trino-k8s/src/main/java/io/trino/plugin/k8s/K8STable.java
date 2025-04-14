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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import io.kubernetes.client.openapi.models.V1CustomResourceDefinition;
import static io.trino.spi.type.VarcharType.VARCHAR;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class K8STable
{
    private final String name;
    private final List<K8SColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final List<URI> sources;
    private final Optional<String> group;
    private final Optional<String> version;
    private final Optional<String> plural;
    private final Optional<Boolean> namespaced;
    private final Optional<K8sOpenApiSchema> schema;

    @JsonCreator
    public K8STable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<K8SColumn> columns,
            @JsonProperty("sources") List<URI> sources)
    {
        this(name, columns, sources, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public K8STable(
            String name,
            List<K8SColumn> columns,
            List<URI> sources,
            Optional<String> group,
            Optional<String> version,
            Optional<String> plural,
            Optional<Boolean> namespaced,
            Optional<K8sOpenApiSchema> schema)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
        this.group = requireNonNull(group, "group is null");
        this.version = requireNonNull(version, "version is null");
        this.plural = requireNonNull(plural, "plural is null");
        this.namespaced = requireNonNull(namespaced, "namespaced is null");
        this.schema = requireNonNull(schema, "schema is null");

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (K8SColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    public static K8STable fromCrd(V1CustomResourceDefinition crd, K8sOpenApiSchema schema) {
        String name = crd.getSpec().getNames().getPlural();
        String group = crd.getSpec().getGroup();
        String version = crd.getSpec().getVersions().get(0).getName(); // Use first version
        String plural = crd.getSpec().getNames().getPlural();
        boolean namespaced = "Namespaced".equals(crd.getSpec().getScope());

        // Build columns from schema
        ImmutableList.Builder<K8SColumn> columns = ImmutableList.builder();
        
        // Add standard metadata columns
        if (namespaced) {
            columns.add(new K8SColumn("namespace", VARCHAR));
        }
        columns.add(new K8SColumn("name", VARCHAR));

        // Add columns from schema properties
        for (Map.Entry<String, K8sOpenApiSchema> entry : schema.getProperties().entrySet()) {
            columns.add(new K8SColumn(entry.getKey(), entry.getValue().toTrinoType()));
        }

        return new K8STable(
                name,
                columns.build(),
                ImmutableList.of(), // Empty sources list since CRDs don't have external sources
                Optional.of(group),
                Optional.of(version),
                Optional.of(plural),
                Optional.of(namespaced),
                Optional.of(schema));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<K8SColumn> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public List<URI> getSources()
    {
        return sources;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    @JsonProperty
    public Optional<String> getGroup()
    {
        return group;
    }

    @JsonProperty
    public Optional<String> getVersion()
    {
        return version;
    }

    @JsonProperty
    public Optional<String> getPlural()
    {
        return plural;
    }

    @JsonProperty
    public Optional<Boolean> isNamespaced()
    {
        return namespaced;
    }

    @JsonProperty
    public Optional<K8sOpenApiSchema> getSchema()
    {
        return schema;
    }

    public boolean isCustomResource()
    {
        return group.isPresent() && version.isPresent() && plural.isPresent();
    }
}
