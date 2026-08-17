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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.kubernetes.client.KubernetesClient;
import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KubernetesPageSink
        implements ConnectorPageSink
{
    private final KubernetesClient client;
    private final ResourceDescriptor resource;
    private final List<KubernetesColumnHandle> columns;
    private final String defaultNamespace;

    public KubernetesPageSink(KubernetesClient client, ResourceDescriptor resource, List<KubernetesColumnHandle> columns, String defaultNamespace)
    {
        this.client = requireNonNull(client, "client is null");
        this.resource = requireNonNull(resource, "resource is null");
        this.columns = ImmutableList.copyOf(columns);
        this.defaultNamespace = requireNonNull(defaultNamespace, "defaultNamespace is null");
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            ObjectNode object = KubernetesObjectBuilder.buildObject(resource, columns, page, position);
            KubernetesObjectBuilder.stripServerPopulatedFields(object);
            Optional<String> namespace = KubernetesObjectBuilder.metadataField(object, "namespace");
            if (resource.namespaced() && namespace.isEmpty()) {
                namespace = Optional.of(defaultNamespace);
                KubernetesObjectBuilder.ensureMetadata(object).put("namespace", defaultNamespace);
            }
            client.createObject(resource, namespace, object);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
    }
}
