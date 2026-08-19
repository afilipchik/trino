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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.kubernetes.client.KubernetesClient;
import io.trino.plugin.kubernetes.client.KubernetesClusterRegistry;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class KubernetesPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final KubernetesClusterRegistry clusterRegistry;
    private final int listPageSize;

    @Inject
    public KubernetesPageSourceProvider(KubernetesClusterRegistry clusterRegistry, KubernetesConfig config)
    {
        this.clusterRegistry = requireNonNull(clusterRegistry, "clusterRegistry is null");
        this.listPageSize = config.getListPageSize();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            Optional<ConnectorTableCredentials> tableCredentials,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter,
            MemoryContext memoryContext)
    {
        KubernetesSplit kubernetesSplit = (KubernetesSplit) split;
        KubernetesTableHandle handle = (KubernetesTableHandle) table;
        ImmutableList.Builder<KubernetesColumnHandle> kubernetesColumns = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            kubernetesColumns.add((KubernetesColumnHandle) column);
        }
        KubernetesClient client = kubernetesSplit.cluster()
                .map(clusterRegistry::client)
                .orElseGet(clusterRegistry::defaultClient);
        return new KubernetesPageSource(client, kubernetesSplit.cluster(), handle, kubernetesColumns.build(), listPageSize);
    }
}
