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

import com.google.inject.Inject;
import io.trino.plugin.kubernetes.client.KubernetesClusterRegistry;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class KubernetesSplitManager
        implements ConnectorSplitManager
{
    private final KubernetesClusterRegistry clusterRegistry;

    @Inject
    public KubernetesSplitManager(KubernetesClusterRegistry clusterRegistry)
    {
        this.clusterRegistry = requireNonNull(clusterRegistry, "clusterRegistry is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        if (!clusterRegistry.isClusterColumnEnabled()) {
            return new FixedSplitSource(new KubernetesSplit(Optional.empty()));
        }
        // one split per cluster, pruned by a pushed down cluster equality predicate
        KubernetesTableHandle handle = (KubernetesTableHandle) table;
        List<ConnectorSplit> splits = clusterRegistry.clusterNames().stream()
                .filter(cluster -> handle.clusterFilter().map(cluster::equals).orElse(true))
                .map(cluster -> (ConnectorSplit) new KubernetesSplit(Optional.of(cluster)))
                .collect(toImmutableList());
        return new FixedSplitSource(splits);
    }
}
