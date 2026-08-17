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
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @param dataColumns the visible table columns, in table order, matching the data
 *         channels of the pages the merge sink receives
 */
public record KubernetesMergeTableHandle(KubernetesTableHandle table, List<KubernetesColumnHandle> dataColumns)
        implements ConnectorMergeTableHandle
{
    public KubernetesMergeTableHandle
    {
        requireNonNull(table, "table is null");
        dataColumns = ImmutableList.copyOf(dataColumns);
    }

    @Override
    public ConnectorTableHandle getTableHandle()
    {
        return table;
    }
}
