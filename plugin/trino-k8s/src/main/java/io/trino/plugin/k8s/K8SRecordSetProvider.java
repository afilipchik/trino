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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.TypeManager;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class K8SRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final K8SClient k8sClient;
    private final ObjectMapper objectMapper;
    private final TypeManager typeManager;

    @Inject
    public K8SRecordSetProvider(K8SClient k8sClient, ObjectMapper objectMapper, TypeManager typeManager)
    {
        this.k8sClient = requireNonNull(k8sClient, "k8sClient is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        K8SSplit k8sSplit = (K8SSplit) split;

        ImmutableList.Builder<K8SColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((K8SColumnHandle) handle);
        }

        return new K8SRecordSet(k8sClient, k8sSplit, handles.build(), objectMapper, typeManager);
    }
}
