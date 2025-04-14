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

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.TableNotFoundException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class K8SSplitManager
        implements ConnectorSplitManager
{
    private final K8SClient k8sClient;

    @Inject
    public K8SSplitManager(K8SClient k8sClient)
    {
        this.k8sClient = requireNonNull(k8sClient, "k8sClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        K8STableHandle tableHandle = (K8STableHandle) connectorTableHandle;
        K8STable table = k8sClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());

        // this can happen if table is removed during a query
        if (table == null) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }

        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new K8SSplit(tableHandle));
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }
}
