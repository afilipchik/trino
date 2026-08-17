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
import io.trino.plugin.kubernetes.client.KubernetesClient;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class KubernetesPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final KubernetesClient client;
    private final String defaultNamespace;

    @Inject
    public KubernetesPageSinkProvider(KubernetesClient client, KubernetesConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.defaultNamespace = config.getDefaultNamespace();
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorOutputTableHandle outputTableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support creating tables");
    }

    @Override
    public ConnectorPageSink createPageSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorInsertTableHandle insertTableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        KubernetesInsertTableHandle handle = (KubernetesInsertTableHandle) insertTableHandle;
        return new KubernetesPageSink(client, handle.table().descriptor(), handle.columns(), defaultNamespace);
    }

    @Override
    public ConnectorMergeSink createMergeSink(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorPageSinkId pageSinkId)
    {
        return new KubernetesMergeSink(client, (KubernetesMergeTableHandle) mergeHandle, defaultNamespace);
    }
}
