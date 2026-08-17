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

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.plugin.kubernetes.client.KubernetesClient;
import io.trino.plugin.kubernetes.client.ResourceDescriptor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class KubernetesPageSource
        implements ConnectorPageSource
{
    private final KubernetesClient client;
    private final KubernetesTableHandle table;
    private final ResourceDescriptor resource;
    private final List<KubernetesColumnHandle> columns;
    private final PageBuilder pageBuilder;
    private final int pageSize;

    private Optional<String> continueToken = Optional.empty();
    private boolean firstPageLoaded;
    private boolean finished;
    private long completedBytes;
    private long remainingRows = Long.MAX_VALUE;

    public KubernetesPageSource(KubernetesClient client, KubernetesTableHandle table, List<KubernetesColumnHandle> columns, int pageSize)
    {
        this.client = requireNonNull(client, "client is null");
        this.table = requireNonNull(table, "table is null");
        this.resource = table.descriptor();
        this.columns = requireNonNull(columns, "columns is null");
        this.pageBuilder = new PageBuilder(columns.stream()
                .map(KubernetesColumnHandle::type)
                .collect(toImmutableList()));
        this.pageSize = pageSize;
        if (table.limit().isPresent()) {
            this.remainingRows = table.limit().getAsLong();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (finished) {
            return null;
        }

        int limit = (int) Math.min(pageSize, Math.max(1, remainingRows));
        KubernetesClient.ObjectListPage listPage = client.listObjects(
                resource,
                table.namespaceFilter(),
                table.nameFilter(),
                limit,
                firstPageLoaded ? continueToken : Optional.empty());
        firstPageLoaded = true;
        continueToken = listPage.continueToken();

        for (JsonNode object : listPage.items()) {
            if (remainingRows <= 0) {
                break;
            }
            remainingRows--;
            appendObject(object);
            completedBytes += object.toString().length();
        }

        if (continueToken.isEmpty() || remainingRows <= 0) {
            finished = true;
        }

        if (pageBuilder.isEmpty()) {
            return finished ? null : SourcePage.create(0);
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return SourcePage.create(page);
    }

    private void appendObject(JsonNode object)
    {
        pageBuilder.declarePosition();
        JsonNode metadata = object.path("metadata");
        for (int i = 0; i < columns.size(); i++) {
            KubernetesColumnHandle column = columns.get(i);
            BlockBuilder output = pageBuilder.getBlockBuilder(i);
            switch (column.name()) {
                case KubernetesColumns.NAME_COLUMN -> appendText(output, metadata.path("name"));
                case KubernetesColumns.NAMESPACE_COLUMN -> appendText(output, metadata.path("namespace"));
                case KubernetesColumns.MERGE_ROW_ID_COLUMN -> appendRowId(output, metadata);
                default -> JsonValueDecoder.appendTo(column.type(), object.get(column.jsonName()), output);
            }
        }
    }

    private static void appendText(BlockBuilder output, JsonNode value)
    {
        if (value.isTextual()) {
            VARCHAR.writeSlice(output, utf8Slice(value.asText()));
        }
        else {
            output.appendNull();
        }
    }

    private static void appendRowId(BlockBuilder output, JsonNode metadata)
    {
        ((RowBlockBuilder) output).buildEntry(fieldBuilders -> {
            appendText(fieldBuilders.get(0), metadata.path("namespace"));
            appendText(fieldBuilders.get(1), metadata.path("name"));
            appendText(fieldBuilders.get(2), metadata.path("resourceVersion"));
        });
    }

    @Override
    public void close()
    {
    }
}
