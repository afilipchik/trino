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
package io.trino.plugin.federation.client;

import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import io.trino.client.Column;
import io.trino.client.QueryError;
import io.trino.client.QueryStatusInfo;
import io.trino.client.StatementClient;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_REGION_UNREACHABLE;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_REMOTE_ERROR;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_TYPE_MISMATCH;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * Streaming result of a query executed against one region. Iterates rows whose values are
 * already converted to the stack representation of the mapped Trino types. Closing cancels
 * the remote query.
 */
public class RegionQueryResults
        extends AbstractIterator<List<Object>>
        implements Closeable
{
    private final String regionName;
    private final StatementClient client;
    private final List<ResultColumn> columns;
    private Iterator<List<Object>> rawRows;

    RegionQueryResults(String regionName, StatementClient client)
    {
        this.regionName = requireNonNull(regionName, "regionName is null");
        this.client = requireNonNull(client, "client is null");
        this.columns = fetchColumns().stream()
                .map(column -> new ResultColumn(column.getName(), requiredType(column)))
                .collect(toImmutableList());
        this.rawRows = client.isRunning() ? client.currentRows().iterator() : emptyIterator();
    }

    public List<ResultColumn> columns()
    {
        return columns;
    }

    public List<Type> types()
    {
        return columns.stream()
                .map(ResultColumn::type)
                .collect(toImmutableList());
    }

    @Override
    protected List<Object> computeNext()
    {
        while (!rawRows.hasNext()) {
            if (!client.isRunning() || !advanceClient()) {
                throwIfQueryFailed();
                return endOfData();
            }
            rawRows = client.currentRows().iterator();
        }
        return convertRow(rawRows.next());
    }

    @Override
    public void close()
    {
        client.close();
    }

    private List<Column> fetchColumns()
    {
        while (client.isRunning()) {
            List<Column> currentColumns = client.currentStatusInfo().getColumns();
            if (currentColumns != null) {
                return currentColumns;
            }
            if (!advanceClient()) {
                break;
            }
        }
        throwIfQueryFailed();
        if (client.isFinished()) {
            List<Column> finalColumns = client.finalStatusInfo().getColumns();
            if (finalColumns != null) {
                return finalColumns;
            }
        }
        throw new TrinoException(FEDERATION_REMOTE_ERROR, "Region '%s' query returned no columns".formatted(regionName));
    }

    private boolean advanceClient()
    {
        try {
            return client.advance();
        }
        catch (RuntimeException e) {
            throw transportError(regionName, e);
        }
    }

    private void throwIfQueryFailed()
    {
        if (client.isClientAborted()) {
            return;
        }
        if (!client.isFinished()) {
            throw new TrinoException(FEDERATION_REMOTE_ERROR, "Region '%s' query failed".formatted(regionName));
        }
        QueryStatusInfo status = client.finalStatusInfo();
        if (status.getError() != null) {
            throw remoteError(regionName, status.getError());
        }
    }

    private Type requiredType(Column column)
    {
        return FederationTypeMapper.toTrinoType(column.getType())
                .orElseThrow(() -> new TrinoException(
                        FEDERATION_TYPE_MISMATCH,
                        "Region '%s' returned unsupported type '%s' for column '%s'".formatted(regionName, column.getType(), column.getName())));
    }

    private List<Object> convertRow(List<Object> row)
    {
        List<Object> converted = new ArrayList<>(row.size());
        for (int channel = 0; channel < row.size(); channel++) {
            converted.add(FederationTypeMapper.toNativeValue(columns.get(channel).type(), row.get(channel)));
        }
        return unmodifiableList(converted);
    }

    static TrinoException transportError(String regionName, RuntimeException exception)
    {
        if (Throwables.getCausalChain(exception).stream().anyMatch(IOException.class::isInstance)) {
            return new TrinoException(
                    FEDERATION_REGION_UNREACHABLE,
                    "Region '%s' is unreachable: %s".formatted(regionName, exception.getMessage()),
                    exception);
        }
        return new TrinoException(FEDERATION_REMOTE_ERROR, "Region '%s' query failed: %s".formatted(regionName, exception.getMessage()), exception);
    }

    static TrinoException remoteError(String regionName, QueryError error)
    {
        return new TrinoException(FEDERATION_REMOTE_ERROR, "Region '%s' query failed: %s".formatted(regionName, error.getMessage()));
    }
}
