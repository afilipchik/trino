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

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.QueryError;
import io.trino.client.StatementClient;
import io.trino.plugin.federation.Region;
import io.trino.spi.connector.SchemaTableName;
import okhttp3.OkHttpClient;

import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.StatementClientFactory.newStatementClient;
import static io.trino.plugin.federation.client.RegionQueryResults.remoteError;
import static io.trino.plugin.federation.client.RegionQueryResults.transportError;
import static java.util.Objects.requireNonNull;

/**
 * Executes SQL against one region's Trino cluster over the client protocol. Data queries
 * stream converted rows through {@link RegionQueryResults}; metadata listing helpers run
 * small {@code information_schema} queries against the configured remote catalog.
 * <p>
 * Instances are thread-safe; each query uses its own {@link StatementClient} on the shared
 * HTTP client.
 */
public class RegionClient
{
    private static final ZoneId SESSION_TIME_ZONE = ZoneId.of("UTC");

    private final OkHttpClient httpClient;
    private final Region region;
    private final String remoteCatalog;
    private final String user;
    private final Duration requestTimeout;

    public RegionClient(OkHttpClient httpClient, Region region, String remoteCatalog, String user, Duration requestTimeout)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.region = requireNonNull(region, "region is null");
        this.remoteCatalog = requireNonNull(remoteCatalog, "remoteCatalog is null");
        this.user = requireNonNull(user, "user is null");
        this.requestTimeout = requireNonNull(requestTimeout, "requestTimeout is null");
    }

    public String regionName()
    {
        return region.name();
    }

    /**
     * Starts the given query on the region and returns a streaming handle over the converted
     * rows. The caller must close the handle; closing before the rows are exhausted cancels
     * the remote query.
     */
    public RegionQueryResults execute(String sql)
    {
        StatementClient client = startQuery(sql);
        try {
            return new RegionQueryResults(region.name(), client);
        }
        catch (Throwable e) {
            try {
                client.close();
            }
            catch (RuntimeException closeError) {
                if (e != closeError) {
                    e.addSuppressed(closeError);
                }
            }
            throw e;
        }
    }

    public List<String> listSchemas()
    {
        String sql = "SELECT schema_name FROM %s.information_schema.schemata ORDER BY schema_name".formatted(quoted(remoteCatalog));
        return queryRows(sql).stream()
                .map(row -> (String) row.get(0))
                .collect(toImmutableList());
    }

    public List<SchemaTableName> listTables(Optional<String> schema)
    {
        StringBuilder sql = new StringBuilder()
                .append("SELECT table_schema, table_name FROM %s.information_schema.tables".formatted(quoted(remoteCatalog)));
        schema.ifPresent(value -> sql.append(" WHERE table_schema = %s".formatted(literal(value))));
        sql.append(" ORDER BY table_schema, table_name");
        return queryRows(sql.toString()).stream()
                .map(row -> new SchemaTableName((String) row.get(0), (String) row.get(1)))
                .collect(toImmutableList());
    }

    /**
     * Returns the ordered columns of a remote table. Columns with remote types the connector
     * does not support are still listed, with an empty Trino type, so callers can skip them.
     */
    public List<RemoteColumn> describeTable(String schema, String table)
    {
        String sql = ("SELECT column_name, data_type FROM %s.information_schema.columns " +
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position")
                .formatted(quoted(remoteCatalog), literal(schema), literal(table));
        return queryRows(sql).stream()
                .map(row -> {
                    String remoteType = (String) row.get(1);
                    return new RemoteColumn((String) row.get(0), remoteType, FederationTypeMapper.toTrinoType(remoteType));
                })
                .collect(toImmutableList());
    }

    private List<List<Object>> queryRows(String sql)
    {
        try (StatementClient client = startQuery(sql)) {
            ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
            do {
                for (List<Object> row : client.currentRows()) {
                    rows.add(row);
                }
            }
            while (client.isRunning() && advance(client));

            if (!client.isFinished()) {
                throw transportError(region.name(), new IllegalStateException("Query did not complete"));
            }
            QueryError error = client.finalStatusInfo().getError();
            if (error != null) {
                throw remoteError(region.name(), error);
            }
            return rows.build();
        }
    }

    private StatementClient startQuery(String sql)
    {
        ClientSession session = ClientSession.builder()
                .server(region.uri())
                .user(Optional.of(user))
                .source("trino-federation")
                .catalog(remoteCatalog)
                .timeZone(SESSION_TIME_ZONE)
                .locale(Locale.ENGLISH)
                .clientRequestTimeout(requestTimeout)
                .compressionDisabled(true)
                .build();
        try {
            return newStatementClient(httpClient, session, sql, Optional.empty());
        }
        catch (RuntimeException e) {
            throw transportError(region.name(), e);
        }
    }

    private boolean advance(StatementClient client)
    {
        try {
            return client.advance();
        }
        catch (RuntimeException e) {
            throw transportError(region.name(), e);
        }
    }

    private static String quoted(String identifier)
    {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    private static String literal(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }
}
