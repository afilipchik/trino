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
package io.trino.plugin.federation;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.federation.client.RegionClients;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.DistributedQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.federation.FederationColumns.REGION_COLUMN;
import static io.trino.plugin.federation.FederationColumns.REGION_COLUMN_NAME;
import static io.trino.plugin.federation.FederationErrorCode.FEDERATION_REGION_UNREACHABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.PointerType.TARGET_ID;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestFederationMetadata
{
    private static final SchemaTableName ITEMS = new SchemaTableName("default", "items");

    private DistributedQueryRunner queryRunner;
    private URI baseUrl;
    private RegionClients regionClients;
    private FederationMetadata metadata;

    @BeforeAll
    void setUp()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog("memory").setSchema("default").build())
                .setWorkerCount(0)
                .build();
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");
        queryRunner.execute("CREATE SCHEMA memory.sales");
        queryRunner.execute("CREATE TABLE memory.default.items (id integer, name varchar(10), tags array(integer), created timestamp(6))");
        queryRunner.execute("CREATE TABLE memory.sales.orders (orderkey bigint, total double)");

        baseUrl = queryRunner.getCoordinator().getBaseUrl();
        regionClients = new RegionClients(new FederationConfig()
                .setRegions("region-a=" + baseUrl)
                .setRemoteCatalog("memory"));
        metadata = new FederationMetadata(regionClients);
    }

    @AfterAll
    void tearDown()
    {
        if (regionClients != null) {
            regionClients.close();
            regionClients = null;
        }
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Test
    void testListSchemaNames()
    {
        List<String> schemas = metadata.listSchemaNames(SESSION);
        assertThat(schemas).contains("default", "sales");
        assertThat(schemas).doesNotContain("information_schema");
    }

    @Test
    void testListTables()
    {
        assertThat(metadata.listTables(SESSION, Optional.of("default"))).containsExactly(ITEMS);
        assertThat(metadata.listTables(SESSION, Optional.empty()))
                .contains(ITEMS, new SchemaTableName("sales", "orders"))
                .allMatch(table -> !table.getSchemaName().equals("information_schema"));
        assertThat(metadata.listTables(SESSION, Optional.of("information_schema"))).isEmpty();
    }

    @Test
    void testGetTableHandle()
    {
        FederationTableHandle handle = metadata.getTableHandle(SESSION, ITEMS, Optional.empty(), Optional.empty());
        assertThat(handle).isNotNull();
        assertThat(handle.schemaTableName()).isEqualTo(ITEMS);
        // the unmapped array(integer) column is skipped and _region is appended last
        assertThat(handle.columns()).containsExactly(
                new FederationColumnHandle("id", INTEGER, false),
                new FederationColumnHandle("name", createVarcharType(10), false),
                new FederationColumnHandle("created", createTimestampType(6), false),
                REGION_COLUMN);
        assertThat(handle.activeRegions()).containsExactly("region-a");
        assertThat(handle.constraint()).isEqualTo(TupleDomain.<FederationColumnHandle>all());
        assertThat(handle.limit()).isEmpty();
        assertThat(handle.topN()).isEmpty();
        assertThat(handle.aggregation()).isEmpty();
    }

    @Test
    void testGetTableHandleMissingTable()
    {
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("default", "missing_table"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("missing_schema", "missing_table"), Optional.empty(), Optional.empty())).isNull();
        assertThat(metadata.getTableHandle(SESSION, new SchemaTableName("information_schema", "tables"), Optional.empty(), Optional.empty())).isNull();
    }

    @Test
    void testGetTableHandleRejectsVersions()
    {
        ConnectorTableVersion version = new ConnectorTableVersion(TARGET_ID, BIGINT, 1L);
        assertTrinoExceptionThrownBy(() -> metadata.getTableHandle(SESSION, ITEMS, Optional.of(version), Optional.empty()))
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    void testGetTableMetadata()
    {
        FederationTableHandle handle = metadata.getTableHandle(SESSION, ITEMS, Optional.empty(), Optional.empty());
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, handle);
        assertThat(tableMetadata.getTable()).isEqualTo(ITEMS);
        assertThat(tableMetadata.getColumns()).containsExactly(
                new ColumnMetadata("id", INTEGER),
                new ColumnMetadata("name", createVarcharType(10)),
                new ColumnMetadata("created", createTimestampType(6)),
                new ColumnMetadata(REGION_COLUMN_NAME, VARCHAR));
        assertThat(tableMetadata.getColumns().getLast().isHidden()).isFalse();
    }

    @Test
    void testGetColumnHandles()
    {
        FederationTableHandle handle = metadata.getTableHandle(SESSION, ITEMS, Optional.empty(), Optional.empty());
        Map<String, ColumnHandle> columns = metadata.getColumnHandles(SESSION, handle);
        assertThat(columns.keySet()).containsExactly("id", "name", "created", REGION_COLUMN_NAME);
        assertThat(columns.get(REGION_COLUMN_NAME)).isEqualTo(REGION_COLUMN);

        ColumnMetadata regionMetadata = metadata.getColumnMetadata(SESSION, handle, columns.get(REGION_COLUMN_NAME));
        assertThat(regionMetadata.getName()).isEqualTo(REGION_COLUMN_NAME);
        assertThat(regionMetadata.getType()).isEqualTo(VARCHAR);
        assertThat(regionMetadata.isHidden()).isFalse();
    }

    @Test
    void testStreamRelationColumns()
    {
        Iterator<RelationColumnsMetadata> relations = metadata.streamRelationColumns(SESSION, Optional.of("default"), UnaryOperator.identity());
        List<RelationColumnsMetadata> relationList = ImmutableList.copyOf(relations);
        assertThat(relationList).hasSize(1);
        RelationColumnsMetadata items = relationList.getFirst();
        assertThat(items.name()).isEqualTo(ITEMS);
        assertThat(items.tableColumns().orElseThrow().stream().map(ColumnMetadata::getName).collect(toImmutableList()))
                .containsExactly("id", "name", "created", REGION_COLUMN_NAME);
    }

    @Test
    void testFirstRegionUnreachableFailsOver()
    {
        try (RegionClients failoverClients = new RegionClients(new FederationConfig()
                .setRegions("region-down=http://127.0.0.1:1,region-a=" + baseUrl)
                .setRemoteCatalog("memory"))) {
            FederationMetadata failoverMetadata = new FederationMetadata(failoverClients);

            assertThat(failoverMetadata.listSchemaNames(SESSION)).contains("default", "sales");
            assertThat(failoverMetadata.listTables(SESSION, Optional.of("default"))).containsExactly(ITEMS);

            FederationTableHandle handle = failoverMetadata.getTableHandle(SESSION, ITEMS, Optional.empty(), Optional.empty());
            assertThat(handle).isNotNull();
            assertThat(handle.activeRegions()).containsExactly("region-down", "region-a");
        }
    }

    @Test
    void testAllRegionsUnreachable()
    {
        try (RegionClients unreachableClients = new RegionClients(new FederationConfig()
                .setRegions("region-x=http://127.0.0.1:1,region-y=http://127.0.0.1:2")
                .setRemoteCatalog("memory"))) {
            FederationMetadata unreachableMetadata = new FederationMetadata(unreachableClients);

            assertTrinoExceptionThrownBy(() -> unreachableMetadata.listSchemaNames(SESSION))
                    .hasErrorCode(FEDERATION_REGION_UNREACHABLE)
                    .hasMessageContaining("region-x")
                    .hasMessageContaining("region-y");
            assertTrinoExceptionThrownBy(() -> unreachableMetadata.getTableHandle(SESSION, ITEMS, Optional.empty(), Optional.empty()))
                    .hasErrorCode(FEDERATION_REGION_UNREACHABLE)
                    .hasMessageContaining("region-x")
                    .hasMessageContaining("region-y");
        }
    }
}
