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

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.plugin.federation.FederationColumns.REGION_COLUMN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestFederationTableHandle
{
    private static final FederationColumnHandle ORDER_KEY = new FederationColumnHandle("orderkey", BIGINT, false);
    private static final FederationColumnHandle NAME = new FederationColumnHandle("name", createVarcharType(25), false);
    private static final FederationColumnHandle PRICE = new FederationColumnHandle("price", DOUBLE, false);

    private final JsonCodec<FederationTableHandle> tableHandleCodec;
    private final JsonCodec<FederationColumnHandle> columnHandleCodec;

    TestFederationTableHandle()
    {
        JsonMapper jsonMapper = new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(
                        Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                        Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE)))
                .withJsonSerializers(ImmutableMap.of(
                        Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
                .get();
        JsonCodecFactory codecFactory = new JsonCodecFactory(jsonMapper);
        tableHandleCodec = codecFactory.jsonCodec(FederationTableHandle.class);
        columnHandleCodec = codecFactory.jsonCodec(FederationColumnHandle.class);
    }

    @Test
    void testColumnHandleRoundTrip()
    {
        assertThat(columnHandleCodec.fromJson(columnHandleCodec.toJson(ORDER_KEY))).isEqualTo(ORDER_KEY);
        assertThat(columnHandleCodec.fromJson(columnHandleCodec.toJson(NAME))).isEqualTo(NAME);
        assertThat(columnHandleCodec.fromJson(columnHandleCodec.toJson(REGION_COLUMN))).isEqualTo(REGION_COLUMN);
    }

    @Test
    void testTableHandleRoundTrip()
    {
        FederationTableHandle handle = FederationTableHandle.of(
                new SchemaTableName("tiny", "orders"),
                ImmutableList.of(ORDER_KEY, NAME, REGION_COLUMN),
                ImmutableList.of("region-a", "region-b"));
        assertThat(tableHandleCodec.fromJson(tableHandleCodec.toJson(handle))).isEqualTo(handle);
    }

    @Test
    void testFullyPopulatedTableHandleRoundTrip()
    {
        TupleDomain<FederationColumnHandle> constraint = TupleDomain.withColumnDomains(ImmutableMap.of(
                REGION_COLUMN, Domain.singleValue(REGION_COLUMN.type(), utf8Slice("region-a")),
                ORDER_KEY, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 5L, true, 100L, false)), true)));
        FederationAggregation aggregation = new FederationAggregation(
                ImmutableList.of(NAME),
                ImmutableList.of(
                        new FederationAggregateColumn(Optional.empty(), "row_count", BIGINT, CombineKind.COUNT_SUM),
                        new FederationAggregateColumn(Optional.of(ORDER_KEY), "sum_orderkey", BIGINT, CombineKind.SUM_LONG),
                        new FederationAggregateColumn(Optional.of(PRICE), "avg_price", DOUBLE, CombineKind.AVG_DOUBLE)));
        FederationTopN topN = new FederationTopN(
                ImmutableList.of(
                        new FederationSortColumn(ORDER_KEY, SortOrder.DESC_NULLS_LAST),
                        new FederationSortColumn(NAME, SortOrder.ASC_NULLS_FIRST)),
                10);
        FederationTableHandle handle = new FederationTableHandle(
                new SchemaTableName("tiny", "orders"),
                ImmutableList.of(ORDER_KEY, NAME, REGION_COLUMN),
                ImmutableList.of("region-a"),
                constraint,
                OptionalLong.empty(),
                Optional.of(topN),
                Optional.of(aggregation));

        assertThat(tableHandleCodec.fromJson(tableHandleCodec.toJson(handle))).isEqualTo(handle);
    }

    @Test
    void testLimitAndTopNAreMutuallyExclusive()
    {
        FederationTopN topN = new FederationTopN(ImmutableList.of(new FederationSortColumn(ORDER_KEY, SortOrder.ASC_NULLS_FIRST)), 5);
        assertThatThrownBy(() -> new FederationTableHandle(
                new SchemaTableName("tiny", "orders"),
                ImmutableList.of(ORDER_KEY, REGION_COLUMN),
                ImmutableList.of("region-a"),
                TupleDomain.all(),
                OptionalLong.of(10),
                Optional.of(topN),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("mutually exclusive");
    }

    @Test
    void testWithers()
    {
        FederationTableHandle handle = FederationTableHandle.of(
                new SchemaTableName("tiny", "orders"),
                ImmutableList.of(ORDER_KEY, NAME, REGION_COLUMN),
                ImmutableList.of("region-a", "region-b"));

        TupleDomain<FederationColumnHandle> constraint = TupleDomain.withColumnDomains(
                ImmutableMap.of(ORDER_KEY, Domain.singleValue(BIGINT, 42L)));
        FederationTopN topN = new FederationTopN(ImmutableList.of(new FederationSortColumn(ORDER_KEY, SortOrder.ASC_NULLS_FIRST)), 5);
        FederationAggregation aggregation = new FederationAggregation(
                ImmutableList.of(),
                ImmutableList.of(new FederationAggregateColumn(Optional.of(ORDER_KEY), "max_orderkey", BIGINT, CombineKind.MAX)));

        FederationTableHandle changed = handle
                .withColumns(ImmutableList.of(ORDER_KEY, REGION_COLUMN))
                .withActiveRegions(ImmutableList.of("region-b"))
                .withConstraint(constraint)
                .withLimit(7)
                .withAggregation(aggregation);

        assertThat(changed.schemaTableName()).isEqualTo(handle.schemaTableName());
        assertThat(changed.columns()).containsExactly(ORDER_KEY, REGION_COLUMN);
        assertThat(changed.activeRegions()).containsExactly("region-b");
        assertThat(changed.constraint()).isEqualTo(constraint);
        assertThat(changed.limit()).isEqualTo(OptionalLong.of(7));
        assertThat(changed.topN()).isEmpty();
        assertThat(changed.aggregation()).contains(aggregation);
        assertThat(handle.constraint().isAll()).isTrue();

        assertThat(tableHandleCodec.fromJson(tableHandleCodec.toJson(changed))).isEqualTo(changed);

        FederationTableHandle sorted = changed.withTopN(topN);
        assertThat(sorted.topN()).contains(topN);
        assertThat(sorted.limit()).isEmpty();

        assertThat(tableHandleCodec.fromJson(tableHandleCodec.toJson(sorted))).isEqualTo(sorted);
    }

    @Test
    void testJsonContainsColumnFields()
    {
        String json = columnHandleCodec.toJson(new FederationColumnHandle("id", BIGINT, false));
        assertThat(json).contains("\"id\"");
        assertThat(json).contains("bigint");
        assertThat(columnHandleCodec.toJson(REGION_COLUMN)).contains("_region");
    }
}
