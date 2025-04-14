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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.ArrayList;

import static java.util.Objects.requireNonNull;

public class K8SRecordSet
        implements RecordSet
{
    private final K8SClient k8sClient;
    private final K8SSplit split;
    private final List<K8SColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ObjectMapper objectMapper;
    private final TypeManager typeManager;

    public K8SRecordSet(K8SClient k8sClient, K8SSplit split, List<K8SColumnHandle> columnHandles, ObjectMapper objectMapper, TypeManager typeManager)
    {
        this.k8sClient = requireNonNull(k8sClient, "k8sClient is null");
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (K8SColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        K8STableHandle tableHandle = (K8STableHandle) split.getTableHandle();
        List<Object> values = new ArrayList<>(); // Initialize empty values list
        return new K8SRecordCursor(k8sClient, split, columnHandles, tableHandle.getSchemaName(), tableHandle.getTableName(), objectMapper, typeManager, values, columnTypes);
    }
}
