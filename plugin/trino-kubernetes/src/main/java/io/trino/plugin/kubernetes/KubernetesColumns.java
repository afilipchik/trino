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
import io.trino.spi.type.RowType;

import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Synthetic columns added to every table: {@code name} and {@code namespace}
 * scalars derived from object metadata (Kubernetes objects never have top-level
 * fields with these names) plus the hidden merge row id used by UPDATE and DELETE.
 * The scalars make the two selectors the Kubernetes API supports natively easy to
 * push down, and make INSERT ergonomic without constructing the full metadata row.
 */
public final class KubernetesColumns
{
    public static final String NAME_COLUMN = "name";
    public static final String NAMESPACE_COLUMN = "namespace";
    public static final String MERGE_ROW_ID_COLUMN = "$merge_row_id";

    public static final RowType MERGE_ROW_ID_TYPE = RowType.from(ImmutableList.of(
            RowType.field("namespace", VARCHAR),
            RowType.field("name", VARCHAR),
            RowType.field("resource_version", VARCHAR)));

    public static final KubernetesColumnHandle NAME_HANDLE = new KubernetesColumnHandle(NAME_COLUMN, "", VARCHAR, false);
    public static final KubernetesColumnHandle NAMESPACE_HANDLE = new KubernetesColumnHandle(NAMESPACE_COLUMN, "", VARCHAR, false);
    public static final KubernetesColumnHandle MERGE_ROW_ID_HANDLE = new KubernetesColumnHandle(MERGE_ROW_ID_COLUMN, "", MERGE_ROW_ID_TYPE, true);

    private KubernetesColumns() {}

    public static boolean isSynthetic(KubernetesColumnHandle column)
    {
        return column.jsonName().isEmpty();
    }
}
