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
package io.trino.plugin.federation.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * SQL text to send to a region, the ordered columns the result stream produces, and the
 * constraint columns whose domains could not be rendered — the caller must keep those as a
 * post-filter because the remote WHERE clause does not enforce them.
 */
public record RemoteQuery(String sql, List<RemoteColumn> outputColumns, Set<RemoteColumn> unsupportedFilterColumns)
{
    public RemoteQuery
    {
        requireNonNull(sql, "sql is null");
        outputColumns = ImmutableList.copyOf(requireNonNull(outputColumns, "outputColumns is null"));
        unsupportedFilterColumns = ImmutableSet.copyOf(requireNonNull(unsupportedFilterColumns, "unsupportedFilterColumns is null"));
    }
}
