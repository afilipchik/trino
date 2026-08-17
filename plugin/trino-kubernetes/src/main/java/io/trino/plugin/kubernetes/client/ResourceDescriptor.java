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
package io.trino.plugin.kubernetes.client;

import static java.util.Objects.requireNonNull;

/**
 * A list/get-able Kubernetes API resource. The API group maps to a Trino schema
 * ({@code core} for the legacy group) and the resource plural name to a table.
 */
public record ResourceDescriptor(
        String schemaName,
        String tableName,
        String group,
        String version,
        String kind,
        boolean namespaced)
{
    public ResourceDescriptor
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(group, "group is null");
        requireNonNull(version, "version is null");
        requireNonNull(kind, "kind is null");
    }

    /**
     * The URL prefix for this resource's group version, without leading slash,
     * for example {@code api/v1} or {@code apis/apps/v1}.
     */
    public String groupVersionPath()
    {
        if (group.isEmpty()) {
            return "api/" + version;
        }
        return "apis/" + group + "/" + version;
    }

    public String apiVersion()
    {
        if (group.isEmpty()) {
            return version;
        }
        return group + "/" + version;
    }
}
