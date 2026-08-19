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

import io.trino.spi.connector.ConnectorSplit;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A whole-table scan of one cluster; the page source paginates through that
 * cluster's API server list endpoint.
 *
 * @param cluster cluster to scan in multi-cluster catalogs; empty for the single
 *         configured cluster
 */
public record KubernetesSplit(Optional<String> cluster)
        implements ConnectorSplit
{
    public KubernetesSplit
    {
        requireNonNull(cluster, "cluster is null");
    }
}
