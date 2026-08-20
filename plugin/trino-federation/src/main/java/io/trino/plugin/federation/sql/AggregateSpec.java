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

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A single remote partial aggregate. The output column carries the alias used in the remote
 * SELECT list and the Trino type the caller expects back; the builder never derives output
 * types itself.
 */
public record AggregateSpec(AggregateKind kind, Optional<RemoteColumn> argument, RemoteColumn output)
{
    public AggregateSpec
    {
        requireNonNull(kind, "kind is null");
        requireNonNull(argument, "argument is null");
        requireNonNull(output, "output is null");
        checkArgument((kind == AggregateKind.COUNT_ALL) == argument.isEmpty(),
                "count(*) takes no argument and every other aggregate requires one: %s",
                kind);
    }
}
