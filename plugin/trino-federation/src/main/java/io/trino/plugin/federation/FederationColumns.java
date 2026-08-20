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

import static io.trino.spi.type.VarcharType.VARCHAR;

public final class FederationColumns
{
    /**
     * Synthetic column holding the name of the region each row came from. It is a regular
     * selectable column, appended last to every federated table.
     */
    public static final String REGION_COLUMN_NAME = "_region";
    public static final FederationColumnHandle REGION_COLUMN = new FederationColumnHandle(REGION_COLUMN_NAME, VARCHAR, true);

    private FederationColumns() {}
}
