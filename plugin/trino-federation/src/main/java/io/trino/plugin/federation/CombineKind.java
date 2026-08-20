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

/**
 * How the per-region partial results of one aggregate are combined into the final value on
 * the central cluster. The AVG kinds are reserved for {@code avg} decomposed into remote
 * sum+count pairs and divided at combine time.
 */
public enum CombineKind
{
    SUM_LONG,
    SUM_DOUBLE,
    SUM_DECIMAL,
    COUNT_SUM,
    MIN,
    MAX,
    AVG_DOUBLE,
    AVG_DECIMAL,
}
