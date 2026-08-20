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
package io.trino.plugin.federation.client;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.TypeUtils.writeNativeValue;

/**
 * Accumulates converted rows (stack representation, as produced by
 * {@link RegionQueryResults}) into {@link Page}s for the given column types.
 */
public class RegionPageBuilder
{
    private final List<Type> types;
    private final PageBuilder pageBuilder;

    public RegionPageBuilder(List<Type> types)
    {
        this.types = ImmutableList.copyOf(types);
        this.pageBuilder = new PageBuilder(this.types);
    }

    public void appendRow(List<Object> nativeValues)
    {
        checkArgument(nativeValues.size() == types.size(), "Expected %s values but got %s", types.size(), nativeValues.size());
        pageBuilder.declarePosition();
        for (int channel = 0; channel < types.size(); channel++) {
            writeNativeValue(types.get(channel), pageBuilder.getBlockBuilder(channel), nativeValues.get(channel));
        }
    }

    public boolean isFull()
    {
        return pageBuilder.isFull();
    }

    public boolean isEmpty()
    {
        return pageBuilder.isEmpty();
    }

    public long retainedSizeInBytes()
    {
        return pageBuilder.getRetainedSizeInBytes();
    }

    /**
     * Returns the accumulated rows as a page and resets the builder.
     */
    public Page flush()
    {
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }
}
