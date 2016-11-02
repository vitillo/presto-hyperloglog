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

package com.mozilla.presto.hyperloglog;

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.twitter.algebird.DenseHLL;

import static com.google.common.base.Preconditions.checkNotNull;

public class HyperLogLogStateFactory
        implements AccumulatorStateFactory<HyperLogLogState>
{
    @Override
    public HyperLogLogState createSingleState()
    {
       return new SingleHyperLogLogState();
    }

    @Override
    public Class<? extends HyperLogLogState> getSingleStateClass()
    {
        return SingleHyperLogLogState.class;
    }

    @Override
    public HyperLogLogState createGroupedState()
    {
        return new GroupedHyperLogLogState();
    }

    @Override
    public Class<? extends HyperLogLogState> getGroupedStateClass()
    {
        return GroupedHyperLogLogState.class;
    }

    public static class GroupedHyperLogLogState
            extends AbstractGroupedAccumulatorState
            implements HyperLogLogState
    {
        private final ObjectBigArray<DenseHLL> bfs = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            bfs.ensureCapacity(size);
        }

        @Override
        public DenseHLL getHyperLogLog()
        {
            return bfs.get(getGroupId());
        }

        @Override
        public void setHyperLogLog(DenseHLL hll)
        {
            checkNotNull(hll, "value is null");
            bfs.set(getGroupId(), hll);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return size + bfs.sizeOf();
        }
    }

    public static class SingleHyperLogLogState
            implements HyperLogLogState
    {
        private DenseHLL hll;

        @Override
        public DenseHLL getHyperLogLog()
        {
            return hll;
        }

        @Override
        public void setHyperLogLog(DenseHLL hll)
        {
            this.hll = hll;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            return hll.size();
        }
    }
}
