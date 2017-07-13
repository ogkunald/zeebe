/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
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
package io.zeebe.hashindex;

import static io.zeebe.hashindex.HashIndexDataBuffer.ALLOCATION_FACTOR;
import static io.zeebe.hashindex.HashIndexDescriptor.BLOCK_DATA_OFFSET;
import static io.zeebe.hashindex.HashIndexDescriptor.getRecordLength;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.After;
import org.junit.Test;

public class HashIndexTest
{

    public static final long KEY = Long.MAX_VALUE;
    public static final long VALUE = Long.MAX_VALUE;
    public static final long MISSING_VALUE = 0;

    private Long2LongHashIndex index;

    @After
    public void tearDown()
    {
        if (index != null)
        {
            index.close();
        }
    }

    @Test
    public void shouldRoundToPowerOfTwo()
    {
        // given index not power of two
        final int indexSize = 11;

        // when
        index = new Long2LongHashIndex(indexSize, 1);

        // then index size is set to next power of two
        assertThat(index.indexSize).isEqualTo(16);

        // and a value can be inserted and read again
        index.put(KEY, VALUE);
        assertThat(index.get(KEY, MISSING_VALUE)).isEqualTo(VALUE);
    }

    @Test
    public void shouldUseLimitPowerOfTwo()
    {
        // given index which is higher than the limit 1 << 27
        final int indexSize = 1 << 28;

        // when
        index = new Long2LongHashIndex(indexSize, 1);

        // then index size is set to max value
        assertThat(index.indexSize).isEqualTo(HashIndex.MAX_INDEX_SIZE);

        // and a value can be inserted and read again
        index.put(KEY, VALUE);
        assertThat(index.get(KEY, MISSING_VALUE)).isEqualTo(VALUE);
    }

    @Test
    public void shouldThrowOnRecordLengthOverflow()
    {
        assertThatThrownBy(() ->
            getRecordLength(1, Integer.MAX_VALUE)
        )
            .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() ->
            getRecordLength(Integer.MAX_VALUE, 1)
        )
            .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() ->
            getRecordLength(Integer.MAX_VALUE / 2, Integer.MAX_VALUE / 2)
        )
            .isInstanceOf(ArithmeticException.class);
    }


    @Test
    public void shouldThrowOnMaxBockLengthOverflow()
    {
        assertThatThrownBy(() ->
                new Long2LongHashIndex(1, Integer.MAX_VALUE)
        )
                .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() ->
                new Long2LongHashIndex(1, Integer.MAX_VALUE / 20)
        )
                .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() ->
                new Long2BytesHashIndex(1, Integer.MAX_VALUE, 8)
        )
                .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() ->
                new Long2BytesHashIndex(1, Integer.MAX_VALUE / 20, 8)
        )
                .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() ->
                new Bytes2LongHashIndex(1, Integer.MAX_VALUE, 8)
        )
                .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() ->
                new Bytes2LongHashIndex(1, Integer.MAX_VALUE / 20, 8)
        )
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    public void shouldThrowOnLengthOverflowOnInitialAllocation()
    {
        assertThatThrownBy(() ->
            new Long2LongHashIndex(1, maxRecordPerBlockForLong2LongIndex() + 1)
        )
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Unable to allocate index data buffer")
            .hasCauseInstanceOf(ArithmeticException.class);
    }

    private int maxRecordPerBlockForLong2LongIndex()
    {
        return (Integer.MAX_VALUE - BLOCK_DATA_OFFSET - BLOCK_DATA_OFFSET * ALLOCATION_FACTOR) / (getRecordLength(SIZE_OF_LONG, SIZE_OF_LONG) * ALLOCATION_FACTOR);
    }

}
