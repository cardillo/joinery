/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package joinery.impl;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.Arrays;

/**
 * A sparse bit set implementation inspired by Drs. Haddon and Lemire.
 *
 * https://github.com/brettwooldridge/SparseBitSet/blob/master/SparseBitSet.pdf
 * http://lemire.me/blog/archives/2012/11/13/fast-sets-of-integers/
 */
public class SparseBitSet {
    //
    // these are the tuning knobs
    //
    // after taking out the bits for indexing longs,
    // how to divide up the remaining bits among the levels
    // larger numbers means levels 2 and 3 are smaller
    // leaving more bits to be indexed in level 1
    private static final int INDEX_FACTOR   = 4;
    // how much bigger is the second level than the first
    // (the third is just whatever bits are leftover)
    private static final int INDEX_GROWTH   = 1;

    // l3 constants
    //                                        v-- number of bits to index a long value
    private static final int L3_SHIFT       = Long.SIZE - 1 - Long.numberOfLeadingZeros(Long.SIZE);
    //                                        v-- divide remaining bits by factor
    private static final int L3_BITS        = (Integer.SIZE - L3_SHIFT) / INDEX_FACTOR;
    private static final int L3_SIZE        = (1 << L3_BITS);
    private static final int L3_MASK        = L3_SIZE - 1;

    // l2 constants
    private static final int L2_SHIFT       = L3_SHIFT + L3_BITS;
    private static final int L2_BITS        = L3_BITS + INDEX_GROWTH;
    private static final int L2_SIZE        = (1 << L2_BITS);
    private static final int L2_MASK        = L2_SIZE - 1;

    // l1 constants
    // 32 bits - index size - 1 more prevent shifting the sign bit
    // into frame
    private static final int L1_SHIFT       = L2_SHIFT + L2_BITS;
    private static final int L1_BITS        = (Integer.SIZE - L1_SHIFT - 1);
    private static final int L1_SIZE        = (1 << L1_BITS);
    // note the l1 mask is one more bit left than size would indicate
    // this prevents masking the sign bit, causing the appropriate
    // index out of bounds exception if a negative index is used
    private static final int L1_MASK        = (L1_SIZE << 1) - 1;

    // l4 mask
    private static final int L4_MASK        = Long.SIZE - 1;

    long[][][] bits = new long[L3_SIZE / INDEX_FACTOR][][];
    int cardinality = 0;

    public boolean get(final int index) {
        final int l1i = (index >> L1_SHIFT) & L1_MASK,
                  l2i = (index >> L2_SHIFT) & L2_MASK,
                  l3i = (index >> L3_SHIFT) & L3_MASK,
                  l4i = (index            ) & L4_MASK;
        // index < 0 allowed through so appropriate index out of bounds exception is thrown
        if (index < 0 || l1i < bits.length && (bits[l1i] != null && bits[l1i][l2i] != null)) {
            return (bits[l1i][l2i][l3i] & (1L << l4i)) != 0L;
        }
        return false;
    }

    public void set(final int index, final boolean value) {
        final int l1i = (index >> L1_SHIFT) & L1_MASK,
                  l2i = (index >> L2_SHIFT) & L2_MASK,
                  l3i = (index >> L3_SHIFT) & L3_MASK,
                  l4i = (index            ) & L4_MASK;
        if (value) {
            if (bits.length <= l1i && l1i < L1_SIZE) {
                final int size = min(L1_SIZE, max(bits.length << 1,
                    1 << (Integer.SIZE - Integer.numberOfLeadingZeros(l1i))));
                if (bits.length < size) {
                    bits = Arrays.copyOf(bits, size);
                }
            }
            if (bits[l1i] == null) {
                bits[l1i] = new long[L2_SIZE][];
            }
            if (bits[l1i][l2i] == null) {
                bits[l1i][l2i] = new long[L3_SIZE];
            }

            bits[l1i][l2i][l3i] |= (1L << l4i);
            cardinality++;
        } else {
            // don't allocate blocks if clearing bits
            if (l1i < bits.length && bits[l1i] != null && bits[l1i][l2i] != null) {
                bits[l1i][l2i][l3i] &= ~(1L << l4i);
                cardinality--;
            }
        }
    }

    public void set(final int index) {
        set(index, true);
    }

    public void set(final int start, final int end) {
        for (int i = start; i < end; i++) {
            set(i);
        }
    }

    public void clear(final int index) {
        set(index, false);
    }

    public void clear(final int start, final int end) {
        for (int i = start; i < end; i++) {
            clear(i);
        }
    }

    public void flip(final int index) {
        set(index, !get(index));
    }

    public void flip(final int start, final int end) {
        for (int i = start; i < end; i++) {
            flip(i);
        }
    }

    public void clear() {
        Arrays.fill(bits, null);
        cardinality = 0;
    }

    public int cardinality() {
        return cardinality;
    }

    public int nextSetBit(final int index) {
        int l1i = (index >> L1_SHIFT) & L1_MASK,
            l2i = (index >> L2_SHIFT) & L2_MASK,
            l3i = (index >> L3_SHIFT) & L3_MASK,
            l4i = (index            ) & L4_MASK;

        for ( ; l1i < bits.length; l1i++, l2i = 0) {
            for ( ; bits[l1i] != null && l2i < bits[l1i].length; l2i++, l3i = 0) {
                for ( ; bits[l1i][l2i] != null && l3i < bits[l1i][l2i].length; l3i++, l4i = 0) {
                    l4i += Long.numberOfTrailingZeros(bits[l1i][l2i][l3i] >> l4i);
                    if ((bits[l1i][l2i][l3i] & (1L << l4i)) != 0L) {
                        return (l1i << L1_SHIFT) |
                               (l2i << L2_SHIFT) |
                               (l3i << L3_SHIFT) |
                               l4i;
                    }
                }
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = nextSetBit(0); i >= 0; i = nextSetBit(i + 1)) {
            if (sb.length() > 1) {
                sb.append(", ");
            }
            sb.append(i);
        }
        sb.append("}");
        return sb.toString();
    }

    public static String parameters() {
        final StringBuilder sb = new StringBuilder();
        sb.append(String.format("%s parameters:\n", SparseBitSet.class.getName()))
          .append(String.format("size:\tlevel 1=%d\tlevel 2=%d\tlevel 3=%d\n", L1_SIZE, L2_SIZE, L3_SIZE))
          .append(String.format("bits:\tlevel 1=%d\tlevel 2=%d\tlevel 3=%d\n", L1_BITS, L2_BITS, L3_BITS))
          .append(String.format("shift:\tlevel 1=%d\tlevel 2=%d\tlevel 3=%d\n", L1_SHIFT, L2_SHIFT, L3_SHIFT))
          .append(String.format("mask:\tlevel 1=%s\tlevel 2=%s\tlevel 3=%s\n",
                Integer.toHexString(L1_MASK), Integer.toHexString(L2_MASK), Integer.toHexString(L3_MASK)));
        return sb.toString();
    }
}
