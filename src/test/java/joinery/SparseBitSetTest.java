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

package joinery;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import joinery.impl.SparseBitSet;

import org.junit.Before;
import org.junit.Test;

public class SparseBitSetTest {
    private SparseBitSet bits;

    @Before
    public void setUp() {
        //System.out.println(SparseBitSet.parameters());
        bits = new SparseBitSet();
    }

    @Test
    public void testGetInt() {
        assertFalse(bits.get(0));
        for (int i = 0; i < 10; i++) {
            assertFalse(bits.get((int)Math.random() * Integer.MAX_VALUE));
        }
    }

    @Test
    public void testSetInt() {
        bits.set(0);
        assertTrue(bits.get(0));
        assertFalse(bits.get(1));
    }

    @Test
    public void testSetIntInt() {
        bits.set(1, 3);
        assertFalse(bits.get(0));
        for (int i = 1; i < 3; i++) {
            assertTrue(bits.get(i));
        }
        assertFalse(bits.get(3));
    }

    @Test
    public void testClear() {
        bits.set(0);
        assertTrue(bits.get(0));
        bits.clear();
        assertFalse(bits.get(0));
        assertEquals(0, bits.cardinality());
    }

    @Test
    public void testClearInt() {
        bits.set(0,8);
        bits.clear(3);
        assertFalse(bits.get(3));
    }

    @Test
    public void testClearIntInt() {
        bits.set(0, 8);
        bits.clear(3, 6);
        for (int i = 3; i < 6; i++) {
            assertFalse(bits.get(i));
        }
        assertEquals(5, bits.cardinality());
    }

    @Test
    public void testFlipInt() {
        bits.flip(1);
        assertFalse(bits.get(0));
        assertTrue(bits.get(1));
        assertFalse(bits.get(2));
    }

    @Test
    public void testFlipIntInt() {
        bits.flip(1, 4);
        bits.flip(2);
        for (int i = 0; i < 5; i++) {
            if (i % 2 == 0){
                assertFalse(bits.get(i));
            } else {
                assertTrue(bits.get(i));
            }
        }
    }

    @Test
    public void testCardinality() {
        assertEquals(0, bits.cardinality());
        bits.set(1);
        assertEquals(1, bits.cardinality());
        bits.flip(2);
        assertEquals(2, bits.cardinality());
        bits.flip(2);
        assertEquals(1, bits.cardinality());
    }

    @Test
    public void testNextSetBit() {
       int sum = 0;
       for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
           sum += i;
       }
       assertEquals(0, sum);

       bits.set(3);
       bits.set(7);
       for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
           sum += i;
       }
       assertEquals(10, sum);
    }

    @Test
    public void testToString() {
        String s = "";
        for (int i = 0; i < 10; i += 2) {
            s += String.valueOf(i);
            bits.set(i);
        }
        assertEquals(s, bits.toString().replaceAll("[^\\d]", ""));
    }

    @Test
    public void testBoundarys() {
        assertEquals(-1, bits.nextSetBit(0));

        bits.set(0);
        assertTrue(bits.get(0));
        assertEquals(0, bits.nextSetBit(0));
        assertEquals(-1, bits.nextSetBit(1));

        bits.set(63);
        bits.set(64);
        assertEquals(63, bits.nextSetBit(1));
        assertEquals(64, bits.nextSetBit(64));

        bits.set(127);
        bits.set(128);
        assertEquals(127, bits.nextSetBit(65));
        assertEquals(127, bits.nextSetBit(127));
        assertEquals(128, bits.nextSetBit(128));

        bits.set(191);
        bits.set(192);
        assertEquals(191, bits.nextSetBit(129));
        assertEquals(191, bits.nextSetBit(191));
        assertEquals(192, bits.nextSetBit(192));

        bits.set(255, 257);
        assertTrue(bits.get(255));
        assertTrue(bits.get(256));

        bits.set(Integer.MAX_VALUE);
        assertTrue(bits.get(Integer.MAX_VALUE));

        bits.set(Integer.MAX_VALUE - 255);
        assertTrue(bits.get(Integer.MAX_VALUE - 255));

        bits.set(Integer.MAX_VALUE - 256);
        assertTrue(bits.get(Integer.MAX_VALUE - 256));
    }

    @Test(expected=IndexOutOfBoundsException.class)
    public void testSetNegative() {
        bits.set(-1);
    }

    @Test(expected=IndexOutOfBoundsException.class)
    public void testGetNegative() {
        bits.get(-1);
    }

    @Test
    public void testSparse() {
        final List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i += 10_000) {
            bits.set(i);
            expected.add(i);
        }
        final List<Integer> answer = new ArrayList<>();
        for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
            answer.add(i);
            if (i > 200_000)
                return;
        }
        assertArrayEquals(
                expected.toArray(),
                answer.toArray()
            );
    }

    @Test
    public void testLarge() {
        int count = 0;
        for (int i = 0; 0 <= i && i < Integer.MAX_VALUE; i++) {
            bits.set(i);
            assertTrue(bits.get(i));
            i += 0xffff + (int)Math.random() * 0xffff;
            count++;
            assertEquals(count, bits.cardinality());
        }
    }
}
