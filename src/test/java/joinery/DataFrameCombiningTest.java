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
import joinery.DataFrame.JoinType;

import org.junit.Before;
import org.junit.Test;

public class DataFrameCombiningTest {
    private DataFrame<Object> left;
    private DataFrame<Object> right;

    @Before
    public void setUp() throws Exception {
        left = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("left.csv"));
        right = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("right.csv"));
    }

    @Test
    public void testJoinLeftOnIndex() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L,
                    "a", "a", "a",
                    10.0, 20.0, 30.0,
                    1L, 2L, 4L,
                    "b", "b", "b",
                    30.0, 40.0, 80.0
                },
                left.join(right).toArray()
            );
    }

    @Test
    public void testJoinRightOnIndex() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 4L,
                    "b", "b", "b",
                    30.0, 40.0, 80.0,
                    1L, 2L, 3L,
                    "a", "a", "a",
                    10.0, 20.0, 30.0
                },
                left.join(right, JoinType.RIGHT).toArray()
            );
    }

    @Test
    public void testJoinLeftWithMissing() {
        assertArrayEquals(
                new Object[] {
                    "a", "a", "a",
                    10.0, 20.0, 30.0,
                    "b", "b", null,
                    30.0, 40.0, null
                },
                left.reindex(0).join(right.reindex(0)).toArray()
            );
    }

    @Test
    public void testJoinRightWithMissing() {
        assertArrayEquals(
                new Object[] {
                    "b", "b", "b",
                    30.0, 40.0, 80.0,
                    "a", "a", null,
                    10.0, 20.0, null
                },
                left.reindex(0).join(right.reindex(0), JoinType.RIGHT).toArray()
            );
    }

    @Test
    public void testJoinInnerWithMissing() {
        assertArrayEquals(
                new Object[] {
                    "a", "a",
                    10.0, 20.0,
                    "b", "b",
                    30.0, 40.0
                },
                left.reindex(0).join(right.reindex(0), JoinType.INNER).toArray()
            );
    }

    @Test
    public void testJoinOuterWithMissing() {
        assertArrayEquals(
                new Object[] {
                    "a", "a", "a", null,
                    10.0, 20.0, 30.0, null,
                    "b", "b", null, "b",
                    30.0, 40.0, null, 80.0
                },
                left.reindex(0).join(right.reindex(0), JoinType.OUTER).toArray()
            );
    }

    @Test
    public void testJoinOnColIndexWithMissing() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L,
                    "a", "a", "a",
                    10.0, 20.0, 30.0,
                    1L, 2L, null,
                    "b", "b", null,
                    30.0, 40.0, null
                },
                left.joinOn(right, 0).toArray()
            );
    }

    @Test
    public void testJoinOnColNameWithMissing() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L,
                    "a", "a", "a",
                    10.0, 20.0, 30.0,
                    1L, 2L, null,
                    "b", "b", null,
                    30.0, 40.0, null
                },
                left.joinOn(right, "key").toArray()
            );
    }

    @Test
    public void testJoinOuterOnColIndexWithMissing() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L, null,
                    "a", "a", "a", null,
                    10.0, 20.0, 30.0, null,
                    1L, 2L, null, 4L,
                    "b", "b", null, "b",
                    30.0, 40.0, null, 80.0
                },
                left.joinOn(right, JoinType.OUTER, 0).toArray()
            );
    }

    @Test
    public void testJoinOuterOnColNameWithMissing() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L, null,
                    "a", "a", "a", null,
                    10.0, 20.0, 30.0, null,
                    1L, 2L, null, 4L,
                    "b", "b", null, "b",
                    30.0, 40.0, null, 80.0
                },
                left.joinOn(right, JoinType.OUTER, "key").toArray()
            );
    }

    @Test
    public void testJoinInnerOnColIndexWithMissing() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L,
                    "a", "a",
                    10.0, 20.0,
                    1L, 2L,
                    "b", "b",
                    30.0, 40.0
                },
                left.joinOn(right, JoinType.INNER, 0).toArray()
            );
    }

    @Test
    public void testJoinInnerOnColNameWithMissing() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L,
                    "a", "a",
                    10.0, 20.0,
                    1L, 2L,
                    "b", "b",
                    30.0, 40.0
                },
                left.joinOn(right, JoinType.INNER, "key").toArray()
            );
    }

    @Test
    public void testMergeLeft() {
        left.convert(String.class);
        right.convert(String.class);
        assertArrayEquals(
                new Object[] {
                    "a", "a", "a",
                    10.0, 20.0, 30.0,
                    "b", "b", null,
                    30.0, 40.0, null
                },
                left.merge(right).toArray()
            );
    }

    @Test
    public void testMergeRight() {
        left.convert(String.class);
        right.convert(String.class);
        assertArrayEquals(
                new Object[] {
                    "b", "b", "b",
                    30.0, 40.0, 80.0,
                    "a", "a", null,
                    10.0, 20.0, null
                },
                left.merge(right, JoinType.RIGHT).toArray()
            );
    }

    @Test
    public void testMergeOuter() {
        left.convert(String.class);
        right.convert(String.class);
        assertArrayEquals(
                new Object[] {
                    "a", "a", "a", null,
                    10.0, 20.0, 30.0, null,
                    "b", "b", null, "b",
                    30.0, 40.0, null, 80.0
                },
                left.merge(right, JoinType.OUTER).toArray()
            );
    }

    @Test
    public void testMergeInner() {
        left.convert(String.class);
        right.convert(String.class);
        assertArrayEquals(
                new Object[] {
                    "a", "a",
                    10.0, 20.0,
                    "b", "b",
                    30.0, 40.0
                },
                left.merge(right, JoinType.INNER).toArray()
            );
    }

    @Test
    public void testUpdate() {
        right.set(1, 1, null);
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 4L,
                    "b", "a" /* remains from left */, "b",
                    30.0, 40.0, 80.0
                },
                left.update(right).toArray()
            );
    }

    @Test
    public void testUpdateNoNulls() {
        assertArrayEquals(
                right.toArray(),
                left.update(right).toArray()
            );
    }

    @Test
    public void testCoalesce() {
        left.set(1, 1, null);
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L,
                    "a", "b" /* taken from right */, "a",
                    10.0, 20.0, 30.0
                },
                left.coalesce(right).toArray()
            );
    }

    @Test
    public void testCoalesceNoNulls() {
        assertArrayEquals(
                left.toArray(),
                left.coalesce(right).toArray()
            );
    }

    @Test
    public void testConcat() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L, 1L, 2L, 4L,
                    "a", "a", "a", null, null, null,
                    10.0, 20.0, 30.0, 30.0, 40.0, 80.0,
                    null, null, null, "b", "b", "b"
                },
                left.concat(right).toArray()
            );
    }

    @Test
    public void testOuterConcat() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L, 1L, 2L, 4L,
                    "a", "a", "a", null, null, null,
                    10.0, 20.0, 30.0, 30.0, 40.0, 80.0,
                    null, null, null, "b", "b", "b"
                },
                left.concat(JoinType.OUTER, right).toArray()
            );
    }

    @Test
    public void testInnerConcat() {
        assertArrayEquals(
                new Object[] {
                    1L, 2L, 3L, 1L, 2L, 4L,
                    10.0, 20.0, 30.0, 30.0, 40.0, 80.0,
                },
                left.concat(JoinType.INNER, right).toArray()
            );
    }
}
