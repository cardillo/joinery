/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 * Copyright (c) 2026 zc (1.11 line, GLM-5.2 AI-assisted refactor)
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

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

public class DataFrameManipulationTest {
    private DataFrame<Object> df;

    @Before
    public final void setUp() {
        df = new DataFrame<Object>();
    }

    @Test
    public final void testAdd() {
        df.add("test");
        assertEquals(
                "size is equal to number of columns",
                1,
                df.size()
            );
    }

    @Test
    public final void testAddMultiple() {
        df.add("one", "two", "three");
        assertArrayEquals(
                new String[] { "one", "two", "three" },
                df.columns().toArray()
            );
    }

    @Test
    public final void testAddWithRows() {
        df.add("one");
        df.append(Arrays.<Object>asList(1));
        df.add("two");
        assertArrayEquals(
                new String[] { "one", "two" },
                df.columns().toArray()
            );
        assertArrayEquals(
                new Object[] { 1, null },
                df.toArray()
            );
    }

    @Test
    public final void testAddCalculated() {
        DataFrame<Object> newDf = df
                .add("value", Arrays.asList(1, 2, 3))
                .add("squared", row -> Math.pow((Integer)row.get(0), 2));
        assertArrayEquals(
                "calculated column equals square of value",
                new Object[] { 1.0, 4.0, 9.0 },
                newDf.col(1).toArray()
            );
    }

    @Test
    public final void testAddExisting() {
        // 注：原测试 expected=IllegalArgumentException，把「add 同名列」当错误。这是 bug
        // —— join/concat 后两侧同名列很常见。2026-08 修复后 Index 支持重名，add 同名应成功。
        // 详见 joinery.impl.Index 的重名支持。
        df.add("test")
          .add("test");
    }

    @Test
    public final void testDrop() {
        df.add("test1").add("test2");
        final DataFrame<Object> newDf = df.drop("test1");
        assertEquals(
                "original size is equal to number of columns",
                2,
                df.size()
            );
        assertArrayEquals(
                "original column list is correct",
                new Object[] { "test1", "test2" },
                df.columns().toArray()
            );
        assertEquals(
                "new size is equal to number of columns",
                1,
                newDf.size()
            );
        assertArrayEquals(
                "new column list is correct",
                new Object[] { "test2" },
                newDf.columns().toArray()
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public final void testDropInvalid() {
        df.drop("does-not-exist");
    }

    @Test
    public final void testAppend() {
        df.add("test").append(Arrays.<Object>asList(1));
        assertEquals(
                "size is equal to number of columns",
                1,
                df.size()
            );
        assertEquals(
                "length is equal to number of rows",
                1,
                df.length()
            );
        assertArrayEquals(
                "column values are correct",
                new Object[] { 1 },
                df.col(0).toArray()
            );
    }

    @Test
    public final void testAppendExisting() {
        // 注：原测试 expected=IllegalArgumentException，把「append 同名行索引」当错误。这是 bug
        // —— 多条记录同名 index 合法（pandas 允许 df.append 同 index）。2026-08 修复后应成功。
        // 详见 joinery.impl.Index 的重名支持。
        df.add("test")
          .append("one", Collections.emptyList())
          .append("one", Collections.emptyList());
    }
}
