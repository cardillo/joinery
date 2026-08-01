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

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class DataFrameBasicTest {
    private DataFrame<Object> df;

    @Before
    public final void setUp() {
        df = new DataFrame<Object>(
                Arrays.<Object>asList("row1", "row2", "row3"),
                Arrays.<Object>asList("category", "name", "value"),
                Arrays.<List<Object>>asList(
                        Arrays.<Object>asList("test", "test", "test", "beta", "beta", "beta"),
                        Arrays.<Object>asList("one", "two", "three", "one", "two", "three" ),
                        Arrays.<Object>asList(10, 20, 30, 40, 50, 60)
                    )
            );
    }

    @Test
    public final void testDuplicateColumnsInConstructor() {
        // 注：原测试 expected=IllegalArgumentException，把「重名列抛异常」当契约 —— 这是 bug。
        // joinery 1.x 的 Index 强制列名唯一，导致 Excel 重名列、join 同名列都崩溃。
        // 2026-08 修复后 Index 支持 pandas 风格的重名列：构造器不再抛异常，重名列原样保留。
        // 详见 joinery.impl.Index 的重名支持。
        new DataFrame<>(Arrays.<Object>asList("test", "test"));
    }

    @Test
    public final void testConstructorWithRowsAndData() {
        assertArrayEquals(
                "column names are correct",
                new Object[] { "category", "name", "value" },
                df.columns().toArray()
            );
        assertArrayEquals(
                "row names are correct",
                new Object[] { "row1", "row2", "row3", 3, 4, 5 },
                df.index().toArray()
            );
        assertArrayEquals(
                "data is correct",
                new Object[] { "one", "two", "three", "one", "two", "three" },
                df.col(1).toArray()
            );
    }

    @Test
    public final void testColByIndex() {
        final List<Object> col = df.col(2);
        assertArrayEquals(
                "data is correct",
                new Object[] { 10, 20, 30, 40, 50, 60 },
                col.toArray()
            );
    }

    @Test
    public final void testColByName() {
        final List<Object> col = df.col("value");
        assertArrayEquals(
                "data is correct",
                new Object[] { 10, 20, 30, 40, 50, 60 },
                col.toArray()
            );
    }

    @Test
    public final void testRowByIndex() {
        final List<Object> row = df.row(1);
        assertArrayEquals(
                "data is correct",
                new Object[] { "test", "two", 20 },
                row.toArray()
            );
    }

    @Test
    public final void testRowByName() {
        final List<Object> row = df.row("row2");
        assertArrayEquals(
                "data is correct",
                new Object[] { "test", "two", 20 },
                row.toArray()
            );
    }

    @Test
    public final void testRowByGeneratedName() {
        final List<Object> row = df.row(5);
        assertArrayEquals(
                "data is correct",
                new Object[] { "beta", "three", 60 },
                row.toArray()
            );
    }

    @Test
    public final void testReindexInt() {
        assertArrayEquals(
                "index is correct",
                new Object[] { 10, 20, 30, 40, 50, 60 },
                df.reindex(2).index().toArray()
            );
    }

    @Test
    public final void testReindexIntMultiple() {
        assertArrayEquals(
                "index is correct",
                new Object[] {
                    Arrays.asList("one", 10),
                    Arrays.asList("two", 20),
                    Arrays.asList("three", 30),
                    Arrays.asList("one", 40),
                    Arrays.asList("two", 50),
                    Arrays.asList("three", 60)
                },
                df.reindex(1, 2).index().toArray()
            );
    }

    @Test
    public final void testReindexIntDuplicates() {
        // 注：原测试 expected=IllegalArgumentException，把「reindex 后行 index 有重复值」当错误。
        // 这是 bug —— 实际数据中 index 列有重复值完全合法（如时间序列的多条同日期记录、
        // 分组键的多条同组记录）。2026-08 修复后 Index 支持重名，reindex(0) 应成功。
        // 详见 joinery.impl.Index 的重名支持。
        df.reindex(0);
    }

    @Test
    public final void testReindexString() {
        assertArrayEquals(
                "index is correct",
                new Object[] { 10, 20, 30, 40, 50, 60 },
                df.reindex("value").index().toArray()
            );
    }

    @Test
    public final void testReindexStringMultiple() {
        assertArrayEquals(
                "index is correct",
                new Object[] {
                    Arrays.asList("one", 10),
                    Arrays.asList("two", 20),
                    Arrays.asList("three", 30),
                    Arrays.asList("one", 40),
                    Arrays.asList("two", 50),
                    Arrays.asList("three", 60)
                },
                df.reindex("name", "value").index().toArray()
            );
    }

    @Test
    public final void testReindexStringDuplicates() {
        // 注：原测试 expected=IllegalArgumentException，把「reindex 后行 index 有重复值」当错误。
        // 这是 bug —— 与 testReindexIntDuplicates 同理，category 列含重复值（"test"/"beta"）完全合法。
        // 2026-08 修复后 Index 支持重名，reindex("category") 应成功。
        df.reindex("category");
    }
}
