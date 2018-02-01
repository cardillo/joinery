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

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class DataFrameBasicTest {
    private DataFrame<Object> df;

    @Before
    public final void setUp() {
        df = new LocalDataFrame<Object>(
                Arrays.<Object>asList("row1", "row2", "row3"),
                Arrays.<Object>asList("category", "name", "value"),
                Arrays.<List<Object>>asList(
                        Arrays.<Object>asList("test", "test", "test", "beta", "beta", "beta"),
                        Arrays.<Object>asList("one", "two", "three", "one", "two", "three" ),
                        Arrays.<Object>asList(10, 20, 30, 40, 50, 60)
                    )
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public final void testDuplicateColumnsInConstructor() {
        new LocalDataFrame<String>(Arrays.<Object>asList("test", "test"));
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

    @Test(expected=IllegalArgumentException.class)
    public final void testReindexIntDuplicates() {
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

    @Test(expected=IllegalArgumentException.class)
    public final void testReindexStringDuplicates() {
        df.reindex("category");
    }
}
