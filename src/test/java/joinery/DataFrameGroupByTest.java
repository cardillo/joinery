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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import joinery.DataFrame.Aggregate;
import joinery.DataFrame.KeyFunction;

import org.junit.Before;
import org.junit.Test;

public class DataFrameGroupByTest {
    private DataFrame<Object> df;

    @Before
    public final void setUp()
    throws IOException {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("grouping.csv"));
    }

    @Test
    public final void testGroupBy() {
        final DataFrame<Object> df = new DataFrame<>();
        df.add("name", Arrays.<Object>asList("one", "two", "three", "four", "one", "two"));
        df.add("value", Arrays.<Object>asList(1, 2, 3, 4, 5, 6));
        final DataFrame<Object> grouped = df.groupBy(0).count();
        assertEquals(
                "group by result has correct number of rows",
                4,
                grouped.length()
            );
        assertArrayEquals(
                "group by result has correct names",
                new Object[] { "one", "two", "three", "four" },
                grouped.index().toArray()
            );
        assertArrayEquals(
                "group by result has correct values",
                new Object[] { 2, 2, 1, 1 },
                grouped.col(1).toArray()
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public final void testGroupByInvalid() {
        new DataFrame<String>()
            .add("name", Arrays.<String>asList("one", "two", "three", "four", "one", "two"))
            .add("value", Arrays.<String>asList("1", "2", "3", "4", "1", "6"))
            .groupBy(0)
            .sum();
    }

    @Test
    public final void testGroupByMultiple() {
        final DataFrame<Object> df = new DataFrame<>();
        df.add("name", Arrays.<Object>asList("one", "two", "three", "four", "one", "two"));
        df.add("category", Arrays.<Object>asList("alpha", "beta", "alpha", "beta", "alpha", "beta"));
        df.add("value", Arrays.<Object>asList(1, 2, 3, 4, 5, 6));
        final Object[][] expected = new Object[][] {
                new Object[] { 2, 2, 2 },
                new Object[] { 2, 2, 2 },
                new Object[] { 1, 1, 1 },
                new Object[] { 1, 1, 1 }
            };
        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    "group by result has correct values",
                    expected[i],
                    df.groupBy("category", "name").count().row(i).toArray()
                );
        }
    }

    @Test
    public void testGroups() {
        final Map<Object, DataFrame<Object>> groups =
                df.convert().groupBy("b").groups();

        assertArrayEquals(
                new Object[] {
                    "alpha", "bravo",
                    "one", "one",
                    10L, 20L,
                    10.0, 20.0
                },
                groups.get("one").toArray()
            );
        assertArrayEquals(
                new Object[] {
                    "charlie", "delta",
                    "two", "two",
                    30L, 40L,
                    30.0, 40.0
                },
                groups.get("two").toArray()
            );
        assertArrayEquals(
                new Object[] {
                    "echo", "foxtrot", "golf",
                    "three", "three", "three",
                    50L, 60L, 70L,
                    50.0, 60.0, 70.0
                },
                groups.get("three").toArray()
            );
    }

    @Test
    public void testGroupApply() {
        assertArrayEquals(
                new Object[] {
                    2, 2, 3,
                    2, 2, 3,
                    2, 2, 3,
                    2, 2, 3
                },
                df.groupBy("b").apply(new Aggregate<Object, Object>() {
                    @Override
                    public Integer apply(final List<Object> value) {
                        return value.size();
                    }
                }).toArray()
            );
    }

    @Test
    public void testKeyFunction() {
        assertArrayEquals(
                new Object[] {
                    30.0, 70.0, 180.0,
                    30.0, 70.0, 180.0
                },
                df.convert().groupBy(new KeyFunction<Object>() {
                    @Override
                    public Object apply(final List<Object> value) {
                        return value.get(1);
                    }
                }).sum().toArray()
            );
    }
}
