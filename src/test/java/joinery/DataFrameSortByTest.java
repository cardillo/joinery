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
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class DataFrameSortByTest {
    private DataFrame<Object> df;
    private List<Object> values;

    @Before
    public final void setUp() {
        df = new DataFrame<Object>();

        values = Arrays.<Object>asList(1, 2, 3, 4, 5, 6);
        Collections.shuffle(values);

        df.add("name", Arrays.<Object>asList("one", "two", "three", "four", "one", "two"));
        df.add("value", values);
    }

    @Test
    public final void testSortBy() {
        final DataFrame<Object> sorted = df.sortBy(1);
        assertArrayEquals(
                "original values are unsorted",
                values.toArray(),
                df.col(1).toArray()
            );
        assertArrayEquals(
                "values are sorted",
                new Object[] { 1, 2, 3, 4, 5, 6 },
                sorted.col(1).toArray()
            );
    }

    @Test
    public final void testSortByString() {
        final DataFrame<Object> sorted = df.sortBy("value");
        assertArrayEquals(
                "original values are unsorted",
                values.toArray(),
                df.col(1).toArray()
            );
        assertArrayEquals(
                "values are sorted",
                new Object[] { 1, 2, 3, 4, 5, 6 },
                sorted.col(1).toArray()
            );
    }

    @Test
    public final void testSortByDesc() {
        final DataFrame<Object> sorted = df.sortBy(-1);
        assertArrayEquals(
                "original values are unsorted",
                values.toArray(),
                df.col(1).toArray()
            );
        assertArrayEquals(
                "values are sorted",
                new Object[] { 6, 5, 4, 3, 2, 1 },
                sorted.col(1).toArray()
            );
    }

    @Test
    public final void testSortByStringDesc() {
        final DataFrame<Object> sorted = df.sortBy("-value");
        assertArrayEquals(
                "original values are unsorted",
                values.toArray(),
                df.col(1).toArray()
            );
        assertArrayEquals(
                "values are sorted",
                new Object[] { 6, 5, 4, 3, 2, 1 },
                sorted.col(1).toArray()
            );
    }

    @Test
    public final void testSortByIndex() {
        final DataFrame<Object> sorted = df.sortBy("name");
        System.out.println(sorted);
        System.out.println(sorted.sortIndex(1).index());
        assertArrayEquals(
                new Object[] { 0, 1, 2, 3, 4, 5 },
                sorted.sortIndex(1).index().toArray()
            );
    }

    @Test
    public final void testSortByStringIndex() {
        df = new DataFrame<>("name", "value");
        df.append("row1", Arrays.asList("charlie", 3));
        df.append("row2", Arrays.asList("alpha", 1));
        df.append("row3", Arrays.asList("bravo", 2));
        final DataFrame<Object> sorted = df.sortBy("name");
        System.out.println(sorted.sortIndex(1));
        assertArrayEquals(
                new Object[] { "row1", "row2", "row3" },
                sorted.sortIndex(1).index().toArray()
            );
    }

    @Test
    public final void testSortByIndexDecreasing() {
        final DataFrame<Object> sorted = df.sortBy("name");
        System.out.println(sorted.sortIndex(-1).index());
        assertArrayEquals(
                new Object[] { 5, 4, 3, 2, 1, 0 },
                sorted.sortIndex(-1).index().toArray()
            );
    }
}
