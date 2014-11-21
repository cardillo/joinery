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

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

public class DataFrameGroupByTest {
    private DataFrame<Object> df;

    @Before
    public final void setUp() {
        df = new DataFrame<Object>();
    }

    @Test
    public final void testGroupBy() {
        df.add("name", Arrays.<Object>asList("one", "two", "three", "four", "one", "two"));
        df.add("value", Arrays.<Object>asList(1, 2, 3, 4, 5, 6));
        DataFrame<Object> grouped = df.groupBy(0);
        assertEquals(
                "group by result has correct number of rows",
                4,
                grouped.length()
            );
        assertArrayEquals(
                "group by result has correct names",
                new Object[] { "one", "two", "three", "four" },
                grouped.rows().toArray()
            );
        assertArrayEquals(
                "group by result has correct values",
                new Object[] { 2, 2, 1, 1 },
                grouped.col(1).toArray()
            );
    }

    @Test
    public final void testGroupBySum() {
        df.add("name", Arrays.<Object>asList("one", "two", "three", "four", "one", "two"));
        df.add("value", Arrays.<Object>asList(1, 2, 3, 4, 5, 6));
        DataFrame<Object> grouped = df.groupBy(
                Collections.singleton(0),
                Collections.<Integer, DataFrame.Aggregator<Object>>singletonMap(1, new DataFrame.Sum<Object>())
            );
        assertEquals(
                "group by result has correct number of rows",
                4,
                grouped.length()
            );
        assertArrayEquals(
                "group by result has correct names",
                new Object[] { "one", "two", "three", "four" },
                grouped.rows().toArray()
            );
        assertArrayEquals(
                "group by result has correct values",
                new Object[] { 6.0, 8.0, 3.0, 4.0 },
                grouped.col(1).toArray()
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public final void testGroupByInvalid() {
        new DataFrame<String>()
            .add("name", Arrays.<String>asList("one", "two", "three", "four", "one", "two"))
            .add("value", Arrays.<String>asList("1", "2", "3", "4", "1", "6"))
            .groupBy(0);
    }

    @Test
    public final void testGroupByMultiple() {
        df.add("name", Arrays.<Object>asList("one", "two", "three", "four", "one", "two"));
        df.add("category", Arrays.<Object>asList("alpha", "beta", "alpha", "beta", "alpha", "beta"));
        df.add("value", Arrays.<Object>asList(1, 2, 3, 4, 5, 6));
        Object[][] expected = new Object[][] {
                new Object[] { "alpha", "one",   2 },
                new Object[] { "beta",  "two",   2 },
                new Object[] { "alpha", "three", 1 },
                new Object[] { "beta",  "four",  1 }
            };
        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    "group by result has correct values",
                    expected[i],
                    df.groupBy("category", "name").row(i).toArray()
                );
        }
    }
}
