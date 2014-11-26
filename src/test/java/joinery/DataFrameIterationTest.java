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
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class DataFrameIterationTest {
    private DataFrame<Object> empty;
    private DataFrame<Object> df;

    @Before
    public void setUp() {
        empty = new DataFrame<>(Arrays.<String>asList());
        df = new DataFrame<>(
                Arrays.<String>asList(),
                Arrays.<String>asList("name", "value"),
                Arrays.asList(
                    Arrays.<Object>asList("alpha", "beta", "alpha", "beta"),
                    Arrays.<Object>asList("1", "2", "3", "4")
                )
            );
    }

    @Test
    public void testIter() {
        int i = 0;
        for (final List<Object> row : df) {
            assertEquals(i % 2 == 0 ? "alpha" : "beta", row.get(0));
            assertEquals(String.valueOf(i + 1), row.get(1));
            i++;
        }
        assertEquals(i, df.length());
    }

    @Test
    public void testItersEmpty() {
        int i = 0;
        for (final List<Object> row : empty) {
            fail(String.format("found row: %s!!!", row));
            i++;
        }
        assertEquals(i, empty.length());
    }

    @Test
    public void testIterRows() {
        int i = 0;
        final ListIterator<List<Object>> it = df.iterrows();
        while (it.hasNext()) {
            final List<Object> row = it.next();
            assertEquals(i % 2 == 0 ? "alpha" : "beta", row.get(0));
            assertEquals(String.valueOf(i + 1), row.get(1));
            i++;
        }
        assertEquals(df.length() , i);
        while (it.hasPrevious()) {
            i--;
            final List<Object> row = it.previous();
            assertEquals(i % 2 == 0 ? "alpha" : "beta", row.get(0));
            assertEquals(String.valueOf(i + 1), row.get(1));
        }
        assertEquals(0, i);
    }

    @Test
    public void testIterMap() {
        int i = 0;
        final ListIterator<Map<String, Object>> it = df.itermap();
        while (it.hasNext()) {
            final Map<String, Object> row = it.next();
            assertEquals(i % 2 == 0 ? "alpha" : "beta", row.get("name"));
            assertEquals(String.valueOf(i + 1), row.get("value"));
            i++;
        }
        assertEquals(df.length() , i);
        while (it.hasPrevious()) {
            i--;
            final Map<String, Object> row = it.previous();
            assertEquals(i % 2 == 0 ? "alpha" : "beta", row.get("name"));
            assertEquals(String.valueOf(i + 1), row.get("value"));
        }
        assertEquals(0, i);
    }

    @Test
    public void testIterCols() {
        int i = 0;
        final Iterator<List<Object>> it = df.itercols();
        while (it.hasNext()) {
            final List<Object> col = it.next();
            assertEquals(4, col.size());
            i++;
        }
        assertEquals(i, df.size());
    }

    @Test
    public void testIterValues() {
        int i = 0;
        final ListIterator<Object> it = df.itervalues();
        while (it.hasNext()) {
            it.next();
            i++;
        }
        assertEquals(df.size() * df.length(), i);
        while (it.hasPrevious()) {
            i--;
            it.previous();
        }
        assertEquals(0, i);
    }

    @Test
    public void testTranspose() {
        final DataFrame<Integer> df = new DataFrame<>(
                Arrays.asList(
                        Arrays.<Integer>asList(1, 2),
                        Arrays.<Integer>asList(3, 4)
                    )
            );
        assertArrayEquals(
                new Object[] { 1, 2 },
                df.transpose().row(0).toArray()
            );
    }
}
