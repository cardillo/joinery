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

import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class DataFramePivotTest {
    final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws Exception {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("pivot.csv"));
    }

    @Test
    public void testBasicPivot()
    throws Exception {
        final DataFrame<Object> pivot = df.pivot("date", "category", "value1");
        assertArrayEquals(
                pivot.index().toArray(),
                new Object[] {
                    fmt.parse("2014-01-01").toString(),
                    fmt.parse("2014-01-02").toString(),
                    fmt.parse("2014-01-03").toString(),
                    fmt.parse("2014-01-04").toString(),
                    fmt.parse("2014-01-05").toString()
                }
            );
        assertArrayEquals(
                pivot.columns().toArray(),
                new Object[] { "alpha", "bravo", "charlie" }
            );
        assertArrayEquals(
                pivot.toArray(),
                new Object[] {
                    1L,  2L,  3L,  4L,  5L,
                    2L,  3L,  5L,  7L, 11L,
                    1L,  2L,  4L,  8L, 16L
                }
            );
    }

    @Test
    public void testMultipleValues()
    throws Exception {
        final DataFrame<Object> pivot = df.pivot("date", "category", "value1", "value2");
        assertArrayEquals(
                pivot.index().toArray(),
                new Object[] {
                    fmt.parse("2014-01-01").toString(),
                    fmt.parse("2014-01-02").toString(),
                    fmt.parse("2014-01-03").toString(),
                    fmt.parse("2014-01-04").toString(),
                    fmt.parse("2014-01-05").toString()
                }
            );
        assertArrayEquals(
                pivot.columns().toArray(),
                new Object[] {
                    Arrays.asList("value1", "alpha").toString(),
                    Arrays.asList("value2", "alpha").toString(),
                    Arrays.asList("value1", "bravo").toString(),
                    Arrays.asList("value2", "bravo").toString(),
                    Arrays.asList("value1", "charlie").toString(),
                    Arrays.asList("value2", "charlie").toString(),
                }
            );
        assertArrayEquals(
                pivot.toArray(),
                new Object[] {
                      1L,    2L,    3L,    4L,    5L,
                    101L,  102L,  103L,  104L,  105L,
                      2L,    3L,    5L,    7L,   11L,
                    200L,  300L,  500L,  700L, 1100L,
                      1L,    2L,    4L,    8L,   16L,
                    128L,  256L,  512L, 1024L, 2048L
                }
            );
    }
}
