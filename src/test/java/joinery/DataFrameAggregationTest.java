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

import org.junit.Before;
import org.junit.Test;

public class DataFrameAggregationTest {
    DataFrame<Object> df;

    @Before
    public void setUp()
    throws Exception {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("grouping.csv"));
    }

    @Test
    public void testSum() {
        assertArrayEquals(
                new Double[] { 280.0, 280.0 },
                df.convert().sum().toArray()
            );
    }

    @Test
    public void testMean() {
        assertArrayEquals(
                new Double[] { 40.0, 40.0 },
                df.convert().mean().toArray()
            );
    }

    @Test
    public void testStd() {
        assertArrayEquals(
                new double[] { 21.6024, 21.6024 },
                (double[])df.convert().stddev().toArray(double[].class),
                0.0001
            );
    }

    @Test
    public void testVar() {
        assertArrayEquals(
                new double[] { 466.6666, 466.6666 },
                (double[])df.convert().var().toArray(double[].class),
                0.0001
            );
    }

    @Test
    public void testSkew() {
        assertArrayEquals(
                new Double[] { 0.0, 0.0 },
                df.convert().skew().toArray()
            );
    }

    @Test
    public void testKurt() {
        assertArrayEquals(
                new Double[] { -1.2, -1.2 },
                df.convert().kurt().toArray()
            );
    }

    @Test
    public void testMin() {
        assertArrayEquals(
                new Double[] { 10.0, 10.0 },
                df.convert().min().toArray()
            );
    }

    @Test
    public void testMax() {
        assertArrayEquals(
                new Double[] { 70.0, 70.0 },
                df.convert().max().toArray()
            );
    }

    @Test
    public void testMedian() {
        assertArrayEquals(
                new Double[] { 40.0, 40.0 },
                df.convert().median().toArray()
            );
    }

    @Test
    public void testStorelessStatisticWithNulls() {
        df.set(0, 2, null);
        df.set(1, 3, null);
        df.mean();
    }

    @Test
    public void testStatisticWithNulls() {
        df.set(0, 2, null);
        df.set(1, 3, null);
        df.median();
    }

    /*
    @Test
    public void testApplyVarargs() {
        assertArrayEquals(
                new Object[] { 100L, 400L, 900L, 1600L, 2500L, 3600L, 4900L },
                df.convert()
                  .apply(null, null, new Function<Object, Long>() {
                    @Override
                    public Long apply(final Object value) {
                        final long x = Long.class.cast(value).longValue();
                        return x * x;
                    }
                  }) .toArray()
            );
    }

    @Test
    public void testApply() {
        final Map<Integer, Function<Object, Long>> map =
            Collections.<Integer, Function<Object, Long>>singletonMap(
                2, new Function<Object, Long>() {
                    @Override
                    public Long apply(final Object value) {
                        final long x = Long.class.cast(value).longValue();
                        return x * x;
                    }
                  }
            );
        assertArrayEquals(
                new Object[] { 100L, 400L, 900L, 1600L, 2500L, 3600L, 4900L },
                df.convert()
                  .apply(map).toArray()
            );
    }
     */
}
