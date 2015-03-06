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

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class DataFrameTimeseriesTest {
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws IOException {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("timeseries.csv"));
    }

    @Test
    public void testOutOfWindowRowsAreNull() {
        assertArrayEquals(
                new Object[] { "Thu Feb 05 00:00:00 EST 2015", null },
                df.retain("Date", "Close")
                  .diff()
                  .row(0)
                  .toArray()
          );
        assertArrayEquals(
                new Object[] { "Wed Feb 11 00:00:00 EST 2015", null },
                df.retain("Date", "Close")
                  .diff(5)
                  .row(4)
                  .toArray()
          );
    }

    @Test
    public void testDiscreteDifference() {
        assertArrayEquals(
                new double[] {
                    Double.NaN, -1.01, 0.79, 2.30, 2.86,
                    1.58, 0.62, 0.75, 0.89, -0.27
                },
                toArray(df.retain("Close")
                          .cast(Number.class)
                          .diff()
                          .col("Close")),
                0.0001
            );
    }

    @Test
    public void testDiscreteDifferencePeriod() {
        assertArrayEquals(
                new double[] {
                    Double.NaN, Double.NaN, Double.NaN, 2.08, 5.95,
                    6.74, 5.06, 2.95, 2.26, 1.37
                },
                toArray(df.retain("Close")
                          .cast(Number.class)
                          .diff(3)
                          .col("Close")),
                0.0001
            );
    }

    @Test
    public void testPercentChange() {
        assertArrayEquals(
                new double[] {
                    Double.NaN, -0.0084, 0.0066, 0.0192, 0.0234,
                    0.01265, 0.0049, 0.0059, 0.0070, -0.0021
                },
                toArray(df.retain("Close")
                          .cast(Number.class)
                          .percentChange()
                          .col("Close")),
                0.0001
            );
    }

    @Test
    public void testPercentChangePeriod() {
        assertArrayEquals(
                new double[] {
                    Double.NaN, Double.NaN, Double.NaN, 0.0173, 0.0500,
                    0.0563, 0.0415, 0.0236, 0.0179, 0.0108
                },
                toArray(df.retain("Close")
                          .cast(Number.class)
                          .percentChange(3)
                          .col("Close")),
                0.0001
            );
    }

    private static double[] toArray(final List<Number> values) {
        final double[] a = new double[values.size()];
        for (int i = 0; i < a.length; i++) {
            final Number n = values.get(i);
            a[i] = n != null ? n.doubleValue() : Double.NaN;
        }
        return a;
    }
}
