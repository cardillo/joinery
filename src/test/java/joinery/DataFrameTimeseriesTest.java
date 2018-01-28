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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;

import joinery.DataFrame.Function;

import org.junit.Before;
import org.junit.Test;

public class DataFrameTimeseriesTest {
    private DateFormat fmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws IOException {
        Locale.setDefault(Locale.US);
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("timeseries.csv"));
    }

    @Test
    public void testOutOfWindowRowsAreNull()
    throws ParseException {
        assertArrayEquals(
                new Object[] { fmt.parse("Thu Feb 05 00:00:00 EST 2015"), null },
                df.retain("Date", "Close")
                  .diff()
                  .row(0)
                  .toArray()
          );
        assertArrayEquals(
                new Object[] { fmt.parse("Wed Feb 11 00:00:00 EST 2015"), null },
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
                df.retain("Close")
                  .cast(Number.class)
                  .diff()
                  .fillna(Double.NaN)
                  .toArray(double[].class),
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
                df.retain("Close")
                  .cast(Number.class)
                  .diff(3)
                  .fillna(Double.NaN)
                  .toArray(double[].class),
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
                df.retain("Close")
                  .cast(Number.class)
                  .percentChange()
                  .fillna(Double.NaN)
                  .toArray(double[].class),
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
                df.retain("Close")
                  .percentChange(3)
                  .fillna(Double.NaN)
                  .toArray(double[].class),
                0.0001
            );
    }

    @Test
    public void testRollApplyMultiColumn() {
        assertArrayEquals(
                new double[] {
                        Double.NaN, 119.435, 119.325, 120.870, 123.450,
                            125.670, 126.770, 127.455, 128.275, 128.585,
                        Double.NaN, 42976406.0, 41298182.0, 50449151.5, 67785151.5,
                            74018131.5, 64373342.5, 58712312.0, 54022071.0, 41066090.5
                    },
                df.numeric()
                  .retain("Close", "Volume")
                  .rollapply(new Function<List<Number>, Number>() {
                    @Override
                    public Number apply(final List<Number> values) {
                        if (values.contains(null))
                            return null;
                        return (values.get(0).doubleValue() + values.get(1).doubleValue()) / 2.0;
                    }
                  })
                  .fillna(Double.NaN)
                  .toArray(double[].class),
                0.0001
            );
    }
}
