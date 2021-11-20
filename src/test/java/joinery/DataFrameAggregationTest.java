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

import org.apache.commons.math3.exception.MathIllegalArgumentException;
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
    public void testMode1() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Name", "DOB", "Age");
        DF.append("row1", Arrays.asList("Cody", 1024, 23));
        DF.append("row2", Arrays.asList("Elena", 826, 26));
        DF.append("row3", Arrays.asList("Cody", 1024, 18));

        assertArrayEquals(
                new Object[] {"Cody", 1024, null},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode2() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Name", "DOB", "Age");
        DF.append("row1", Arrays.asList("Cody", 1024, 23));
        DF.append("row2", Arrays.asList("Elena", 826, 26));
        DF.append("row3", Arrays.asList("Emma", 917, 18));

        assertArrayEquals(
                new Object[] {null, null, null},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode3() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Name", "DOB", "Age");
        DF.append("row1", Arrays.asList("Cody", 1024, 23));
        DF.append("row2", Arrays.asList("Elena", 826, 26));
        DF.append("row3", Arrays.asList("Emma", 917, 18));
        DF.append("row4", Arrays.asList("John", 1112, 22));
        DF.append("row5", Arrays.asList("Elena", 917, 23));
        DF.append("row6", Arrays.asList("Emma", 922, 18));

        assertArrayEquals(
                new Object[] {"Emma", 917, 18},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode4() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Value");
        DF.append("row1", Arrays.asList(10));

        assertArrayEquals(
                new Object[] {null},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode5() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Value");
        DF.append("row1", Arrays.asList(10));
        DF.append("row2", Arrays.asList(10));
        DF.append("row3", Arrays.asList(20));

        assertArrayEquals(
                new Object[] {10},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode6() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Value");
        DF.append("row1", Arrays.asList(10));
        DF.append("row2", Arrays.asList(10));
        DF.append("row3", Arrays.asList(20));
        DF.append("row4", Arrays.asList(20));

        assertArrayEquals(
                new Object[] {20},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode7() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Value");
        DF.append("row1", Arrays.asList(10));
        DF.append("row2", Arrays.asList(10));
        DF.append("row3", Arrays.asList(20));
        DF.append("row4", Arrays.asList(20));
        DF.append("row5", Arrays.asList(30));
        DF.append("row6", Arrays.asList(30));

        assertArrayEquals(
                new Object[] {20},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode8() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Value");
        DF.append("row1", Arrays.asList(10));
        DF.append("row2", Arrays.asList(20));
        DF.append("row3", Arrays.asList(30));
        DF.append("row4", Arrays.asList(40));
        DF.append("row5", Arrays.asList(50));
        DF.append("row6", Arrays.asList(60));

        assertArrayEquals(
                new Object[] {null},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode9() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Value");
        DF.append("row1", Arrays.asList(10));
        DF.append("row2", Arrays.asList(20));
        DF.append("row3", Arrays.asList(30));
        DF.append("row4", Arrays.asList(30));
        DF.append("row5", Arrays.asList(50));
        DF.append("row6", Arrays.asList(60));

        assertArrayEquals(
                new Object[] {30},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode10() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Name");
        DF.append("row1", Arrays.asList("Cody"));
        DF.append("row2", Arrays.asList("Elena"));
        DF.append("row3", Arrays.asList("Emma"));
        DF.append("row4", Arrays.asList("John"));
        DF.append("row5", Arrays.asList("ELENA"));
        DF.append("row6", Arrays.asList("Emma"));

        assertArrayEquals(
                new Object[] {"Emma"},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode11() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Name", "Age");
        DF.append("row1", Arrays.asList("Cody", 22));
        DF.append("row2", Arrays.asList("Elena", 23));
        DF.append("row3", Arrays.asList("Emma", 24));
        DF.append("row4", Arrays.asList("John", 16));
        DF.append("row5", Arrays.asList("ELENA", 25));
        DF.append("row6", Arrays.asList("Emma", 26));

        assertArrayEquals(
                new Object[] {"Emma", null},
                DF.mode().toArray()
        );
    }

    @Test
    public void testMode12() {
        DataFrame<Object> DF = new DataFrame<Object>();
        DF.add("Name", "Age");
        DF.append("row1", Arrays.asList("Cody", 22));
        DF.append("row2", Arrays.asList("Elena", 23));
        DF.append("row3", Arrays.asList("Emma", 22));
        DF.append("row4", Arrays.asList("John", 14));
        DF.append("row5", Arrays.asList("ELENA", 25));
        DF.append("row6", Arrays.asList("Emily", 25));

        assertArrayEquals(
                new Object[] {null, 22},
                DF.mode().toArray()
        );
    }

    @Test
    public void testSum() {
        assertArrayEquals(
                new Double[] { 280.0, 280.0 },
                df.sum().toArray()
            );
    }

    @Test
    public void testMean() {
        assertArrayEquals(
                new Double[] { 40.0, 40.0 },
                df.mean().toArray()
            );
    }

    @Test
    public void testStd() {
        assertArrayEquals(
                new double[] { 21.6024, 21.6024 },
                df.stddev().toArray(double[].class),
                0.0001
            );
    }

    @Test
    public void testVar() {
        assertArrayEquals(
                new double[] { 466.6666, 466.6666 },
                df.var().toArray(double[].class),
                0.0001
            );
    }

    @Test
    public void testSkew() {
        assertArrayEquals(
                new Double[] { 0.0, 0.0 },
                df.skew().toArray()
            );
    }

    @Test
    public void testKurt() {
        assertArrayEquals(
                new Double[] { -1.2, -1.2 },
                df.kurt().toArray()
            );
    }

    @Test
    public void testMin() {
        assertArrayEquals(
                new Double[] { 10.0, 10.0 },
                df.min().toArray()
            );
    }

    @Test
    public void testMax() {
        assertArrayEquals(
                new Double[] { 70.0, 70.0 },
                df.max().toArray()
            );
    }

    @Test
    public void testMedian() {
        assertArrayEquals(
                new Double[] { 40.0, 40.0 },
                df.median().toArray()
            );
    }

    @Test
    public void testCumsum() {
        assertArrayEquals(
                new Double[] {
                    10.0, 30.0, 60.0, 100.0, 150.0, 210.0, 280.0,
                    10.0, 30.0, 60.0, 100.0, 150.0, 210.0, 280.0
                },
                df.cumsum().toArray()
            );
    }

    @Test
    public void testCumsumGrouped() {
        assertArrayEquals(
                new Object[] {
                    "one", "one", "two", "two", "three", "three", "three",
                    10.0, 30.0, 30.0, 70.0, 50.0, 110.0, 180.0,
                    10.0, 30.0, 30.0, 70.0, 50.0, 110.0, 180.0
                },
                df.groupBy("b").cumsum().toArray()
            );
    }

    @Test
    public void testCumprod() {
        assertArrayEquals(
                new Double[] {
                   10.0, 200.0, 6000.0, 240000.0, 12000000.0, 720000000.0, 50400000000.0,
                   10.0, 200.0, 6000.0, 240000.0, 12000000.0, 720000000.0, 50400000000.0
                },
                df.cumprod().toArray()
            );
    }

    @Test
    public void testCummin() {
        df.set(4, 2, 1);
        assertArrayEquals(
                new Double[] {
                   10.0, 10.0, 10.0, 10.0, 1.0, 1.0, 1.0,
                   10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0
                },
                df.cummin().toArray()
            );
    }

    @Test
    public void testCummax() {
        df.set(4, 2, 100);
        assertArrayEquals(
                new Double[] {
                   10.0, 20.0, 30.0, 40.0, 100.0, 100.0, 100.0,
                   10.0, 20.0, 30.0, 40.0,  50.0,  60.0,  70.0
                },
                df.cummax().toArray()
            );
    }

    @Test
    public void testPercentile() {
        assertArrayEquals(
                new Double[] { 60.0, 60.0 },
                df.percentile(75).toArray()
            );
    }

    @Test(expected=MathIllegalArgumentException.class)
    public void testPercentileInvalid() {
        df.percentile(101);
    }

    @Test
    public void testDescribe() {
        assertArrayEquals(
                new double[] {
                        7.0, 40.0, 21.6024, 466.6667, 70.0, 10.0,
                        7.0, 40.0, 21.6024, 466.6667, 70.0, 10.0
                    },
                df.describe().toArray(double[].class),
                0.0001
            );
    }

    @Test
    public void testDescribeGrouped() {
        assertArrayEquals(
                new double[] {
                        2.00000000, 15.00000000, 7.07106781, 50.00000000, 20.00000000, 10.00000000,
                        2.00000000, 35.00000000, 7.07106781, 50.00000000, 40.00000000, 30.00000000,
                        3.00000000, 60.00000000, 10.00000000, 100.00000000, 70.00000000, 50.00000000,
                        2.00000000, 15.00000000, 7.07106781, 50.00000000, 20.00000000, 10.00000000,
                        2.00000000, 35.00000000, 7.07106781, 50.00000000, 40.00000000, 30.00000000,
                        3.00000000, 60.00000000, 10.00000000, 100.00000000, 70.00000000, 50.00000000
                    },
                df.groupBy("b").describe().toArray(double[].class),
                0.0001
            );
    }

    @Test
    public void testCov() {
        assertArrayEquals(
                new double[] { 466.66667, 466.66667, 466.66667, 466.66667 },
                df.cov().toArray(double[].class),
                0.0001
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
}
