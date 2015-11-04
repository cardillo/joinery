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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import joinery.impl.Conversion;

import org.junit.Before;
import org.junit.Test;

public class DataFrameConversionTest {
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws Exception {
        df = new DataFrame<>(
                Arrays.<Object>asList("row1", "row2", "row3", "row4", "row5", "row6"),
                Arrays.<Object>asList("string", "long", "double", "date", "bool", "null"),
                Arrays.<List<Object>>asList(
                        Arrays.<Object>asList("one", "two", "three", "four", "five", "six"),
                        Arrays.<Object>asList("1", "2", "3", "4", "5", "6"),
                        Arrays.<Object>asList("1.1", "2.2", "3.3", "4.4", "5.5", "6.6"),
                        Arrays.<Object>asList("2014-01-01", "2014-01-02", "2014-01-03", "2014-01-04", "2014-01-05", "2014-01-06"),
                        Arrays.<Object>asList("t", "true", "f", "false", "yes", "no"),
                        Arrays.<Object>asList(null, null, null, null, null, null)
                    )
            );
    }

    @Test
    public void testCast() {
        final DataFrame<String> strings = df.cast(String.class);
        assertArrayEquals(
                new String[] {
                        "one", "two", "three", "four", "five", "six",
                        "1", "2", "3", "4", "5", "6",
                        "1.1", "2.2", "3.3", "4.4", "5.5", "6.6",
                        "2014-01-01", "2014-01-02", "2014-01-03", "2014-01-04", "2014-01-05", "2014-01-06",
                        "t", "true", "f", "false", "yes", "no",
                        null, null, null, null, null, null
                },
                strings.toArray()
            );
    }

    @Test(expected=ClassCastException.class)
    public void testCastFails() {
        final DataFrame<Date> dates = df.cast(Date.class);
        @SuppressWarnings("unused")
        final Date dt = dates.get(0,  0);
    }

    @Test
    public void testConvert() {
        df.convert();
        assertEquals(
                String.class,
                df.get("row1", "string").getClass()
            );
        assertEquals(
                Long.class,
                df.get("row1", "long").getClass()
            );
        assertEquals(
                Double.class,
                df.get("row1", "double").getClass()
            );
        assertEquals(
                Date.class,
                df.get("row1", "date").getClass()
            );
        assertEquals(
                Boolean.class,
                df.get("row1", "bool").getClass()
            );
    }

    @Test(expected=ClassCastException.class)
    public void testConvertFails() {
        final DataFrame<String> bad = new DataFrame<>(
                Arrays.<Object>asList("row1", "row2", "row3", "row4", "row5", "row6"),
                Arrays.<Object>asList("string", "long", "double", "date"),
                Arrays.<List<String>>asList(
                        Arrays.<String>asList("one", "two", "three", "four", "five", "six"),
                        Arrays.<String>asList("1", "2", "3", "4", "5", "6"),
                        Arrays.<String>asList("1.1", "2.2", "3.3", "4.4", "5.5", "6.6"),
                        Arrays.<String>asList("2014-01-01", "2014-01-02", "2014-01-03", "2014-01-04", "2014-01-05", "2014-01-06")
                    )
            );
        bad.convert();
        @SuppressWarnings("unused")
        final String tmp = bad.get("row1", "long");
    }

    @Test
    public void testIsNull() {
        final DataFrame<Boolean> nulls = df.isnull();
        final Object[] expected = new Boolean[] {
            false, false, false, false, false, false,
            false, false, false, false, false, false,
            false, false, false, false, false, false,
            false, false, false, false, false, false,
            false, false, false, false, false, false,
            true,  true,  true,  true,  true,  true
        };
        assertArrayEquals(
                expected,
                nulls.toArray()
            );
    }

    @Test
    public void testNotNull() {
        final DataFrame<Boolean> nonnulls = df.notnull();
        final Object[] expected = new Boolean[] {
            true,  true,  true,  true,  true,  true,
            true,  true,  true,  true,  true,  true,
            true,  true,  true,  true,  true,  true,
            true,  true,  true,  true,  true,  true,
            true,  true,  true,  true,  true,  true,
            false, false, false, false, false, false
        };
        assertArrayEquals(
                expected,
                nonnulls.toArray()
            );
    }

    @Test
    public void testConvertColumns() {
        df.convert(null, Long.class, Number.class);
        assertEquals(
                String.class,
                df.get("row1", "string").getClass()
            );
        assertEquals(
                Long.class,
                df.get("row1", "long").getClass()
            );
        assertEquals(
                Double.class,
                df.get("row1", "double").getClass()
            );
        assertEquals(
                String.class,
                df.get("row1", "date").getClass()
            );
        assertEquals(
                String.class,
                df.get("row1", "bool").getClass()
            );
    }

    @Test
    public void testTwoDimensionalToArray()
    throws ParseException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        assertArrayEquals(
                df.convert().toArray(new Object[df.length()][df.size()]),
                new Object[][] {
                    new Object[] { "one",   1L, 1.1, sdf.parse("2014-01-01"), true,  null },
                    new Object[] { "two",   2L, 2.2, sdf.parse("2014-01-02"), true,  null },
                    new Object[] { "three", 3L, 3.3, sdf.parse("2014-01-03"), false, null },
                    new Object[] { "four",  4L, 4.4, sdf.parse("2014-01-04"), false, null },
                    new Object[] { "five",  5L, 5.5, sdf.parse("2014-01-05"), true,  null },
                    new Object[] { "six",   6L, 6.6, sdf.parse("2014-01-06"), false, null }
                }
            );
    }

    @Test
    public void testToArrayType()
    throws ParseException {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        assertArrayEquals(
                df.convert().toArray(Object[][].class),
                new Object[][] {
                    new Object[] { "one",   1L, 1.1, sdf.parse("2014-01-01"), true,  null },
                    new Object[] { "two",   2L, 2.2, sdf.parse("2014-01-02"), true,  null },
                    new Object[] { "three", 3L, 3.3, sdf.parse("2014-01-03"), false, null },
                    new Object[] { "four",  4L, 4.4, sdf.parse("2014-01-04"), false, null },
                    new Object[] { "five",  5L, 5.5, sdf.parse("2014-01-05"), true,  null },
                    new Object[] { "six",   6L, 6.6, sdf.parse("2014-01-06"), false, null }
                }
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public void testToArrayTypeInvalid() {
        df.toArray(Double[][].class);
    }

    @Test
    public void testToArrayPrimitiveType() {
        assertArrayEquals(
                df.convert().numeric().toArray(double[][].class),
                new double[][] {
                    new double[] { 1.0, 1.1 },
                    new double[] { 2.0, 2.2 },
                    new double[] { 3.0, 3.3 },
                    new double[] { 4.0, 4.4 },
                    new double[] { 5.0, 5.5 },
                    new double[] { 6.0, 6.6 }
                }
            );
    }

    @Test
    public void testToModelMatrixWithIntercept() throws IOException {
        DataFrame<Object> df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization.csv"));
        assertEquals(3, df.columns().size());
        //System.out.println(df);
        //System.out.println(df.types());
        DataFrame<Number> mm = Conversion.toModelMatrixDataFrame(df, null, true, null);
        //System.out.println(mm);
        // Intercept + {a,b,c}.size() + {alpha,bravo...}.size() + value
        int expectedColNos = 1+2+5+1;
        //System.out.println(mm.col(1));
        assertEquals(expectedColNos, mm.columns().size());
        // Intercept
        assertEquals(1.0, mm.get(0, 0));
        assertEquals(1.0, mm.get(1, 0));
        assertEquals(1.0, mm.get(2, 0));
        assertEquals(1.0, mm.get(3, 0));
        assertEquals(1.0, mm.get(4, 0));
        assertEquals(1.0, mm.get(5, 0));
        // First category dummy variable i.e "a"
        assertEquals(1.0, mm.get(0, 1));
        assertEquals(1.0, mm.get(1, 1));
        assertEquals(0.0, mm.get(2, 1));
        assertEquals(0.0, mm.get(3, 1));
        assertEquals(0.0, mm.get(4, 1));
        assertEquals(0.0, mm.get(5, 1));
        // Second category dummy variable i.e "b"
        assertEquals(0.0, mm.get(0, 2));
        assertEquals(0.0, mm.get(1, 2));
        assertEquals(1.0, mm.get(2, 2));
        assertEquals(1.0, mm.get(3, 2));
        assertEquals(0.0, mm.get(4, 2));
        assertEquals(0.0, mm.get(5, 2));
        // First name dummy variable i.e "alpha"
        assertEquals(1.0, mm.get(0, 3));
        assertEquals(0.0, mm.get(1, 3));
        assertEquals(0.0, mm.get(2, 3));
        assertEquals(0.0, mm.get(3, 3));
        assertEquals(0.0, mm.get(4, 3));
        assertEquals(0.0, mm.get(5, 3));
        // Second name dummy variable i.e "bravo"
        assertEquals(0.0, mm.get(0, 4));
        assertEquals(1.0, mm.get(1, 4));
        assertEquals(0.0, mm.get(2, 4));
        assertEquals(0.0, mm.get(3, 4));
        assertEquals(0.0, mm.get(4, 4));
        assertEquals(0.0, mm.get(5, 4));
        // Third name dummy variable i.e "charlie"
        assertEquals(0.0, mm.get(0, 5));
        assertEquals(0.0, mm.get(1, 5));
        assertEquals(1.0, mm.get(2, 5));
        assertEquals(0.0, mm.get(3, 5));
        assertEquals(0.0, mm.get(4, 5));
        assertEquals(0.0, mm.get(5, 5));
        // Forth name dummy variable i.e "delta"
        assertEquals(0.0, mm.get(0, 6));
        assertEquals(0.0, mm.get(1, 6));
        assertEquals(0.0, mm.get(2, 6));
        assertEquals(1.0, mm.get(3, 6));
        assertEquals(0.0, mm.get(4, 6));
        assertEquals(0.0, mm.get(5, 6));
        // Fifth name dummy variable i.e "echo"
        assertEquals(0.0, mm.get(0, 7));
        assertEquals(0.0, mm.get(1, 7));
        assertEquals(0.0, mm.get(2, 7));
        assertEquals(0.0, mm.get(3, 7));
        assertEquals(1.0, mm.get(4, 7));
        assertEquals(0.0, mm.get(5, 7));
        // Value column
        assertEquals(1L, mm.get(0, 8));
        assertEquals(2L, mm.get(1, 8));
        assertEquals(3L, mm.get(2, 8));
        assertEquals(4L, mm.get(3, 8));
        assertEquals(5L, mm.get(4, 8));
        assertEquals(6L, mm.get(5, 8));
    }

    @Test
    public void testToModelMatrix() throws IOException {
        DataFrame<Object> df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization.csv"));
        assertEquals(3, df.columns().size());
        //System.out.println(df);
        //System.out.println(df.types());
        DataFrame<Number> mm = Conversion.toModelMatrixDataFrame(df, null, false, null);
        //System.out.println(mm);
        // {a,b,c}.size() + {alpha,bravo...}.size() + value
        int expectedColNos = 2+5+1;
        assertEquals(expectedColNos, mm.columns().size());
        // First category dummy variable i.e "a"
        assertEquals(1.0, mm.get(0, 0));
        assertEquals(1.0, mm.get(1, 0));
        assertEquals(0.0, mm.get(2, 0));
        assertEquals(0.0, mm.get(3, 0));
        assertEquals(0.0, mm.get(4, 0));
        assertEquals(0.0, mm.get(5, 0));
        // Second category dummy variable i.e "b"
        assertEquals(0.0, mm.get(0, 1));
        assertEquals(0.0, mm.get(1, 1));
        assertEquals(1.0, mm.get(2, 1));
        assertEquals(1.0, mm.get(3, 1));
        assertEquals(0.0, mm.get(4, 1));
        assertEquals(0.0, mm.get(5, 1));
        // First name dummy variable i.e "alpha"
        assertEquals(1.0, mm.get(0, 2));
        assertEquals(0.0, mm.get(1, 2));
        assertEquals(0.0, mm.get(2, 2));
        assertEquals(0.0, mm.get(3, 2));
        assertEquals(0.0, mm.get(4, 2));
        assertEquals(0.0, mm.get(5, 2));
        // Second name dummy variable i.e "bravo"
        assertEquals(0.0, mm.get(0, 3));
        assertEquals(1.0, mm.get(1, 3));
        assertEquals(0.0, mm.get(2, 3));
        assertEquals(0.0, mm.get(3, 3));
        assertEquals(0.0, mm.get(4, 3));
        assertEquals(0.0, mm.get(5, 3));
        // Third name dummy variable i.e "charlie"
        assertEquals(0.0, mm.get(0, 4));
        assertEquals(0.0, mm.get(1, 4));
        assertEquals(1.0, mm.get(2, 4));
        assertEquals(0.0, mm.get(3, 4));
        assertEquals(0.0, mm.get(4, 4));
        assertEquals(0.0, mm.get(5, 4));
        // Forth name dummy variable i.e "delta"
        assertEquals(0.0, mm.get(0, 5));
        assertEquals(0.0, mm.get(1, 5));
        assertEquals(0.0, mm.get(2, 5));
        assertEquals(1.0, mm.get(3, 5));
        assertEquals(0.0, mm.get(4, 5));
        assertEquals(0.0, mm.get(5, 5));
        // Fifth name dummy variable i.e "echo"
        assertEquals(0.0, mm.get(0, 6));
        assertEquals(0.0, mm.get(1, 6));
        assertEquals(0.0, mm.get(2, 6));
        assertEquals(0.0, mm.get(3, 6));
        assertEquals(1.0, mm.get(4, 6));
        assertEquals(0.0, mm.get(5, 6));
        // Value column
        assertEquals(1L, mm.get(0, 7));
        assertEquals(2L, mm.get(1, 7));
        assertEquals(3L, mm.get(2, 7));
        assertEquals(4L, mm.get(3, 7));
        assertEquals(5L, mm.get(4, 7));
        assertEquals(6L, mm.get(5, 7));
    }
    
    @Test
    public void testToModelMatrixWithReferenceFactorOnAll() throws IOException {
        DataFrame<Object> df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization.csv"));
        assertEquals(3, df.columns().size());
        //System.out.println(df);
        //System.out.println(df.types());
        Map<String,String> references = new TreeMap<String,String>();
        references.put("category","a");
        references.put("name","bravo");
        
        DataFrame<Number> mm = Conversion.toModelMatrixDataFrame(df, null, false, references);
        //System.out.println(mm);
        // {a,b,c}.size() + {alpha,bravo...}.size() + value
        int expectedColNos = 2+5+1;
        assertEquals(expectedColNos, mm.columns().size());
        // First category dummy variable i.e "b"
        assertEquals(0.0, mm.get(0, 0));
        assertEquals(0.0, mm.get(1, 0));
        assertEquals(1.0, mm.get(2, 0));
        assertEquals(1.0, mm.get(3, 0));
        assertEquals(0.0, mm.get(4, 0));
        assertEquals(0.0, mm.get(5, 0));
        // Second category dummy variable i.e "c"
        assertEquals(0.0, mm.get(0, 1));
        assertEquals(0.0, mm.get(1, 1));
        assertEquals(0.0, mm.get(2, 1));
        assertEquals(0.0, mm.get(3, 1));
        assertEquals(1.0, mm.get(4, 1));
        assertEquals(1.0, mm.get(5, 1));
        // First name dummy variable i.e "alpha"
        assertEquals(1.0, mm.get(0, 2));
        assertEquals(0.0, mm.get(1, 2));
        assertEquals(0.0, mm.get(2, 2));
        assertEquals(0.0, mm.get(3, 2));
        assertEquals(0.0, mm.get(4, 2));
        assertEquals(0.0, mm.get(5, 2));
        // Second name dummy variable i.e "charlie" since we use bravo as reference
        assertEquals(0.0, mm.get(0, 3));
        assertEquals(0.0, mm.get(1, 3));
        assertEquals(1.0, mm.get(2, 3));
        assertEquals(0.0, mm.get(3, 3));
        assertEquals(0.0, mm.get(4, 3));
        assertEquals(0.0, mm.get(5, 3));
        // Third name dummy variable i.e "delta"
        assertEquals(0.0, mm.get(0, 4));
        assertEquals(0.0, mm.get(1, 4));
        assertEquals(0.0, mm.get(2, 4));
        assertEquals(1.0, mm.get(3, 4));
        assertEquals(0.0, mm.get(4, 4));
        assertEquals(0.0, mm.get(5, 4));
        // Forth name dummy variable i.e "echo"
        assertEquals(0.0, mm.get(0, 5));
        assertEquals(0.0, mm.get(1, 5));
        assertEquals(0.0, mm.get(2, 5));
        assertEquals(0.0, mm.get(3, 5));
        assertEquals(1.0, mm.get(4, 5));
        assertEquals(0.0, mm.get(5, 5));
        // Fifth name dummy variable i.e "foxtrot"
        assertEquals(0.0, mm.get(0, 6));
        assertEquals(0.0, mm.get(1, 6));
        assertEquals(0.0, mm.get(2, 6));
        assertEquals(0.0, mm.get(3, 6));
        assertEquals(0.0, mm.get(4, 6));
        assertEquals(1.0, mm.get(5, 6));
        // Value column
        assertEquals(1L, mm.get(0, 7));
        assertEquals(2L, mm.get(1, 7));
        assertEquals(3L, mm.get(2, 7));
        assertEquals(4L, mm.get(3, 7));
        assertEquals(5L, mm.get(4, 7));
        assertEquals(6L, mm.get(5, 7));
    }
    
    @Test
    public void testToModelMatrixWithReferenceFactorOnOne() throws IOException {
        DataFrame<Object> df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization.csv"));
        assertEquals(3, df.columns().size());
        //System.out.println(df);
        //System.out.println(df.types());
        Map<String,String> references = new TreeMap<String,String>();
        references.put("name","bravo");
        
        DataFrame<Number> mm = Conversion.toModelMatrixDataFrame(df, null, false, references);
        //System.out.println(mm);
        // {a,b,c}.size() + {alpha,bravo...}.size() + value
        int expectedColNos = 2+5+1;
        assertEquals(expectedColNos, mm.columns().size());
        // First category dummy variable i.e "a"
        assertEquals(1.0, mm.get(0, 0));
        assertEquals(1.0, mm.get(1, 0));
        assertEquals(0.0, mm.get(2, 0));
        assertEquals(0.0, mm.get(3, 0));
        assertEquals(0.0, mm.get(4, 0));
        assertEquals(0.0, mm.get(5, 0));
        // Second category dummy variable i.e "b"
        assertEquals(0.0, mm.get(0, 1));
        assertEquals(0.0, mm.get(1, 1));
        assertEquals(1.0, mm.get(2, 1));
        assertEquals(1.0, mm.get(3, 1));
        assertEquals(0.0, mm.get(4, 1));
        assertEquals(0.0, mm.get(5, 1));
        // First name dummy variable i.e "alpha"
        assertEquals(1.0, mm.get(0, 2));
        assertEquals(0.0, mm.get(1, 2));
        assertEquals(0.0, mm.get(2, 2));
        assertEquals(0.0, mm.get(3, 2));
        assertEquals(0.0, mm.get(4, 2));
        assertEquals(0.0, mm.get(5, 2));
        // Second name dummy variable i.e "charlie" since we use bravo as reference
        assertEquals(0.0, mm.get(0, 3));
        assertEquals(0.0, mm.get(1, 3));
        assertEquals(1.0, mm.get(2, 3));
        assertEquals(0.0, mm.get(3, 3));
        assertEquals(0.0, mm.get(4, 3));
        assertEquals(0.0, mm.get(5, 3));
        // Third name dummy variable i.e "delta"
        assertEquals(0.0, mm.get(0, 4));
        assertEquals(0.0, mm.get(1, 4));
        assertEquals(0.0, mm.get(2, 4));
        assertEquals(1.0, mm.get(3, 4));
        assertEquals(0.0, mm.get(4, 4));
        assertEquals(0.0, mm.get(5, 4));
        // Forth name dummy variable i.e "echo"
        assertEquals(0.0, mm.get(0, 5));
        assertEquals(0.0, mm.get(1, 5));
        assertEquals(0.0, mm.get(2, 5));
        assertEquals(0.0, mm.get(3, 5));
        assertEquals(1.0, mm.get(4, 5));
        assertEquals(0.0, mm.get(5, 5));
        // Fifth name dummy variable i.e "foxtrot"
        assertEquals(0.0, mm.get(0, 6));
        assertEquals(0.0, mm.get(1, 6));
        assertEquals(0.0, mm.get(2, 6));
        assertEquals(0.0, mm.get(3, 6));
        assertEquals(0.0, mm.get(4, 6));
        assertEquals(1.0, mm.get(5, 6));
        // Value column
        assertEquals(1L, mm.get(0, 7));
        assertEquals(2L, mm.get(1, 7));
        assertEquals(3L, mm.get(2, 7));
        assertEquals(4L, mm.get(3, 7));
        assertEquals(5L, mm.get(4, 7));
        assertEquals(6L, mm.get(5, 7));
    }
}
