package joinery;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class DataFrameConversionTest {
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws Exception {
        df = new DataFrame<>(
                Arrays.<String>asList("row1", "row2", "row3", "row4", "row5", "row6"),
                Arrays.<String>asList("string", "long", "double", "date", "bool", "null"),
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
                Arrays.<String>asList("row1", "row2", "row3", "row4", "row5", "row6"),
                Arrays.<String>asList("string", "long", "double", "date"),
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
}
