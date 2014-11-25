package joinery;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

public class DataFrameSerializationTest {
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws Exception {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization.csv"));
    }

    @Test(expected=FileNotFoundException.class)
    public void testReadCsvString()
    throws IOException {
        DataFrame.readCsv("does-not-exist.csv");
    }

    @Test
    public void testReadCsvInputStream() {
        final Object[][] expected = new Object[][] {
                new Object[] { "a", "a", "b", "b", "c", "c" },
                new Object[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" },
                new Object[] { 1L, 2L, 3L, 4L, 5L, 6L }
            };

        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    expected[i],
                    df.col(i).toArray()
                );
        }
    }

    @Test
    public void testWriteCsvString()
    throws IOException {
        final File tmp = File.createTempFile(getClass().getName(), ".csv");
        tmp.deleteOnExit();
        df.writeCsv(tmp.getPath());
    }

    @Test
    public void testWriteCsvInputStream()
    throws IOException {
        final File tmp = File.createTempFile(getClass().getName(), ".csv");
        tmp.deleteOnExit();
        df.writeCsv(new FileOutputStream(tmp));
    }

    @Test
    public void testToStringInt() {
        assertThat(
                df.toString(2),
                containsString(String.format("... %d rows skipped ...", df.length() - 2))
            );
        assertEquals(
                6,
                df.toString(2).split("\n").length
            );
    }

    @Test
    public void testToString() {
        assertThat(
                df.toString(),
                not(containsString("..."))
            );
        assertEquals(
                7,
                df.toString().split("\n").length
            );
    }

}
