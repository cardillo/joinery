package joinery;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Before;
import org.junit.Test;

public class DataFrameInspectionTest {
    private DataFrame<Object> df;

    @Before
    public void setUp() throws Exception {
       df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("inspection.csv"));
    }

    @Test
    public void testNumeric() {
        assertArrayEquals(
                new Object[] { 1L, 2L, 3L, 2L, 3L, 4L },
                df.numeric().toArray()
            );
    }

    @Test
    public void testNonNumeric() {
        assertArrayEquals(
                new Object[] { "alpha", "beta", "gamma" },
                df.nonnumeric().toArray()
            );
    }
}
