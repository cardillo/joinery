package joinery;

import static org.junit.Assert.assertArrayEquals;

import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class DataFrameUnpivotTest {
	final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
    private DataFrame<Object> df;
	
    @Before
    public void setUp()
    throws Exception {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("unpivot.csv"));
    }

    @Test
    public void testUnpivot()
    throws Exception {
        final DataFrame<Object> unpivot = df.melt(
        		new Object[]{"a", "b"}, new Object[]{"c","d"}, 
        		"variable", "value");

        assertArrayEquals(
                unpivot.columns().toArray(),
                new Object[] { "a", "b", "variable", "value" }
            );
        assertArrayEquals(
                unpivot.toArray(),
                new Object[] {
                		"alpha", "alpha", "bravo", "bravo", 
                		"one", "one", "two", "two", 
                		"c", "d", "c", "d", 
                		10L, 10L, 20L, 20L
                }
            );
    }

}
