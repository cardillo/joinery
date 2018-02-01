package joinery;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.List;

import joinery.DataFrame.Function;
import joinery.DataFrame.RowFunction;

import org.junit.Before;
import org.junit.Test;

public class DataFrameViewsTest {
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws Exception {
        df = new LocalDataFrame<Object>("one", "two", "three")
                .append(Arrays.asList("a", null, "c"))
                .append(Arrays.asList("aa", "bb", "cc"));
    }

    @Test
    public void testApply() {
        assertArrayEquals(
                new Object[] { 1, 2, 0, 2, 1, 2 },
                df.apply(new Function<Object, Integer>() {
                    @Override
                    public Integer apply(final Object value) {
                        return value == null ? 0 : value.toString().length();
                    }
                }).toArray()
            );
    }

    @Test
    public void testTransform() {
        assertArrayEquals(
                new Object[] {
                    "a", "a", "aa", "aa",
                    null, null, "bb", "bb",
                    "c", "c", "cc", "cc"
                },
                df.transform(new RowFunction<Object, Object>() {
                    @Override
                    public List<List<Object>> apply(final List<Object> values) {
                        return Arrays.asList(values, values);
                    }
                }).toArray()
            );
    }

    @Test
    public void testFillNa() {
        assertArrayEquals(
                new Object[] { "a", "aa", "b", "bb", "c", "cc" },
                df.fillna("b").toArray()
            );
    }
}
