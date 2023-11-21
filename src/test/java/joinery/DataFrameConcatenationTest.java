package joinery;


import org.junit.Test;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

import joinery.DataFrame;
import joinery.impl.Comparison;

import static org.junit.Assert.*;

// CS427 Issue link: https://github.com/cardillo/joinery/issues/83
// This entire class is for testing the concatenate function during the CS 427 Team Project
public class DataFrameConcatenationTest {
    private DataFrame<Object> df1, df2, df3, df4, df5;

    @Before
    public void setUp()
            throws IOException {
        df1 = new DataFrame<>("a", "b", "c");
        df1.append(Arrays.asList(1, 2, 3));
        df1.append(Arrays.asList(4, 5, 6));

        df2 = new DataFrame<>("d", "e", "f");
        df2.append(Arrays.asList(7, 8, 9));
        df2.append(Arrays.asList(10, 11, 12));

        df3 = new DataFrame<>("u", "v", "x", "y", "z");
        df3.append(Arrays.asList(1, 2, 3, 4, 5));
        df3.append(Arrays.asList(6, 7, 8, 9, 10));
        df3.append(Arrays.asList(11, 12, 13, 14, 15));

        df4 = new DataFrame<>("a", "b", "c");
        df4.append(Arrays.asList("A", "B", "C"));
        df4.append(Arrays.asList("D", "E", "F"));

        df5 = new DataFrame<>("h", "i", "j");
        df5.append(Arrays.asList(13, 14, 15));
        df5.append(Arrays.asList(16, 17, 18));
        df5.append(Arrays.asList(19, 20, 21));

    }

    @Test
    public void concat_test1() {
        assertArrayEquals(
                new Object[] {
                        1, 4, 7, 10, 2, 5, 8, 11, 3, 6, 9, 12
                },
                df1.concatenate(df2).toArray()
        );
    }

    @Test
    public void concat_test2() {
        assertArrayEquals(
                new Object[] {
                        1, 4, 7, 10, 2, 5, 8, 11, 3, 6, 9, 12
                },
                df1.concatenate(df2, 0).toArray()
        );
    }

    @Test
    public void concat_test3() {
        assertArrayEquals(
                new Object[] {
                        1, 4, 2, 5, 3, 6, 7, 10, 8, 11, 9, 12
                },
                df1.concatenate(df2, 1).toArray()
        );
    }

    @Test
    public void concat_test4() {
        assertArrayEquals(
                new Object[] {
                        1, 4, 2, 5, 3, 6
                },
                df1.concatenate(df2, 10).toArray()
        );
    }

    @Test
    public void concat_test5() {
        assertArrayEquals(
                new Object[] {
                        1, 4, 2, 5, 3, 6
                },
                df1.concatenate(df3, 0).toArray()
        );
    }

    @Test
    public void concat_test6() {
        assertArrayEquals(
                new Object[] {
                        1, 4, 2, 5, 3, 6
                },
                df1.concatenate(df4, 0).toArray()
        );
    }

    @Test
    public void concat_test7() {
        assertArrayEquals(
                new Object[] {
                        1, 4, 2, 5, 3, 6
                },
                df1.concatenate(df3, 1).toArray()
        );
    }

    @Test
    public void concat_test8() {
        assertArrayEquals(
                new Object[] {
                        7, 10, 1, 4, 8, 11, 2, 5, 9, 12, 3, 6
                },
                df2.concatenate(df1, 0).toArray()
        );
    }

    @Test
    public void concat_test9() {
        assertArrayEquals(
                new Object[] {
                        1, 4, 2, 5, 3, 6, "A", "D", "B", "E", "C", "F"
                },
                df1.concatenate(df4, 1).toArray()
        );
    }

    @Test
    public void concat_test10() {
        assertArrayEquals(
                new Object[] {
                        13, 16, 19, 1, 4, 14, 17, 20, 2, 5, 15, 18, 21, 3, 6
                },
                df5.concatenate(df1, 0).toArray()
        );
    }
}
