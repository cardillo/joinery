package joinery.impl;

import joinery.DataFrame;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DataFrameOfColTest {
    // issue #74, Use numOfCol() to get the col number of a col Name, the type obtained is int.
    //CS304 (manually written) https://github.com/cardillo/joinery/issues/74
    @Test
    public void testColNum(){
        DataFrame<Object> df = new DataFrame<>("name", "age", "value");
        df.append(Arrays.asList("one", 10, 1.0));
        df.append(Arrays.asList("two", 20, 2.3));
        df.append(Arrays.asList("three", 30, 3.9));
        df.append(Arrays.asList("four", 40, 4.5));
        System.out.println(df.columns());
        assertEquals(0, df.numOfCol("name"));
        assertEquals(1, df.numOfCol("age"));
        assertEquals(2, df.numOfCol("value"));
        //        System.out.println(df.row(1));
    }

    // issue #74, Use numOfCol() to get the col number of a col Name, the type obtained is int.
    //CS304 (manually written) https://github.com/cardillo/joinery/issues/74
    @Test
    public void test2ColNum(){
        DataFrame<Object> df = new DataFrame<>("name", "price", "onSale");
        df.append(Arrays.asList("bag1", 30, true));
        df.append(Arrays.asList("bag2", 25, false));
        df.append(Arrays.asList("bag3", 20, false));
        df.append(Arrays.asList("bag4", 15, true));
        System.out.println(df.columns());
        assertEquals(-1, df.numOfCol("pri"));
        assertEquals(-1, df.numOfCol("on_Sale"));
        //        System.out.println(df.row(1));
    }
}
