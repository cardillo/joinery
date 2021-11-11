package examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joinery.DataFrame;

public class Random {
    public static void main(final String[] args){
        DataFrame<Object> df1 = new DataFrame<>("0", "1", "2");
        df1.append(Arrays.asList(1, 2, 3));
        df1.append(Arrays.asList(4, 5, 6));
        //System.out.println(df1);

        DataFrame<Object> df2 = new DataFrame<>("3", "4", "5");
        df2.append(Arrays.asList(10, 20, 30));
        df2.append(Arrays.asList(40, 50, 60));
        //System.out.println(df2);

        df1 = df1.concatenate(df2, 0);

        System.out.println(df1);

        DataFrame<Object> df3 = new DataFrame<>("a", "b", "c");
        df3.append(Arrays.asList(1, 2, 3));
        df3.append(Arrays.asList(4, 5, 6));

        DataFrame<Object> df4 = new DataFrame<>("x", "y", "z");
        df4.append(Arrays.asList(10, 20, 30));
        df4.append(Arrays.asList(40, 50, 60));

        df3 = df3.concatenate(df4, 1);
        System.out.println(df3);

    }
}
