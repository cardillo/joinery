package examples;

import java.util.Arrays;
import java.util.List;

import joinery.DataFrame;

public class Random {
    public static void main(final String[] args){
        DataFrame<Object> df1 = new DataFrame<>("0", "1", "2");
        df1.append("0", Arrays.asList(10, 20, 30));
        df1.append("1", Arrays.asList(40, 50, 60));
        System.out.println(df1);

        DataFrame<Object> df2 = new DataFrame<>("3", "4", "5");
        df2.append("0", Arrays.asList(40, 50, 60));
        df2.append("1",Arrays.asList(70, 80, 90));
        System.out.println(df2);

        // If we want to concat two dataframes, we can use the append function and add rows one by one

        for(List<Object> row: df2){
            df1.append(row);
        }

        System.out.println(df1);
    }
}
