package examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joinery.DataFrame;

public class Random_Export {
    public static void main(final String[] args) {
        DataFrame<Object> df1 = new DataFrame<>("0", "1", "2");
        df1.append("a",Arrays.asList(1, 2, 3));
        df1.append("b",Arrays.asList(4, 5, 6));
        df1.append("c",Arrays.asList(7, 8, 9));
        String filename1 = "df1_origin.csv";
        String filename2 = "df1_rowname.csv";

        /*
        The output in the csv file is
                0,1,2
                1,2,3
                4,5,6
         */

        try{
            df1.writeCsv(filename1);
            df1.add("labels", df1.index()).writeCsv(filename2);
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
}
