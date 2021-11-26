package examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joinery.DataFrame;

public class Random_Export {
    public static void main(final String[] args) {
        DataFrame<Object> df1 = new DataFrame<>("ID", "Name", "Age");
        df1.append(11,Arrays.asList(1, "A", 10));
        df1.append(22,Arrays.asList(2, "B", 20));
        df1.append(33,Arrays.asList(3, "C", 30));
        df1.append(44,Arrays.asList(4, "D", 40));
        String filename1 = "df1_origin.csv";

        // System.out.println(df1.types());


        try{
            df1.writeCsv(filename1, true);
            // df1.add("labels", df1.index()).writeCsv(filename2);
        }
        catch (Exception e){
            e.printStackTrace();
        }





    }
}
