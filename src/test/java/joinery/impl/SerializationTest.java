package joinery.impl;

import joinery.DataFrame;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.charset.Charset;

public class SerializationTest extends TestCase {

    public void testReadCsv_utf_8() throws IOException {
       DataFrame df= Serialization.readCsv("src/test/resources/csv_utf_8.csv", Charset.forName("gbk"));
       assertEquals(df.get(1,1),"Pinduoduo");
    }
    public void testReadCsv_utf_8_1() throws IOException {
        DataFrame df= Serialization.readCsv("src/test/resources/csv_utf_8.csv", Charset.forName("utf-8"));
        assertEquals(df.get(2,3),"China");
    }

    public void testReadCsv_gbk() throws IOException {
        DataFrame df= Serialization.readCsv("src/test/resources/csv_ANSI.txt", Charset.forName("gbk"));
        assertEquals(df.get(1,1),"Pinduoduo");
    }
    public void testReadCsv_gbk_1() throws IOException {
        DataFrame df= Serialization.readCsv("src/test/resources/csv_ANSI.txt",Charset.forName("utf-8"));
        assertEquals(df.get(1,1),"Pinduoduo");
    }
}