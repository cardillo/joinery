package joinery.impl;

import joinery.DataFrame;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.charset.Charset;

public class SerializationTest extends TestCase {

    public void testReadCsv_gbk() throws IOException {
        DataFrame df= Serialization.readCsv("src/test/resources/csv_gbk.txt", Charset.forName("gbk"));
        assertEquals(df.get(1,1),"Pinduoduo");
        assertEquals(df.get(2,3),"China");
        assertFalse(df.get(1,1).equals("China"));
        assertFalse(df.get(2,3).equals("Pinduoduo"));
    }

}