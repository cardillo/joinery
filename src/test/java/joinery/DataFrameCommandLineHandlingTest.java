/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package joinery;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

public class DataFrameCommandLineHandlingTest {
    private DataFrame<Object> df;

    @Test
    public void testNoArgs() throws IOException {
    	String [] cmdLine = {"src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","High","Low","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );

    }

    @Test
    public void testDropNoCols() throws IOException {
    	String [] cmdLine = {"--drop_column=\"\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","High","Low","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testDropFirst() throws IOException {
    	String [] cmdLine = {"--drop_column=\"0\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Open","High","Low","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testDropLast() throws IOException {
    	String [] cmdLine = {"--drop_column=\"5\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","High","Low","Close"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testDropFirstLast() throws IOException {
    	String [] cmdLine = {"--drop_column=\"0,5\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Open","High","Low","Close"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }
    
    @Test
    public void testDropInBetween() throws IOException {
    	String [] cmdLine = {"--drop_column=\"2\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","Low","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testDropNoColName() throws IOException {
    	String [] cmdLine = {"--drop_name=\"\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","High","Low","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testDropFirstColName() throws IOException {
    	String [] cmdLine = {"--drop_name=\"Date\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Open","High","Low","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testDropLastColName() throws IOException {
    	String [] cmdLine = {"--drop_name=\"Volume\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","High","Low","Close"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testDropInBetweenColName() throws IOException {
    	String [] cmdLine = {"--drop_name=\"Low\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","High","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testDropColIndexAndName() throws IOException {
    	String [] cmdLine = {"--drop_column=\"2\"", "--drop_name=\"Low\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public void testDropSameColIndexAndName() throws IOException {
    	String [] cmdLine = {"--drop_column=\"3\"", "--drop_name=\"Low\"", "src/test/resources/timeseries.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"Date","Open","High","Low","Close","Volume"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }
    
    @Test
    public void testLongDefault() throws IOException {
    	String [] cmdLine = {"src/test/resources/serialization.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"category","name","value"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
        @SuppressWarnings("rawtypes")
		Class [] expectedClasses = {java.lang.String.class, java.lang.String.class, java.lang.Long.class};
        assertArrayEquals(
        		expectedClasses,
        		df.types().toArray()
            );
    }
    
    @Test
    public void testDoubleDefault() throws IOException {
    	String [] cmdLine = {"--dbl", "src/test/resources/serialization.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"category","name","value"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
        @SuppressWarnings("rawtypes")
		Class [] expectedClasses = {java.lang.String.class, java.lang.String.class, java.lang.Double.class};
        assertArrayEquals(
        		expectedClasses,
        		df.types().toArray()
            );
    }
    
    @Test
    public void testSemicolonSep() throws IOException {
    	String [] cmdLine = {"--sep=;", "src/test/resources/serialization_semicolon.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"category","name","value"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }

    @Test
    public void testTabSep() throws IOException {
    	String [] cmdLine = {"--sep=\"\\t\"", "src/test/resources/serialization_tab.csv"};
    	df = DataFrame.processCommandline(cmdLine);
    	String [] expectedCols = {"category","name","value"};
        assertArrayEquals(
        		expectedCols,
                df.columns().toArray()
            );
    }


    //String [] expectedCols = {"Date","Open","High","Low","Close","Volume"};


}
