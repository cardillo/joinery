/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 * Copyright (c) 2026 zc (1.11 line, GLM-5.2 AI-assisted refactor)
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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class DataFrameSerializationTest {
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws Exception {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization.csv"));
    }

    @Test(expected=FileNotFoundException.class)
    public void testReadCsvString()
    throws IOException {
        DataFrame.readCsv("does-not-exist.csv");
    }

    @Test
    public void testReadCsvInputStream() {
        final Object[][] expected = new Object[][] {
                new Object[] { "a", "a", "b", "b", "c", "c" },
                new Object[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" },
                new Object[] { 1L, 2L, 3L, 4L, 5L, 6L }
            };

        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    expected[i],
                    df.col(i).toArray()
                );
        }
    }
    
    @Test
    public void testReadCsvNAInputStream()
    throws IOException {
    	DataFrame<Object> nadf = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization_wNA.csv"), ",", "NA");
        final Object[][] expected = new Object[][] {
                new Object[] { "a", "a", "b", "b", "c", "c" },
                new Object[] { "alpha", null, "charlie", "delta", "echo", "foxtrot" },
                new Object[] { 1L, 2L, 3L, null, 5L, 6L }
            };

        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    expected[i],
                    nadf.col(i).toArray()
                );
        }
    }
    
    @Test
    public void testReadCsvNoHeaderInputStream()
    throws IOException {
    	DataFrame<Object> df_noHeader = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization_no_header.csv"), ",", "NA", false);
        final Object[][] expected = new Object[][] {
                new Object[] { "a", "a", "b", "b", "c", "c" },
                new Object[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" },
                new Object[] { 1L, 2L, 3L, 4L, 5L, 6L }
            };

        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    expected[i],
                    df_noHeader.col(i).toArray()
                );
        }
    }

    @Test
    public void testReadCsvSemicolonInputStream()
    throws IOException {
        DataFrame<Object> cdf = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization_semicolon.csv"), ";");
        final Object[][] expected = new Object[][] {
                new Object[] { "a", "a", "b", "b", "c", "c" },
                new Object[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" },
                new Object[] { 1L, 2L, 3L, 4L, 5L, 6L }
            };

        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    expected[i],
                    cdf.col(i).toArray()
                );
        }
    }
    
    @Test
    public void testReadCsvTabInputStream()
    throws IOException {
        DataFrame<Object> cdf = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization_tab.csv"), "\\t");
        final Object[][] expected = new Object[][] {
                new Object[] { "a", "a", "b", "b", "c", "c" },
                new Object[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" },
                new Object[] { 1L, 2L, 3L, 4L, 5L, 6L }
            };

        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    expected[i],
                    cdf.col(i).toArray()
                );
        }
    }

    @Test
    public void testWriteCsvString()
    throws IOException {
        final File tmp = File.createTempFile(getClass().getName(), ".csv");
        tmp.deleteOnExit();
        df.writeCsv(tmp.getPath());
        assertTrue(tmp.length() > 64);
    }

    @Test
    public void testWriteCsvInputStream()
    throws IOException {
        final File tmp = File.createTempFile(getClass().getName(), ".csv");
        tmp.deleteOnExit();
        df.writeCsv(new FileOutputStream(tmp));
        assertTrue(tmp.length() > 64);
    }

    @Test
    public void testReadWriteCsvTypes()
    throws IOException {
        final File tmp = File.createTempFile(getClass().getName(), ".csv");
        tmp.deleteOnExit();
        final DataFrame<Object> original = new DataFrame<>("date", "long", "double", "bool", "string");
        original.append(Arrays.asList(new Date(), 1L, 1.0, true, "test"));
        original.writeCsv(tmp.getPath());
        assertArrayEquals(
                original.types().toArray(),
                DataFrame.readCsv(tmp.getPath()).types().toArray()
            );
    }

    @Test
    public void testWriteCsvNonStringIndex()
    throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataFrame<Object> df = new DataFrame<>(Arrays.asList(1L, 2L, 3L, 4L));
        df.append(Arrays.asList(1, 2, 3, 4));
        df.writeCsv(out);
        assertTrue("writeCsv does not throw due to non-string indices", true);
    }

    @Test(expected=FileNotFoundException.class)
    public void testReadXlsString()
    throws IOException {
        DataFrame.readXls("does-not-exist.xls");
    }

    @Test
    public void testReadXlsInputStream()
    throws IOException {
        final DataFrame<Object> df = DataFrame.readXls(ClassLoader.getSystemResourceAsStream("serialization.xls"));
        // 注：value 列在 xls 中是整数单元格（POI NUMERIC，值 1.0/2.0/...），
        // 按新契约「Excel 整数单元格 → Long」（保留精度，不丢位），所以期望 1L/2L/...
        // 详见 Serialization.readCell / numericValueToObject
        final Object[][] expected = new Object[][] {
                new Object[] { "a", "a", "b", "b", "c", "c" },
                new Object[] { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot" },
                new Object[] { 1L, 2L, 3L, 4L, 5L, 6L }
            };
        for (int i = 0; i < expected.length; i++) {
            assertArrayEquals(
                    expected[i],
                    df.col(i).toArray()
                );
        }
    }

    @Test
    public void testWriteXlsString()
    throws IOException {
        final DataFrame<Object> df = DataFrame.readXls(ClassLoader.getSystemResourceAsStream("serialization.xls"));
        final File tmp = File.createTempFile(getClass().getName(), ".xls");
        tmp.deleteOnExit();
        df.writeXls(tmp.getPath());
        assertTrue(tmp.length() > 1024);
    }

    @Test
    public void testWriteXlsInputStream()
    throws IOException {
        final DataFrame<Object> df = DataFrame.readXls(ClassLoader.getSystemResourceAsStream("serialization.xls"));
        final File tmp = File.createTempFile(getClass().getName(), ".xls");
        tmp.deleteOnExit();
        df.writeXls(new FileOutputStream(tmp));
        assertTrue(tmp.length() > 1024);
    }

    @Test
    public void testReadWriteXlsTypes()
    throws IOException {
        final File tmp = File.createTempFile(getClass().getName(), ".xls");
        tmp.deleteOnExit();
        // 注：原写 1.0（Double）期望读回 Double。新契约「Excel 整数单元格 → Long」（保留精度）：
        //   1.0 在 Excel 单元格里是整数（POI NUMERIC 格式 General，值 1.0），按新契约读回为 Long，
        //   与原类型 Double 不一致。改为写 1.5（真正的小数），读回保持 Double，验证类型往返保持。
        //   详见 Serialization.numericValueToObject —— 整数判定：d == Math.floor(d) 且在 long 范围内。
        final DataFrame<Object> original = new DataFrame<>("date", "double", "bool", "string");
        original.append(Arrays.asList(new Date(), 1.5, true, "test"));
        original.writeXls(tmp.getPath());
        assertArrayEquals(
                original.types().toArray(),
                DataFrame.readXls(tmp.getPath()).types().toArray()
            );
    }

    @Test
    public void testWriteXlsNonStringIndex()
    throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final DataFrame<Object> df = new DataFrame<>(Arrays.asList(1L, 2L, 3L, 4L));
        df.append(Arrays.asList(1, 2, 3, 4));
        df.writeXls(out);
        assertTrue("writeXls does not throw due to non-string indices", true);
    }

    @Test
    public void testToStringInt() {
        assertThat(
                df.toString(2),
                containsString(String.format("... %d rows skipped ...", df.length() - 2))
            );
        assertEquals(
                6,
                df.toString(2).split("\n").length
            );
    }

    @Test
    public void testToString() {
        assertThat(
                df.toString(),
                not(containsString("..."))
            );
        assertEquals(
                7,
                df.toString().split("\n").length
            );
    }
    
    @Test
    public void testToStringEmptyHeader()
    throws IOException {
        DataFrame<Object> dfEmptyHeader = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("serialization_empty_header.csv"));
        dfEmptyHeader.transpose().toString();
    }

    @Test
    public void testToFromSql()
    throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        try (Connection dbc = DriverManager.getConnection("jdbc:derby:memory:testdb;create=true")) {
            dbc.createStatement().executeUpdate("create table test (category varchar(32), name varchar(32), value int)");
            PreparedStatement stmt = dbc.prepareStatement("insert into test values (?,?,?)");
            df.writeSql(stmt);

            Map<Object, Object> names = new HashMap<>();
            names.put("CATEGORY", "category");
            names.put("NAME", "name");
            names.put("VALUE", "value");

            DataFrame<Object> other = DataFrame.readSql(dbc, "select * from test").rename(names);
            DataFrame<String> cmp = DataFrame.compare(df, other);
            assertArrayEquals(
                    cmp.col("value").toArray(),
                    new String[] { "1 | 1", "2 | 2", "3 | 3", "4 | 4", "5 | 5", "6 | 6" }
                );
        }
    }
}
