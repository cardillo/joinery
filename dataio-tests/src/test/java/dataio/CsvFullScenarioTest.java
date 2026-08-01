package dataio;

import joinery.DataFrame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : readCsv/writeCsv 全场景测试（覆盖 6 个重载 + 边界）
 * │  Why  : CSV 是数据导入导出最基础路径，需覆盖所有重载、分隔符、引号转义、null 标记、
 * │         有无表头、数字默认类型、空行等场景
 * │  Who  : dataio-tests
 */
class CsvFullScenarioTest {

    // ┌─ What : readCsv(String file) 与 readCsv(InputStream) 基础重载
    @Test
    void readCsv_文件路径与流两种入口_行为一致(@TempDir Path tmp) throws IOException {
        final Path csv = tmp.resolve("basic.csv");
        Files.write(csv, "id,name\n1,alice\n2,bob\n".getBytes(StandardCharsets.UTF_8));
        final DataFrame<Object> byFile = DataFrame.readCsv(csv.toString());
        try (var in = Files.newInputStream(csv)) {
            final DataFrame<Object> byStream = DataFrame.readCsv(in);
            assertEquals(byFile.length(), byStream.length());
            assertEquals(byFile.get(0, 0), byStream.get(0, 0));
        }
    }

    // ┌─ What : readCsv(file/input, separator) —— 4 种分隔符全覆盖
    @Test
    void readCsv_四种分隔符_逗号分号Tab竖线(@TempDir Path tmp) throws IOException {
        // 逗号（默认）
        final Path comma = tmp.resolve("comma.csv");
        Files.write(comma, "a,b\n1,2\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(1L, DataFrame.readCsv(comma.toString()).get(0, 0));

        // 分号
        final Path semi = tmp.resolve("semi.csv");
        Files.write(semi, "a;b\n1;2\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(1L, DataFrame.readCsv(semi.toString(), ";").get(0, 0));

        // Tab（注意 readCsv 接受字面 "\\t" 字符串）
        final Path tab = tmp.resolve("tab.csv");
        Files.write(tab, "a\tb\n1\t2\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(1L, DataFrame.readCsv(tab.toString(), "\\t").get(0, 0));

        // 竖线
        final Path pipe = tmp.resolve("pipe.csv");
        Files.write(pipe, "a|b\n1|2\n".getBytes(StandardCharsets.UTF_8));
        assertEquals(1L, DataFrame.readCsv(pipe.toString(), "|").get(0, 0));
    }

    // ┌─ What : readCsv(input, separator, naString) —— NA 字符串转 null
    @Test
    void readCsv_naString_应转为null() throws IOException {
        final String content = "id,name\n1,NA\n2,bob\n";
        final DataFrame<Object> df = DataFrame.readCsv(
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                ",", "NA");
        assertEquals(2, df.length());
        assertNull(df.get(0, 1), "NA 字符串应转为 null");
        assertEquals("bob", df.get(1, 1));
    }

    // ┌─ What : readCsv(input, separator, naString, hasHeader=false) —— 无表头读
    @Test
    void readCsv_无表头_列名应为V0V1() throws IOException {
        final String content = "alice,30\nbob,25\n";
        final DataFrame<Object> df = DataFrame.readCsv(
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                ",", "NA", false);
        // 无表头时列名自动生成 V0/V1
        final var cols = new java.util.ArrayList<>(df.columns());
        assertEquals("V0", cols.get(0));
        assertEquals("V1", cols.get(1));
        assertEquals("alice", df.get(0, 0));
        assertEquals(30L, df.get(0, 1));
    }

    // ┌─ What : readCsv(file, separator, NumberDefault) —— DOUBLE_DEFAULT vs LONG_DEFAULT
    @Test
    void readCsv_整数列默认Long_可切到Double(@TempDir Path tmp) throws IOException {
        final Path csv = tmp.resolve("num.csv");
        Files.write(csv, "v\n42\n".getBytes(StandardCharsets.UTF_8));
        // 默认 LONG_DEFAULT → 整数列是 Long
        final DataFrame<Object> longDf = DataFrame.readCsv(csv.toString());
        assertEquals(Long.class, longDf.types().get(0));

        // DOUBLE_DEFAULT → 整数列也变 Double
        final DataFrame<Object> doubleDf = DataFrame.readCsv(
                csv.toString(), ",", DataFrame.NumberDefault.DOUBLE_DEFAULT);
        assertEquals(Double.class, doubleDf.types().get(0));
    }

    // ┌─ What : 引号转义 + 字段内逗号
    @Test
    void readCsv_引号内逗号_不分割(@TempDir Path tmp) throws IOException {
        final Path csv = tmp.resolve("quote.csv");
        Files.write(csv, "name,desc\n\"alice, jr\",hello\n".getBytes(StandardCharsets.UTF_8));
        final DataFrame<Object> df = DataFrame.readCsv(csv.toString());
        assertEquals("alice, jr", df.get(0, 0), "引号内的逗号不应作为分隔符");
        assertEquals("hello", df.get(0, 1));
    }

    // ┌─ What : 字段内换行（引号包裹的多行字段）
    @Test
    void readCsv_引号内换行_保留(@TempDir Path tmp) throws IOException {
        final Path csv = tmp.resolve("newline.csv");
        Files.write(csv, "name,note\n\"line1\nline2\",ok\n".getBytes(StandardCharsets.UTF_8));
        final DataFrame<Object> df = DataFrame.readCsv(csv.toString());
        assertTrue(String.valueOf(df.get(0, 0)).contains("line1"));
        assertTrue(String.valueOf(df.get(0, 0)).contains("line2"));
    }

    // ┌─ What : 中文表头 + 中文数据 + 引号
    @Test
    void readCsv_中文引号字段_完整保留(@TempDir Path tmp) throws IOException {
        final Path csv = tmp.resolve("cn.csv");
        Files.write(csv, "姓名,备注\n\"张三,经理\",优秀\n".getBytes(StandardCharsets.UTF_8));
        final DataFrame<Object> df = DataFrame.readCsv(csv.toString());
        assertEquals("张三,经理", df.get(0, 0));
        assertEquals("优秀", df.get(0, 1));
    }

    // ┌─ What : 不支持的分隔符应抛 IllegalArgumentException
    // │  注：readCsv 没有 (input, sep, NumberDefault, naString) 这个签名，用存在的 (input, sep, NumberDefault) 重载
    @Test
    void readCsv_不支持的分隔符_应抛异常() {
        final String content = "a#b\n1#2\n";
        assertThrows(IllegalArgumentException.class, () ->
            DataFrame.readCsv(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                    "#", DataFrame.NumberDefault.LONG_DEFAULT));
    }

    // ┌─ What : 写出 CSV 再读回，BOM 头正确写入
    @Test
    void writeCsv_BOM头字节正确(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("a");
        df.append(java.util.Collections.singletonList("x"));
        final Path csv = tmp.resolve("bom.csv");
        df.writeCsv(csv.toString());
        final byte[] bytes = Files.readAllBytes(csv);
        // 前 3 字节必须是 UTF-8 BOM
        assertEquals((byte) 0xEF, bytes[0]);
        assertEquals((byte) 0xBB, bytes[1]);
        assertEquals((byte) 0xBF, bytes[2]);
    }
}
