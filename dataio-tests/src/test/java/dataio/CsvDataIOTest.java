package dataio;

import joinery.DataFrame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : CSV 读写 + 中文 + BOM 回归测试
 * │  Why  : 验证 readCsv/writeCsv 在中文场景下的正确性，重点覆盖：
 * │           - UTF-8 BOM 头自动剥离（Excel 中文 Windows 导出的 CSV 多带 BOM）
 * │           - 写入带 BOM 让 Excel 中文环境打开不乱码
 * │           - 中文字符串/列名/数据完整保留
 * │           - 读写往返后类型保持
 * │  Who  : dataio-tests 测试套件
 * │  When : mvn test 阶段
 * │  Where: dataio-tests/src/test/java/dataio/CsvDataIOTest.java
 */
class CsvDataIOTest {

    // ┌─ What : 读 UTF-8 带 BOM 的中文 CSV（首列列名不应混入 BOM 字符）
    // │  Why  : 旧实现裸 InputStreamReader 不去 BOM，导致首列名变成 "?category" 而非 "category"
    @Test
    void readUtf8BomChineseCsv_首列名不应混入BOM字符() throws IOException {
        // 构造带 BOM 的中文 CSV（模拟 Excel 中文 Windows 导出）
        final byte[] bom = { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };
        final String content = "分类,名称,数量\n水果,苹果,10\n水果,香蕉,20\n蔬菜,白菜,5\n";
        final byte[] bytes = concat(bom, content.getBytes(StandardCharsets.UTF_8));

        final DataFrame<Object> df = DataFrame.readCsv(new ByteArrayInputStream(bytes));

        // 首列名必须干净（不带 BOM）
        assertEquals("分类", df.columns().iterator().next(), "带 BOM 的 CSV 首列名应正确解析");
        assertEquals(3, df.length());
        assertEquals("苹果", df.get(0, 1));   // 第 0 行第 1 列
        assertEquals("香蕉", df.get(1, 1));
    }

    // ┌─ What : 写 CSV 再用 Excel 兼容方式读回，验证 BOM 头 + 中文不乱码
    @Test
    void writeChineseCsv_应包含BOM头且中文不乱码(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("姓名", "城市", "年龄");
        df.append(java.util.Arrays.asList("张三", "北京", 25));
        df.append(java.util.Arrays.asList("李四", "上海", 30));
        df.append(java.util.Arrays.asList("王五", "广州", 28));

        final Path csv = tmp.resolve("中文.csv");
        df.writeCsv(csv.toString());

        // 读回字节数组，前 3 字节必须是 UTF-8 BOM
        final byte[] bytes = java.nio.file.Files.readAllBytes(csv);
        assertEquals((byte) 0xEF, bytes[0], "CSV 头应为 UTF-8 BOM 第 1 字节 EF");
        assertEquals((byte) 0xBB, bytes[1], "CSV 头应为 UTF-8 BOM 第 2 字节 BB");
        assertEquals((byte) 0xBF, bytes[2], "CSV 头应为 UTF-8 BOM 第 3 字节 BF");

        // 写出的内容应包含中文（用 UTF-8 解码后断言）
        final String text = new String(bytes, StandardCharsets.UTF_8).substring(1); // 去掉 BOM 字符
        assertTrue(text.contains("张三"), "CSV 内容应含中文「张三」");
        assertTrue(text.contains("广州"), "CSV 内容应含中文「广州」");

        // 读回应完整保留
        final DataFrame<Object> back = DataFrame.readCsv(csv.toString());
        assertEquals("姓名", back.columns().iterator().next());
        assertEquals("张三", back.get(0, 0));
    }

    // ┌─ What : CSV 读写类型保持（long/double/string/date）
    @Test
    void csvReadWriteRoundTrip_保留推断类型(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("整数", "浮点", "文本");
        df.append(java.util.Arrays.asList(1L, 1.5, "abc"));
        df.append(java.util.Arrays.asList(2L, 2.5, "xyz"));
        df.convert();

        final Path csv = tmp.resolve("types.csv");
        df.writeCsv(csv.toString());
        final DataFrame<Object> back = DataFrame.readCsv(csv.toString());

        assertEquals(Long.class, back.types().get(0), "整数列应为 Long");
        assertEquals(Double.class, back.types().get(1), "浮点列应为 Double");
        assertEquals(String.class, back.types().get(2), "文本列应为 String");
    }

    @Test
    void readCsv文件不存在_应抛IOException() {
        assertThrows(IOException.class, () -> DataFrame.readCsv("/tmp/this-file-does-not-exist-12345.csv"));
    }

    private static byte[] concat(byte[] a, byte[] b) {
        final byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }
}
