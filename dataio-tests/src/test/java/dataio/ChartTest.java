package dataio;

import joinery.DataFrame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.awt.GraphicsEnvironment;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : 图表（hist/box/scatter/pie/saveChart）+ 中文标题 + headless PNG 导出测试
 * │  Why  : 验证 Display 重写后的 XChart 4.0.4 适配 + 图表加强方法 + 中文 CJK 字体渲染 + PNG 落盘
 * │  Who  : dataio-tests 测试套件
 * │  When : mvn test 阶段（headless 环境）
 * │  Where: dataio-tests/src/test/java/dataio/ChartTest.java
 */
class ChartTest {

    private DataFrame<Object> sampleData() {
        final DataFrame<Object> df = new DataFrame<>("分类", "数量", "金额");
        df.append(java.util.Arrays.asList("水果", 10, 100.0));
        df.append(java.util.Arrays.asList("水果", 20, 200.0));
        df.append(java.util.Arrays.asList("饮料", 15, 150.0));
        df.append(java.util.Arrays.asList("饮料", 25, 250.0));
        df.append(java.util.Arrays.asList("零食", 30, 300.0));
        df.convert();
        return df;
    }

    @Test
    void hist_直方图构造成功_含中文标题() {
        final DataFrame<Object> df = sampleData();
        final org.knowm.xchart.XYChart chart = df.hist("数量", 5);
        assertNotNull(chart, "hist 应返回非 null chart");
        // 标题应含中文
        assertTrue(chart.getTitle().contains("直方图"), "标题应含中文「直方图」，实际: " + chart.getTitle());
    }

    @Test
    void box_箱线图构造成功_含中文标题() {
        final DataFrame<Object> df = sampleData();
        final org.knowm.xchart.BoxChart chart = df.box("数量", "金额");
        assertNotNull(chart, "box 应返回非 null chart");
        assertEquals("箱线图", chart.getTitle());
    }

    @Test
    void scatter_散点图构造成功_含中文标题() {
        final DataFrame<Object> df = sampleData();
        final org.knowm.xchart.XYChart chart = df.scatter("数量", "金额");
        assertNotNull(chart);
        assertTrue(chart.getTitle().contains("散点图"), "标题应含「散点图」，实际: " + chart.getTitle());
    }

    @Test
    void pie_饼图构造成功_含中文标题() {
        final DataFrame<Object> df = sampleData();
        final org.knowm.xchart.PieChart chart = df.pie("分类", "金额");
        assertNotNull(chart);
        assertTrue(chart.getTitle().contains("饼图"));
    }

    // ┌─ What : saveChart 在 headless 环境导出 PNG（对标 matplotlib savefig）
    // │  Why  : 服务器/CI 无显示器，图表必须能落盘；BitmapEncoder 用 java.awt.image.BufferedImage，
    // │         在 headless=true 下仍可工作（不依赖真实显示器）
    @Test
    void saveChart_headless环境导出PNG(@TempDir Path tmp) throws Exception {
        // 确保 headless 模式（CI 友好）
        System.setProperty("java.awt.headless", "true");
        assertTrue(GraphicsEnvironment.isHeadless(), "测试需 headless=true 才能在 CI 跑");

        final DataFrame<Object> df = sampleData();
        final org.knowm.xchart.XYChart chart = df.hist("数量", 5);

        final Path png = tmp.resolve("histogram.png");
        DataFrame.saveChart(chart, png.toString());

        // 文件应存在且非空（PNG 文件头前 8 字节：89 50 4E 47 0D 0A 1A 0A）
        assertTrue(Files.exists(png), "PNG 文件应生成");
        assertTrue(Files.size(png) > 1024, "PNG 文件应 > 1KB");
        final byte[] head = Files.readAllBytes(png).clone();
        // 验证 PNG 签名
        assertEquals((byte) 0x89, head.length > 0 ? head[0] : 0, "PNG 头第 1 字节");
        assertEquals((byte) 0x50, head.length > 1 ? head[1] : 0, "PNG 头第 2 字节 P");
        assertEquals((byte) 0x4E, head.length > 2 ? head[2] : 0, "PNG 头第 3 字节 N");
        assertEquals((byte) 0x47, head.length > 3 ? head[3] : 0, "PNG 头第 4 字节 G");
    }
}
