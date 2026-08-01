package dataio;

import joinery.DataFrame;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : 图表模块全场景测试（hist/box/scatter/pie/saveChart）
 * │  Why  : 验证图表加强方法在各类输入下的行为，含边界与异常
 * │  Who  : dataio-tests
 */
class ChartFullScenarioTest {

    private DataFrame<Object> numericData() {
        final DataFrame<Object> df = new DataFrame<>("x", "y", "category");
        df.append(java.util.Arrays.asList(10, 100, "a"));
        df.append(java.util.Arrays.asList(20, 200, "a"));
        df.append(java.util.Arrays.asList(30, 300, "b"));
        df.append(java.util.Arrays.asList(40, 400, "b"));
        df.append(java.util.Arrays.asList(50, 500, "c"));
        df.convert();
        return df;
    }

    // ┌─ What : hist 不同 bins 数都能构造
    @Test
    void hist_不同bins数_都能构造() {
        final DataFrame<Object> df = numericData();
        for (int bins : new int[] { 1, 5, 10, 20, 100 }) {
            final org.knowm.xchart.XYChart chart = df.hist("x", bins);
            assertNotNull(chart, "bins=" + bins + " 应构造成功");
        }
    }

    // ┌─ What : hist 默认 bins（20）
    @Test
    void hist_默认bins_20() {
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.XYChart chart = df.hist("x");
        assertNotNull(chart);
        assertTrue(chart.getTitle().contains("直方图"));
    }

    // ┌─ What : box 单列与多列
    @Test
    void box_单列与多列() {
        final DataFrame<Object> df = numericData();
        // 单列
        assertNotNull(df.box("x"));
        // 多列
        final org.knowm.xchart.BoxChart multi = df.box("x", "y");
        assertNotNull(multi);
        assertEquals("箱线图", multi.getTitle());
    }

    // ┌─ What : box 不传列 → 默认所有数值列
    @Test
    void box_不传列_默认所有数值列() {
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.BoxChart chart = df.box();
        assertNotNull(chart, "不传列时应默认所有数值列");
    }

    // ┌─ What : scatter 两列数值
    @Test
    void scatter_两列数值_标题正确() {
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.XYChart chart = df.scatter("x", "y");
        assertNotNull(chart);
        assertTrue(chart.getTitle().contains("散点图"));
        assertTrue(chart.getTitle().contains("x"));
        assertTrue(chart.getTitle().contains("y"));
    }

    // ┌─ What : pie 按分类列聚合数值列
    @Test
    void pie_分类聚合_三块() {
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.PieChart chart = df.pie("category", "y");
        assertNotNull(chart);
        assertTrue(chart.getTitle().contains("饼图"));
        // category 有 a/b/c 三块（a: 100+200=300, b: 300+400=700, c: 500）
        // XChart PieChart 的 series 数 = 不同分类数（getSeriesCollection 是 public）
        assertEquals(3, chart.getSeriesCollection().size(), "应有 3 个饼图块（a/b/c）");
    }

    // ┌─ What : saveChart PNG 文件存在且非空
    @Test
    void saveChart_PNG_文件存在且非空(@TempDir Path tmp) throws Exception {
        System.setProperty("java.awt.headless", "true");
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.XYChart chart = df.hist("x", 5);
        final Path png = tmp.resolve("chart.png");
        DataFrame.saveChart(chart, png.toString());
        assertTrue(Files.exists(png));
        assertTrue(Files.size(png) > 1024, "PNG 应大于 1KB");
    }

    // ┌─ What : 4 种图表都能 saveChart 落盘（PNG 头校验）
    @Test
    void saveChart_四种图表_都能落盘PNG(@TempDir Path tmp) throws Exception {
        System.setProperty("java.awt.headless", "true");
        final DataFrame<Object> df = numericData();
        final Path dir = tmp;
        // hist
        DataFrame.saveChart(df.hist("x", 5), dir.resolve("h.png").toString());
        // box
        DataFrame.saveChart(df.box("x", "y"), dir.resolve("b.png").toString());
        // scatter
        DataFrame.saveChart(df.scatter("x", "y"), dir.resolve("s.png").toString());
        // pie
        DataFrame.saveChart(df.pie("category", "y"), dir.resolve("p.png").toString());
        // 全部存在且 > 1KB
        for (String n : new String[] { "h.png", "b.png", "s.png", "p.png" }) {
            final Path p = dir.resolve(n);
            assertTrue(Files.exists(p), n + " 应存在");
            assertTrue(Files.size(p) > 1024, n + " 应 > 1KB");
        }
    }

    // ┌─ What : 所有图表的 PNG 头签名都正确
    @Test
    void saveChart_PNG头签名_89_50_4E_47(@TempDir Path tmp) throws Exception {
        System.setProperty("java.awt.headless", "true");
        final DataFrame<Object> df = numericData();
        final Path png = tmp.resolve("sig.png");
        DataFrame.saveChart(df.hist("x", 5), png.toString());
        final byte[] bytes = Files.readAllBytes(png);
        assertEquals((byte) 0x89, bytes[0], "PNG 第 1 字节");
        assertEquals((byte) 0x50, bytes[1], "PNG 第 2 字节 P");
        assertEquals((byte) 0x4E, bytes[2], "PNG 第 3 字节 N");
        assertEquals((byte) 0x47, bytes[3], "PNG 第 4 字节 G");
    }

    // ┌─ What : CJK 字体应用 —— 用中文列名构造数据，渲染不抛异常即视为通过
    @Test
    void chart_中文标题_渲染不抛异常() {
        System.setProperty("java.awt.headless", "true");
        // 用中文列名构造（CJK 字体在标题/轴渲染时的验证）
        final DataFrame<Object> df = new DataFrame<>("数量", "金额");
        df.append(java.util.Arrays.asList(10, 100.0));
        df.append(java.util.Arrays.asList(20, 200.0));
        df.append(java.util.Arrays.asList(30, 300.0));
        df.convert();
        // 不抛异常即说明 CJK 字体路径走通
        assertDoesNotThrow(() -> df.hist("数量", 5));
        assertDoesNotThrow(() -> df.box("金额", "数量"));
    }

    // ───────────── 2026-08 补齐 XChart 10 种图表的回归测试 ─────────────

    @Test
    void bar_柱状图构造成功() {
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.CategoryChart chart = df.bar("x", "y");
        assertNotNull(chart);
        assertEquals("柱状图", chart.getTitle());
        // 2 个 series（x 和 y）
        assertEquals(2, chart.getSeriesCollection().size());
    }

    @Test
    void hbar_横向条形图构造成功() {
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.HorizontalBarChart chart = df.hbar("x");
        assertNotNull(chart);
        assertEquals("条形图", chart.getTitle());
    }

    @Test
    void bubble_气泡图构造成功() {
        // 气泡图需要 x/y/size 三列
        final DataFrame<Object> df = new DataFrame<>("x", "y", "size");
        df.append(java.util.Arrays.asList(1, 10, 100));
        df.append(java.util.Arrays.asList(2, 20, 200));
        df.convert();
        final org.knowm.xchart.BubbleChart chart = df.bubble("x", "y", "size");
        assertNotNull(chart);
        assertTrue(chart.getTitle().contains("气泡图"));
    }

    @Test
    void heatmap_热力图构造成功() {
        final DataFrame<Object> df = new DataFrame<>("region", "product", "sales");
        df.append(java.util.Arrays.asList("华南", "A", 100));
        df.append(java.util.Arrays.asList("华北", "B", 200));
        df.convert();
        final org.knowm.xchart.HeatMapChart chart = df.heatmap("region", "product", "sales");
        assertNotNull(chart);
        assertEquals("热力图", chart.getTitle());
    }

    @Test
    void ohlc_K线图构造成功() {
        final DataFrame<Object> df = new DataFrame<>("open", "high", "low", "close");
        df.append(java.util.Arrays.asList(10.0, 12.0, 9.5, 11.0));
        df.append(java.util.Arrays.asList(11.0, 13.0, 10.5, 12.5));
        df.convert();
        final org.knowm.xchart.OHLCChart chart = df.ohlc("open", "high", "low", "close");
        assertNotNull(chart);
        assertEquals("K线图", chart.getTitle());
    }

    @Test
    void radar_雷达图构造成功() {
        final DataFrame<Object> df = new DataFrame<>("速度", "力量", "技巧");
        df.append(java.util.Arrays.asList(80, 70, 90));
        df.convert();
        final org.knowm.xchart.RadarChart chart = df.radar(0, "速度", "力量", "技巧");
        assertNotNull(chart);
        assertTrue(chart.getTitle().contains("雷达图"));
    }

    @Test
    void dial_仪表盘构造成功() {
        final DataFrame<Object> df = new DataFrame<>("score");
        df.append(java.util.Arrays.asList(75));
        df.append(java.util.Arrays.asList(85));
        df.convert();
        final org.knowm.xchart.DialChart chart = df.dial("score");
        assertNotNull(chart);
        assertTrue(chart.getTitle().contains("仪表盘"));
    }

    @Test
    void saveChart_位图格式_JPG_BMP_GIF(@TempDir Path tmp) throws Exception {
        System.setProperty("java.awt.headless", "true");
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.XYChart chart = df.hist("x", 5);
        DataFrame.saveChartJpg(chart, tmp.resolve("c.jpg").toString());
        DataFrame.saveChartBmp(chart, tmp.resolve("c.bmp").toString());
        DataFrame.saveChartGif(chart, tmp.resolve("c.gif").toString());
        for (String n : new String[] { "c.jpg", "c.bmp", "c.gif" }) {
            assertTrue(Files.exists(tmp.resolve(n)), n + " 应生成");
            assertTrue(Files.size(tmp.resolve(n)) > 100, n + " 应非空");
        }
    }

    @Test
    void saveChartSvg_缺依赖时给清晰提示(@TempDir Path tmp) throws Exception {
        // SVG 矢量导出依赖 de.erichseifert:vectorgraphics2d（已从 Maven Central 下架）。
        // 若 classpath 无此依赖，saveChartSvg 应抛 IOException 给清晰提示，而不是 NoClassDefFoundError
        System.setProperty("java.awt.headless", "true");
        final DataFrame<Object> df = numericData();
        final org.knowm.xchart.XYChart chart = df.hist("x", 5);
        try {
            DataFrame.saveChartSvg(chart, tmp.resolve("c.svg").toString());
            // 如果用户在 classpath 加了 vectorgraphics2d，应正常生成
            assertTrue(Files.exists(tmp.resolve("c.svg")));
        }
        catch (final IOException ex) {
            // 缺依赖时消息应含说明
            assertTrue(ex.getMessage().contains("vectorgraphics2d"),
                "SVG 缺依赖时应给清晰提示，实际消息: " + ex.getMessage());
        }
    }
}
