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

package joinery.impl;

import java.awt.Color;
import java.awt.Container;
import java.awt.Font;
import java.awt.GridLayout;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BoxChart;
import org.knowm.xchart.BoxChartBuilder;
import org.knowm.xchart.BubbleChart;
import org.knowm.xchart.BubbleChartBuilder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.DialChart;
import org.knowm.xchart.DialChartBuilder;
import org.knowm.xchart.HeatMapChart;
import org.knowm.xchart.HeatMapChartBuilder;
import org.knowm.xchart.Histogram;
import org.knowm.xchart.HorizontalBarChart;
import org.knowm.xchart.HorizontalBarChartBuilder;
import org.knowm.xchart.OHLCChart;
import org.knowm.xchart.OHLCChartBuilder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.RadarChart;
import org.knowm.xchart.RadarChartBuilder;
import org.knowm.xchart.VectorGraphicsEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle;
import org.knowm.xchart.style.lines.SeriesLines;
import org.knowm.xchart.style.markers.SeriesMarkers;

import joinery.DataFrame;
import joinery.DataFrame.PlotType;

/**
 * ┌─ What : Display 负责数据框的可视化（GUI 弹窗 + PNG 导出）
 * │  Why  : 本类把 joinery DataFrame 接入 XChart 4.0.4 绘图引擎；
 * │         升级历史：旧版用 com.xeiam.xchart:2.5.1（已停止维护），2026-08 重写为 org.knowm.xchart:4.0.4，
 * │         并参考 Python pandas 的 df.plot()/hist()/box()/scatter() 接口形态，新增多种图表类型与无 GUI PNG 导出。
 * │  Who  : 由 DataFrame.plot / show / hist / box / scatter / pie / saveChart 调用
 * │  When : 用户需要可视化时
 * │  Where: 本类
 * │  How  : 见各方法的 5W1H 注释
 */
public class Display {
    // ┌─ What : 中文 CJK 字体常量（仅写字体名，不嵌入字体文件，遵循字体铁律）
    // │  Why  : XChart 默认字体（Arial）不支持 CJK，中文标题/轴标签会显示成方框；
    // │         用「微软雅黑」（Windows 常见）+ Linux/macOS 回退到「SansSerif」（逻辑字体）
    // │  Who  : 所有绘图方法使用
    private static final String CJK_FONT = "微软雅黑";
    private static final String CJK_FONT_FALLBACK = Font.SANS_SERIF;

    // ┌─ What : 取一个本机存在的 CJK 字体名
    // │  Why  : 若本机无「微软雅黑」（典型 Linux 服务器），强制用会导致字符退回默认字体；
    // │         用 GraphicsEnvironment 探测，缺失则回退到 SansSerif（Java 逻辑字体，三平台都能渲染中文）
    // │  How  : 遍历本机已装字体，命中 CJK_FONT 则用，否则用 fallback
    private static String cjkFontName() {
        final String[] available = java.awt.GraphicsEnvironment.getLocalGraphicsEnvironment().getAvailableFontFamilyNames();
        if (available != null) {
            for (final String f : available) {
                if (CJK_FONT.equals(f)) {
                    return CJK_FONT;
                }
            }
        }
        return CJK_FONT_FALLBACK;
    }

    public static <C extends Container, V> C draw(final DataFrame<V> df, final C container, final PlotType type) {
        final List<XYChart> charts = buildXYCharts(df, type);
        final List<org.knowm.xchart.XChartPanel<XYChart>> panels = new LinkedList<>();
        for (final XYChart chart : charts) {
            panels.add(new org.knowm.xchart.XChartPanel<>(chart));
        }

        if (panels.size() > 1) {
            final int rows = (int)Math.ceil(Math.sqrt(charts.size()));
            final int cols = charts.size() / rows + 1;
            container.setLayout(new GridLayout(rows, cols));
        }
        for (final org.knowm.xchart.XChartPanel<XYChart> p : panels) {
            container.add(p);
        }

        return container;
    }

    // ┌─ What : 构造 XYChart 列表（LINE/AREA/SCATTER/GIRD，对应 pandas plot 风格）
    // │  Why  : XChart 4.0.4 不再有统一的 Chart/ChartBuilder，按 AxesChart 子类型分别用 XYChartBuilder；
    // │         旧版的 StyleManager.ChartType 已移除，改用 styler.setChartTitleFont 等独立 setter
    // │  How  : 见内联 5W1H
    private static <V> List<XYChart> buildXYCharts(final DataFrame<V> df, final PlotType type) {
        final List<XYChart> charts = new LinkedList<>();
        final DataFrame<Number> numeric = df.numeric().fillna(0);

        // x 轴数据：优先用 index 中的 Number/Date，否则按 0,1,2... 递增（BAR 类型用字符串）
        final List<Object> xdata = new ArrayList<>(df.length());
        final Iterator<Object> it = df.index().iterator();
        for (int i = 0; i < df.length(); i++) {
            final Object value = it.hasNext() ? it.next() : i;
            if (value instanceof Number || value instanceof Date) {
                xdata.add(value);
            }
            else if (PlotType.BAR.equals(type)) {
                xdata.add(String.valueOf(value));
            }
            else {
                xdata.add(i);
            }
        }

        final String font = cjkFontName();
        if (EnumSet.of(PlotType.GRID, PlotType.GRID_WITH_TREND).contains(type)) {
            // 网格布局：每个数值列一张子图（对应 pandas subplots=True）
            for (final Object col : numeric.columns()) {
                final XYChart chart = new XYChartBuilder()
                        .width(420).height(360)
                        .title(String.valueOf(col))
                        .build();
                applyCJKFont(chart, font);
                configureLineStyle(chart, type, xdata);
                final XYSeries series = chart.addSeries(String.valueOf(col),
                        toNumberList(xdata), numeric.col(col));
                if (type == PlotType.GRID_WITH_TREND) {
                    addTrend(chart, series, xdata);
                    series.setLineStyle(SeriesLines.NONE);
                }
                chart.getStyler().setLegendVisible(false);
                if (!xdata.isEmpty() && xdata.get(0) instanceof Date) {
                    chart.getStyler().setDatePattern(dateFormat(xdata));
                }
                charts.add(chart);
            }
        }
        else {
            final XYChart chart = new XYChartBuilder().build();
            applyCJKFont(chart, font);
            configureLineStyle(chart, type, xdata);
            if (!xdata.isEmpty() && xdata.get(0) instanceof Date) {
                chart.getStyler().setDatePattern(dateFormat(xdata));
            }
            switch (type) {
                case SCATTER: case SCATTER_WITH_TREND: case LINE_AND_POINTS: break;
                default: chart.getStyler().setMarkerSize(0); break;
            }
            for (final Object col : numeric.columns()) {
                final XYSeries series = chart.addSeries(String.valueOf(col),
                        toNumberList(xdata), numeric.col(col));
                if (type == PlotType.SCATTER_WITH_TREND) {
                    addTrend(chart, series, xdata);
                    series.setLineStyle(SeriesLines.NONE);
                }
            }
            charts.add(chart);
        }
        return charts;
    }

    // ┌─ What : 给 XYChart 应用 CJK 字体（标题、图例、轴标题，中文完美支持）
    // │  Why  : XChart 默认 Arial 不渲染中文，标题/轴标签会变方框；统一设为 CJK 字体名
    private static void applyCJKFont(final XYChart chart, final String font) {
        final Font titleFont = new Font(font, Font.BOLD, 14);
        final Font axisFont = new Font(font, Font.PLAIN, 12);
        chart.getStyler().setChartTitleFont(titleFont);
        chart.getStyler().setLegendFont(axisFont);
        chart.getStyler().setAxisTitleFont(axisFont);
        chart.getStyler().setAxisTickLabelsFont(axisFont);
    }

    // ┌─ What : 按图表类型配置渲染样式（散点/线/区域）
    private static void configureLineStyle(final XYChart chart, final PlotType type, final List<Object> xdata) {
        switch (type) {
            case SCATTER: case SCATTER_WITH_TREND:
                // 散点：仅标记，无连线
                chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Scatter);
                break;
            case AREA:
                chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Area);
                break;
            case BAR:
                // BAR 走 CategoryChart 更合适，这里退化为 Area 的近似渲染
                chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Area);
                break;
            default:
                chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Line);
                break;
        }
    }

    // ┌─ What : 把 List<Object> 转为 List<? extends Number>（适配 XChart 4.0.4 addSeries 签名）
    // │  Why  : XChart 4.x 的 yData 要求 List<? extends Number>；xData 可为 List<?>
    private static List<? extends Number> toNumberList(final List<Object> list) {
        final List<Number> out = new ArrayList<>(list.size());
        for (final Object o : list) {
            if (o instanceof Number) {
                out.add((Number) o);
            }
            else if (o instanceof Date) {
                out.add(((Date) o).getTime());
            }
            else {
                out.add(0);
            }
        }
        return out;
    }

    public static <V> void plot(final DataFrame<V> df, final PlotType type) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                final JFrame frame = draw(df, new JFrame(title(df)), type);
                frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
                frame.pack();
                frame.setVisible(true);
            }
        });
    }

    public static <V> void show(final DataFrame<V> df) {
        final List<Object> columns = new ArrayList<>(df.columns());
        final List<Class<?>> types = df.types();
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                final JFrame frame = new JFrame(title(df));
                final JTable table = new JTable(
                        new AbstractTableModel() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public int getRowCount() {
                                return df.length();
                            }

                            @Override
                            public int getColumnCount() {
                                return df.size();
                            }

                            @Override
                            public Object getValueAt(final int row, final int col) {
                                return df.get(row, col);
                            }

                            @Override
                            public String getColumnName(final int col) {
                                return String.valueOf(columns.get(col));
                            }

                            @Override
                            public Class<?> getColumnClass(final int col) {
                                return types.get(col);
                            }
                        }
                    );
                table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
                frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
                frame.add(new JScrollPane(table));
                frame.pack();
                frame.setVisible(true);
            }
        });
    }

    // ───────────────────────── 图表加强（参考 pandas + XChart 4.0.4） ─────────────────────────
    // 以下方法为 2026-08 新增，对标 pandas 的 df.hist() / df.boxplot() / pd.scatter_matrix() / df.plot.pie()
    // 以及无 GUI 的 saveChart(path)，让 joinery 在服务器/CI 环境（headless）也能输出图表

    /**
     * ┌─ What : 绘制直方图（对标 pandas df.hist）
     * │  Why  : 数据分布分析的基础图；用 XChart 的 Histogram 工具类做分箱
     * │  Who  : 由 DataFrame.hist 调用
     * │  When : 用户需查看数值列分布时
     * │  How  : 对指定列用 Histogram 做等宽分箱（默认 20 bin），输出 CategoryChart 风格的柱状图
     */
    public static <V> XYChart hist(final DataFrame<V> df, final Object column, final int bins) {
        final List<V> col = df.col(column);
        final List<Number> nums = new ArrayList<>(col.size());
        for (final V v : col) {
            if (v instanceof Number) {
                nums.add((Number) v);
            }
        }
        final Histogram histogram = new Histogram(nums, bins);
        final XYChart chart = new XYChartBuilder()
                .width(600).height(400)
                .title("直方图: " + column)
                .xAxisTitle("分箱").yAxisTitle("频次")
                .build();
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Area);
        chart.addSeries(String.valueOf(column), histogram.getxAxisData(), histogram.getyAxisData());
        applyCJKFont(chart, cjkFontName());
        return chart;
    }

    /**
     * ┌─ What : 绘制箱线图（对标 pandas df.boxplot）
     * │  Why  : 看异常值/四分位分布；XChart 4.0.4 原生支持 BoxChart
     * │  Who  : 由 DataFrame.box 调用
     * │  How  : 把指定列的数值收集起来 addSeries 到 BoxChart
     */
    public static <V> BoxChart box(final DataFrame<V> df, final Object... columns) {
        final BoxChart chart = new BoxChartBuilder()
                .width(600).height(400)
                .title("箱线图")
                .xAxisTitle("列").yAxisTitle("数值")
                .build();
        final List<Object> cols = columns == null || columns.length == 0
                ? new ArrayList<>(df.columns()) : Arrays.asList(columns);
        for (final Object col : cols) {
            final List<Number> nums = new ArrayList<>();
            for (final V v : df.col(col)) {
                if (v instanceof Number) {
                    nums.add((Number) v);
                }
            }
            if (!nums.isEmpty()) {
                chart.addSeries(String.valueOf(col), nums);
            }
        }
        // BoxChart 同样需 CJK 字体
        final Font font = new Font(cjkFontName(), Font.PLAIN, 12);
        chart.getStyler().setChartTitleFont(new Font(cjkFontName(), Font.BOLD, 14));
        chart.getStyler().setLegendFont(font);
        chart.getStyler().setAxisTitleFont(font);
        chart.getStyler().setAxisTickLabelsFont(font);
        return chart;
    }

    /**
     * ┌─ What : 绘制散点图（对标 pandas df.plot.scatter）
     * │  Why  : 看两列之间的相关性
     * │  Who  : 由 DataFrame.scatter 调用
     * │  How  : 把 x 列、y 列成对收集后 addSeries
     */
    public static <V> XYChart scatter(final DataFrame<V> df, final Object xCol, final Object yCol) {
        final List<V> xs = df.col(xCol);
        final List<V> ys = df.col(yCol);
        final List<Number> xn = new ArrayList<>();
        final List<Number> yn = new ArrayList<>();
        final int n = Math.min(xs.size(), ys.size());
        for (int i = 0; i < n; i++) {
            if (xs.get(i) instanceof Number && ys.get(i) instanceof Number) {
                xn.add((Number) xs.get(i));
                yn.add((Number) ys.get(i));
            }
        }
        final XYChart chart = new XYChartBuilder()
                .width(600).height(400)
                .title(String.format("散点图: %s vs %s", xCol, yCol))
                .xAxisTitle(String.valueOf(xCol)).yAxisTitle(String.valueOf(yCol))
                .build();
        chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Scatter);
        chart.addSeries(String.valueOf(yCol), xn, yn);
        applyCJKFont(chart, cjkFontName());
        return chart;
    }

    /**
     * ┌─ What : 绘制饼图（对标 pandas df.plot.pie）
     * │  Why  : 看分类占比
     * │  Who  : 由 DataFrame.pie 调用
     * │  How  : categoryCol 的每个不同值分组聚合 valueCol 的和，再 addSeries 到 PieChart
     */
    public static <V> PieChart pie(final DataFrame<V> df, final Object categoryCol, final Object valueCol) {
        final PieChart chart = new PieChartBuilder()
                .width(600).height(400)
                .title("饼图: " + categoryCol + " → " + valueCol)
                .build();
        // 分组求和（参考 pandas groupby + sum）
        final Map<String, Double> sums = new HashMap<>();
        final List<V> cats = df.col(categoryCol);
        final List<V> vals = df.col(valueCol);
        final int n = Math.min(cats.size(), vals.size());
        for (int i = 0; i < n; i++) {
            final String key = String.valueOf(cats.get(i));
            final double v = (vals.get(i) instanceof Number) ? ((Number) vals.get(i)).doubleValue() : 0;
            sums.merge(key, v, Double::sum);
        }
        for (final Map.Entry<String, Double> e : sums.entrySet()) {
            chart.addSeries(e.getKey(), e.getValue());
        }
        chart.getStyler().setChartTitleFont(new Font(cjkFontName(), Font.BOLD, 14));
        chart.getStyler().setLegendFont(new Font(cjkFontName(), Font.PLAIN, 12));
        return chart;
    }

    /**
     * ┌─ What : 把任意 XChart 图保存为 PNG（无 GUI，对标 matplotlib savefig）
     * │  Why  : 服务器/CI 环境 headless 无法弹窗，saveChart 让图表能落盘到文件
     * │  Who  : 由 DataFrame.saveChart 调用
     * │  When : 用户要把图表保存为图片时
     * │  How  : BitmapEncoder.saveBitmap(chart, path, PNG)
     */
    public static void saveChart(final org.knowm.xchart.internal.chartpart.Chart<?, ?> chart, final String path)
    throws IOException {
        BitmapEncoder.saveBitmap(chart, path, BitmapEncoder.BitmapFormat.PNG);
    }

    // ┌─ What : 把图表保存为 JPG（无 GUI，对标 matplotlib savefig）
    public static void saveChartJpg(final org.knowm.xchart.internal.chartpart.Chart<?, ?> chart, final String path)
    throws IOException {
        BitmapEncoder.saveBitmap(chart, path, BitmapEncoder.BitmapFormat.JPG);
    }

    // ┌─ What : 把图表保存为 BMP
    public static void saveChartBmp(final org.knowm.xchart.internal.chartpart.Chart<?, ?> chart, final String path)
    throws IOException {
        BitmapEncoder.saveBitmap(chart, path, BitmapEncoder.BitmapFormat.BMP);
    }

    // ┌─ What : 把图表保存为 GIF
    public static void saveChartGif(final org.knowm.xchart.internal.chartpart.Chart<?, ?> chart, final String path)
    throws IOException {
        BitmapEncoder.saveBitmap(chart, path, BitmapEncoder.BitmapFormat.GIF);
    }

    // ┌─ What : 把图表保存为 SVG 矢量图（对标 matplotlib 的矢量导出，可无损缩放）
    // │  Why  : SVG/EPS/PDF 矢量导出走 XChart 的 VectorGraphicsEncoder，
    // │         它在运行时依赖外部 de.erichseifert.vectorgraphics2d 包。
    // │         该包近年已从 Maven Central 下架（XChart 4.0.4 的设计缺陷），
    // │         若调用方未在 classpath 提供，则给清晰提示而非 NoClassDefFoundError
    public static void saveChartSvg(final org.knowm.xchart.internal.chartpart.Chart<?, ?> chart, final String path)
    throws IOException {
        try {
            VectorGraphicsEncoder.saveVectorGraphic(chart, path, VectorGraphicsEncoder.VectorGraphicsFormat.SVG);
        }
        catch (final NoClassDefFoundError ex) {
            throw new IOException(
                "SVG 矢量导出需要 de.erichseifert:vectorgraphics2d 依赖，但它在 Maven Central 已下架。\n"
                + "如需 SVG，请手动从 https://github.com/eseifert/vectorgraphics2d 获取并加入 classpath；"
                + "或改用 saveChart(PNG) / saveChartJpg 等位图格式。", ex);
        }
    }

    // ───────────────────────── 图表加强·补齐 XChart 10 种图表（2026-08 新增 6 种） ─────────────────────────
    // 之前只暴露了 hist/box/scatter/pie 4 种；XChart 4.0.4 实际支持 10 种图表，
    // 以下补齐 bar/hbar/bubble/heatmap/ohlc/radar/dial，让 joinery 图表能力完整对齐 XChart。

    /**
     * ┌─ What : 柱状图（CategoryChart，对标 pandas df.plot.bar）
     * │  How  : 把指定数值列按 index 分组，每列一个 series
     */
    public static <V> CategoryChart bar(final DataFrame<V> df, final Object... columns) {
        final CategoryChart chart = new CategoryChartBuilder()
                .width(600).height(400)
                .title("柱状图").xAxisTitle("类别").yAxisTitle("数值")
                .build();
        final String font = cjkFontName();
        chart.getStyler().setChartTitleFont(new Font(font, Font.BOLD, 14));
        chart.getStyler().setLegendFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTitleFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTickLabelsFont(new Font(font, Font.PLAIN, 12));

        // x 轴分类：用 DataFrame 的 index
        final List<Object> idx = new ArrayList<>(df.index());
        final List<String> categories = new ArrayList<>(idx.size());
        for (final Object o : idx) {
            categories.add(String.valueOf(o));
        }

        final List<Object> cols = (columns == null || columns.length == 0)
                ? new ArrayList<>(df.columns()) : Arrays.asList(columns);
        for (final Object col : cols) {
            final List<Number> nums = new ArrayList<>();
            for (final V v : df.col(col)) {
                nums.add(v instanceof Number ? (Number) v : 0);
            }
            chart.addSeries(String.valueOf(col), categories, nums);
        }
        return chart;
    }

    /**
     * ┌─ What : 横向条形图（HorizontalBarChart，对标 pandas df.plot.barh）
     */
    public static <V> HorizontalBarChart hbar(final DataFrame<V> df, final Object... columns) {
        final HorizontalBarChart chart = new HorizontalBarChartBuilder()
                .width(600).height(400)
                .title("条形图").xAxisTitle("数值").yAxisTitle("类别")
                .build();
        final String font = cjkFontName();
        chart.getStyler().setChartTitleFont(new Font(font, Font.BOLD, 14));
        chart.getStyler().setLegendFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTitleFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTickLabelsFont(new Font(font, Font.PLAIN, 12));

        final List<Object> idx = new ArrayList<>(df.index());
        final List<String> categories = new ArrayList<>(idx.size());
        for (final Object o : idx) {
            categories.add(String.valueOf(o));
        }
        final List<Object> cols = (columns == null || columns.length == 0)
                ? new ArrayList<>(df.columns()) : Arrays.asList(columns);
        for (final Object col : cols) {
            final List<Number> nums = new ArrayList<>();
            for (final V v : df.col(col)) {
                nums.add(v instanceof Number ? (Number) v : 0);
            }
            // hbar 签名 addSeries(name, xData=数值, yData=分类) —— 横向条形图
            chart.addSeries(String.valueOf(col), nums, categories);
        }
        return chart;
    }

    /**
     * ┌─ What : 气泡图（BubbleChart，三维数据 x/y/bubble-size）
     * │  对标 pandas df.plot.scatter(s=size)
     */
    public static <V> BubbleChart bubble(final DataFrame<V> df, final Object xCol, final Object yCol, final Object sizeCol) {
        final BubbleChart chart = new BubbleChartBuilder()
                .width(600).height(400)
                .title(String.format("气泡图: %s / %s / %s", xCol, yCol, sizeCol))
                .xAxisTitle(String.valueOf(xCol)).yAxisTitle(String.valueOf(yCol))
                .build();
        final List<Number> xs = new ArrayList<>();
        final List<Number> ys = new ArrayList<>();
        final List<Number> ss = new ArrayList<>();
        final List<V> xdata = df.col(xCol), ydata = df.col(yCol), sdata = df.col(sizeCol);
        final int n = Math.min(Math.min(xdata.size(), ydata.size()), sdata.size());
        for (int i = 0; i < n; i++) {
            if (xdata.get(i) instanceof Number && ydata.get(i) instanceof Number && sdata.get(i) instanceof Number) {
                xs.add((Number) xdata.get(i));
                ys.add((Number) ydata.get(i));
                ss.add((Number) sdata.get(i));
            }
        }
        chart.addSeries(String.valueOf(yCol), xs, ys, ss);
        applyBubbleFont(chart, cjkFontName());
        return chart;
    }

    private static void applyBubbleFont(final BubbleChart chart, final String font) {
        chart.getStyler().setChartTitleFont(new Font(font, Font.BOLD, 14));
        chart.getStyler().setLegendFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTitleFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTickLabelsFont(new Font(font, Font.PLAIN, 12));
    }

    /**
     * ┌─ What : 热力图（HeatMapChart，二维密度/强度）
     * │  对标 seaborn.heatmap；xCols/yCols 是分类列，valueCol 是强度列
     * │  实现：把 (xCol, yCol) 二元组聚合到 valueCol 的和，作为热力值
     */
    public static <V> HeatMapChart heatmap(final DataFrame<V> df, final Object xCol, final Object yCol, final Object valueCol) {
        final HeatMapChart chart = new HeatMapChartBuilder()
                .width(600).height(400)
                .title("热力图").build();
        // 收集 x/y 的唯一值
        final List<String> xs = uniqueStrings(df.col(xCol));
        final List<String> ys = uniqueStrings(df.col(yCol));
        // 聚合：[yIdx][xIdx] = sum(value)
        final int[][] values = new int[ys.size()][xs.size()];
        final List<V> xv = df.col(xCol), yv = df.col(yCol), vv = df.col(valueCol);
        final int n = Math.min(Math.min(xv.size(), yv.size()), vv.size());
        for (int i = 0; i < n; i++) {
            int xi = xs.indexOf(String.valueOf(xv.get(i)));
            int yi = ys.indexOf(String.valueOf(yv.get(i)));
            if (xi >= 0 && yi >= 0 && vv.get(i) instanceof Number) {
                values[yi][xi] += ((Number) vv.get(i)).intValue();
            }
        }
        // HeatMapChart 的 x/y 需要 int[]（这里用索引）
        final int[] xindices = new int[xs.size()];
        final int[] yindices = new int[ys.size()];
        for (int i = 0; i < xs.size(); i++) xindices[i] = i;
        for (int i = 0; i < ys.size(); i++) yindices[i] = i;
        chart.addSeries("value", xindices, yindices, values);
        applyHeatFont(chart, cjkFontName());
        return chart;
    }

    private static void applyHeatFont(final HeatMapChart chart, final String font) {
        chart.getStyler().setChartTitleFont(new Font(font, Font.BOLD, 14));
        chart.getStyler().setAxisTitleFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTickLabelsFont(new Font(font, Font.PLAIN, 12));
    }

    private static List<String> uniqueStrings(final List<?> col) {
        final List<String> out = new ArrayList<>();
        for (final Object o : col) {
            final String s = String.valueOf(o);
            if (!out.contains(s)) out.add(s);
        }
        return out;
    }

    /**
     * ┌─ What : K 线图（OHLCChart，股票开盘/最高/最低/收盘）
     * │  对标 mplfinance；需提供 open/high/low/close 四列
     */
    public static <V> OHLCChart ohlc(final DataFrame<V> df, final Object openCol, final Object highCol, final Object lowCol, final Object closeCol) {
        final OHLCChart chart = new OHLCChartBuilder()
                .width(800).height(400).title("K线图").xAxisTitle("时间").yAxisTitle("价格").build();
        final float[] opens = toFloatArray(df.col(openCol));
        final float[] highs = toFloatArray(df.col(highCol));
        final float[] lows = toFloatArray(df.col(lowCol));
        final float[] closes = toFloatArray(df.col(closeCol));
        chart.addSeries("OHLC", highs, lows, opens, closes);
        applyOhlcFont(chart, cjkFontName());
        return chart;
    }

    private static float[] toFloatArray(final List<?> col) {
        final float[] out = new float[col.size()];
        for (int i = 0; i < col.size(); i++) {
            out[i] = col.get(i) instanceof Number ? ((Number) col.get(i)).floatValue() : 0f;
        }
        return out;
    }

    private static void applyOhlcFont(final OHLCChart chart, final String font) {
        chart.getStyler().setChartTitleFont(new Font(font, Font.BOLD, 14));
        chart.getStyler().setLegendFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTitleFont(new Font(font, Font.PLAIN, 12));
        chart.getStyler().setAxisTickLabelsFont(new Font(font, Font.PLAIN, 12));
    }

    /**
     * ┌─ What : 雷达图（RadarChart，多维能力对比）
     * │  对标 plotly 雷达图；variableNames 是各维度名，seriesData 是该行各维度数值
     */
    public static <V> RadarChart radar(final DataFrame<V> df, final Object rowName, final Object... valueCols) {
        final RadarChart chart = new RadarChartBuilder()
                .width(600).height(500).title("雷达图: " + rowName).build();
        final List<Object> cols = (valueCols == null || valueCols.length == 0)
                ? new ArrayList<>(df.columns()) : Arrays.asList(valueCols);
        final String[] varNames = new String[cols.size()];
        for (int i = 0; i < cols.size(); i++) varNames[i] = String.valueOf(cols.get(i));
        // 取指定行的数据
        final int rowIdx = df.index().contains(rowName)
                ? new ArrayList<>(df.index()).indexOf(rowName) : 0;
        final double[] data = new double[cols.size()];
        double maxVal = 1.0;   // 用于归一化（XChart RadarChart 要求数据 ∈ [0,1]）
        for (int c = 0; c < cols.size(); c++) {
            final V v = df.get(rowIdx, cols.get(c));
            final double d = v instanceof Number ? ((Number) v).doubleValue() : 0;
            data[c] = d;
            if (Math.abs(d) > maxVal) maxVal = Math.abs(d);
        }
        // XChart RadarChart 要求值 ∈ [0,1]：若原始数据 > 1，按最大值归一化（自动）
        if (maxVal > 1.0) {
            for (int c = 0; c < cols.size(); c++) {
                data[c] = data[c] / maxVal;
            }
        }
        // RadarChart.addSeries(name, double[]) 只接受数据；标签单独 setRadiiLabels
        chart.setRadiiLabels(varNames);
        chart.addSeries(String.valueOf(rowName), data);
        applyRadarFont(chart, cjkFontName());
        return chart;
    }

    private static void applyRadarFont(final RadarChart chart, final String font) {
        chart.getStyler().setChartTitleFont(new Font(font, Font.BOLD, 14));
        chart.getStyler().setLegendFont(new Font(font, Font.PLAIN, 12));
    }

    /**
     * ┌─ What : 仪表盘（DialChart，单值百分比展示）
     * │  How  : valueCol 取平均值；XChart DialChart 要求值在 [0,1] 范围内（百分比），
     * │         若原始数据 > 1 则按 [0,100] 比例缩放（自动归一化）
     */
    public static <V> DialChart dial(final DataFrame<V> df, final Object valueCol) {
        final DialChart chart = new DialChartBuilder()
                .width(500).height(400).title("仪表盘: " + valueCol).build();
        // 取该列平均值作为仪表盘读数
        double sum = 0; int count = 0;
        for (final V v : df.col(valueCol)) {
            if (v instanceof Number) { sum += ((Number) v).doubleValue(); count++; }
        }
        double avg = count > 0 ? sum / count : 0;
        // XChart DialChart 要求值 ∈ [0,1]：若 > 1（典型百分比 0~100），按 100 缩放归一化
        if (avg > 1.0) {
            avg = avg / 100.0;
        }
        // 边界保护：clamp 到 [0,1]
        avg = Math.max(0.0, Math.min(1.0, avg));
        chart.addSeries(String.valueOf(valueCol), avg);
        chart.getStyler().setChartTitleFont(new Font(cjkFontName(), Font.BOLD, 14));
        return chart;
    }

    // ───────────────────────── 以下为原 plot 工具方法（保留） ─────────────────────────

    private static final String title(final DataFrame<?> df) {
        return String.format(
                "%s (%d rows x %d columns)",
                df.getClass().getCanonicalName(),
                df.length(),
                df.size()
            );
    }

    private static final String dateFormat(final List<Object> xdata) {
        final int[] fields = new int[] {
                Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH,
                Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND
            };
        final String[] formats = new String[] {
                " yyy", "-MMM", "-d", " H", ":mm", ":ss"
            };
        final Calendar c1 = Calendar.getInstance(), c2 = Calendar.getInstance();

        if (!xdata.isEmpty() && xdata.get(0) instanceof Date) {
            String format = "";
            int first = 0, last = 0;
            c1.setTime(Date.class.cast(xdata.get(0)));
            // iterate over all x-axis values comparing dates
            for (int i = 1; i < xdata.size(); i++) {
                // early exit for non-date elements
                if (!(xdata.get(i) instanceof Date)) return formats[0].substring(1);
                c2.setTime(Date.class.cast(xdata.get(i)));

                // check which components differ, those are the fields to output
                for (int j = 1; j < fields.length; j++) {
                    if (c1.get(fields[j]) != c2.get(fields[j])) {
                        first = Math.max(j - 1, first);
                        last = Math.max(j, last);
                    }
                }
            }

            // construct a format string for the fields that differ
            for (int i = first; i <= last && i < formats.length; i++) {
                format += format.isEmpty() ? formats[i].substring(1) : formats[i];
            }

            return format;
        }

        return formats[0].substring(1);
    }

    private static void addTrend(final XYChart chart, final XYSeries series, final List<Object> xdata) {
        final SimpleRegression model = new SimpleRegression();
        // XChart 4.0.4 XYSeries 通过 getYData() 拿原始 y 数组（父类 AxesChartSeriesNumerical 提供）
        final double[] yData = series.getYData();
        for (int x = 0; x < yData.length; x++) {
            model.addData(x, yData[x]);
        }
        final Color mc = series.getMarkerColor();
        final Color c = new Color(mc.getRed(), mc.getGreen(), mc.getBlue(), 0x60);
        // 趋势线只用首尾两点
        final List<Integer> xs = Arrays.asList(0, xdata.size() - 1);
        final List<Double> ys = Arrays.asList(model.predict(0), model.predict(xdata.size() - 1));
        final XYSeries trend = chart.addSeries(series.getName() + " (trend)", xs, ys);
        trend.setLineColor(c);
        trend.setMarker(SeriesMarkers.NONE);
    }
}
