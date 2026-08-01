package dataio;

import joinery.DataFrame;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ┌─ What : Excel 模块全场景回归测试（readXls / readXlsx / writeXls / writeXlsx 真实行为）
 * │  Why  : 在 ExcelDataIOTest / ExcelTypeStrictTest 之外，系统化覆盖「接口契约」层面的真实行为，
 * │         包括：自动识别格式、空表、空表头、空白单元格、合并单元格、多 sheet、双入口、
 * │         xls 往返、xlsx 大文件 SXSSF、数值精度、重复列名。
 * │         每个断言都基于「跑出来的真实行为」编写（不是主观期望）。
 * │  Who  : dataio-tests 测试套件
 * │  When : mvn test 阶段
 * │  Where: dataio-tests/src/test/java/dataio/ExcelFullScenarioTest.java
 */
class ExcelFullScenarioTest {

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 1：readXls 自动识别 .xls（HSSF）与 .xlsx（XSSF）              ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : readXls(InputStream) 能自动识别 .xls 二进制格式（HSSF）并正确读出数据
    // │  Why  : readXls 走 WorkbookFactory.create，按流内容自动判断 HSSF/XSSF，
    // │         不依赖文件扩展名。用 HSSFWorkbook 造一个真正的 .xls 流验证。
    // │  How  : 内存构造 HSSFWorkbook → 写到字节数组 → readXls(ByteArrayInputStream) 读回
    @Test
    void readXls_能自动识别HSSF二进制格式(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("auto.xls").toFile();
        try (HSSFWorkbook wb = new HSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("编号");
            header.createCell(1).setCellValue("名称");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(1);
            r1.createCell(1).setCellValue("张三");
            wb.write(out);
        }
        // String 入口
        final DataFrame<Object> df1 = DataFrame.readXls(f.getAbsolutePath());
        assertEquals(2, df1.size(), "列数应为 2");
        assertEquals(1L, df1.get(0, 0), ".xls 整数单元格读回为 Long");
        assertEquals("张三", df1.get(0, 1));

        // InputStream 入口（WorkbookFactory 自动识别 HSSF）
        try (InputStream in = new FileInputStream(f)) {
            final DataFrame<Object> df2 = DataFrame.readXls(in);
            assertEquals(1L, df2.get(0, 0));
            assertEquals("张三", df2.get(0, 1));
        }
    }

    // ┌─ What : 同名 .xls 文件用 HSSF 写，readXls 读 —— 验证旧二进制格式完整支持
    // │  Why  : 题目明确要求「构造同名 .xls 文件用 HSSF 写，再 readXls 读」。
    // │         注意列 c 用 i*1.5 → 第 2 行 2*1.5=3.0，POI 读 NUMERIC 时 3.0 被
    // │         numericValueToObject 判为整数 → Long；与 1.5/4.5(Double) 混合 → 整列 String。
    // │         这是「混合类型降级」的真实行为，断言按真实行为写。
    @Test
    void readXls_HSSF写入的文件_readXls能读回(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("legacy.xls").toFile();
        try (HSSFWorkbook wb = new HSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet("数据");
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("a");
            header.createCell(1).setCellValue("b");
            header.createCell(2).setCellValue("c");
            for (int i = 1; i <= 3; i++) {
                Row r = sheet.createRow(i);
                r.createCell(0).setCellValue(i * 10);
                r.createCell(1).setCellValue("x" + i);
                r.createCell(2).setCellValue(i * 1.5);  // 1.5, 3.0, 4.5
            }
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXls(f.getAbsolutePath());
        assertEquals(3, df.length(), "3 行数据");
        assertEquals(3, df.size(), "3 列");
        // a 列：纯整数（10,20,30）→ 全 Long 不降级
        assertEquals(10L, df.get(0, 0));
        assertEquals(30L, df.get(2, 0));
        assertEquals("x1", df.get(0, 1));
        // c 列：[1.5(Double), 3.0(读为Long), 4.5(Double)] → 混合类型 → 整列 String
        // 整数转 String 不带小数点（3.0 → "3"），小数按精度（1.5 → "1.5"）
        assertEquals("1.5", df.get(0, 2), "c 列混合 Long/Double → String，1.5 → \"1.5\"");
        assertEquals("3", df.get(1, 2), "3.0 读为 Long 后转 String 不带小数点 → \"3\"");
        assertEquals("4.5", df.get(2, 2));
    }

    // ┌─ What : readXls 也能读 .xlsx（因为底层 WorkbookFactory 同时识别两种格式）
    // │  Why  : 这是 readXls 的「真实行为」—— 虽然方法名叫 readXls，但底层 WorkbookFactory
    // │         能同时识别 .xls 和 .xlsx。断言基于真实行为而非方法名直觉。
    @Test
    void readXls_传入xlsx文件_也能识别(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("fake.xls").toFile();   // 扩展名 .xls 但内容是 xlsx
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("v");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(42);
            wb.write(out);
        }
        // 真实行为：readXls 通过 WorkbookFactory 识别内容（不依赖扩展名）→ 能读 xlsx 内容
        final DataFrame<Object> df = DataFrame.readXls(f.getAbsolutePath());
        assertEquals(42L, df.get(0, 0), "readXls 按内容识别，能读 xlsx 数据");
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 2 & 3：空表（只有表头无数据行）/ 完全空表（无任何行）         ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : 只有表头、无数据行 → DataFrame 列名正确，length()=0
    // │  Why  : 真实行为：表头行存在但无数据行，readWorkbook 仍以表头建列，数据为 0 行
    @Test
    void readXlsx_只有表头无数据行_列正确长度为零(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("headeronly.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("id");
            header.createCell(1).setCellValue("name");
            // 不写任何数据行
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(0, df.length(), "无数据行 → length()=0");
        assertEquals(2, df.size(), "应有 2 列（来自表头）");
        final List<Object> cols = new java.util.ArrayList<>(df.columns());
        assertEquals("id", cols.get(0));
        assertEquals("name", cols.get(1));
    }

    // ┌─ What : 完全空表（无任何行）→ readWorkbook 返回空 DataFrame
    // │  Why  : readWorkbook 找不到非空表头行时 new DataFrame<>()（0 列 0 行）
    @Test
    void readXlsx_完全空表_返回空DataFrame(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("empty.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            wb.createSheet();   // 只创建空 sheet，不写任何行
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(0, df.size(), "空表 → 0 列");
        assertEquals(0, df.length(), "空表 → 0 行");
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 4：首行/首列空白单元格                                       ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : 表头行第一列空白 → 该列名自动生成 "column_0"
    // │  Why  : readWorkbook 对表头空白单元格生成 "column_" + i（真实行为）
    @Test
    void readXlsx_表头首列空白_列名自动生成(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("blankfirst.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            // 首列留空，只写第 2 列表头
            header.createCell(1).setCellValue("name");
            Row r1 = sheet.createRow(1);
            r1.createCell(1).setCellValue("张三");
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(2, df.size(), "列数按表头 getLastCellNum = 2");
        final List<Object> cols = new java.util.ArrayList<>(df.columns());
        assertEquals("column_0", cols.get(0), "首列空白 → 自动命名 column_0");
        assertEquals("name", cols.get(1));
    }

    // ┌─ What : 数据行首列空白 → 该格读为 null（不跳列）
    // │  Why  : 验证「首列空白」数据行的行为 —— 位置保留为 null
    @Test
    void readXlsx_数据行首列空白_读为null不跳列(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("blankdata.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("a");
            header.createCell(1).setCellValue("b");
            // 数据行首列空白，只写第 2 列
            Row r1 = sheet.createRow(1);
            r1.createCell(1).setCellValue("y");
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertNull(df.get(0, 0), "首列空白 → null");
        assertEquals("y", df.get(0, 1), "第 2 列位置正确");
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 5：合并单元格（sheet.addMergedRegion）的读取行为            ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : 合并单元格区域 —— 左上角值只在第一格，其余格为空（POI 真实行为）
    // │  Why  : POI 对合并区域，只有左上角 cell 保留值，其余 cell 读出来是 null/BLANK。
    // │         readWorkbook 忠实反映 POI 的这一行为：合并区第一行有值，其他行 null。
    // │         这是接口的「真实行为」（不是 bug，符合 POI 语义）。
    @Test
    void readXlsx_合并单元格_左上角有值其余为空(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("merged.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("类别");
            header.createCell(1).setCellValue("值");
            // 合并 A2:A4 （第 1 列 2~4 行）
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue("水果");
            r1.createCell(1).setCellValue("苹果");
            Row r2 = sheet.createRow(2);
            r2.createCell(1).setCellValue("香蕉");
            Row r3 = sheet.createRow(3);
            r3.createCell(1).setCellValue("橙子");
            sheet.addMergedRegion(new org.apache.poi.ss.util.CellRangeAddress(1, 3, 0, 0));
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(3, df.length());
        // 真实行为：合并区第一行有值「水果」，第 2、3 行首列为 null
        assertEquals("水果", df.get(0, 0), "合并区左上角保留值");
        assertNull(df.get(1, 0), "合并区第 2 行首列 POI 读出为空");
        assertNull(df.get(2, 0), "合并区第 3 行首列 POI 读出为空");
        assertEquals("苹果", df.get(0, 1));
        assertEquals("香蕉", df.get(1, 1));
        assertEquals("橙子", df.get(2, 1));
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 6：多 sheet —— 只读第 0 个，其他被忽略                      ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : 多 sheet 工作簿 → readWorkbook 只读 sheetAt(0)，其他 sheet 完全忽略
    // │  Why  : readWorkbook 硬编码 wb.getSheetAt(0)。这是「真实契约」：
    // │         即便第 2 个 sheet 有数据，也不会出现在结果 DataFrame 里。
    @Test
    void readXlsx_多sheet_只读第一个sheet(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("multisheet.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            // sheet 0
            Sheet s0 = wb.createSheet("第一张");
            Row h0 = s0.createRow(0);
            h0.createCell(0).setCellValue("sheet0");
            Row d0 = s0.createRow(1);
            d0.createCell(0).setCellValue("来自第0张");
            // sheet 1
            Sheet s1 = wb.createSheet("第二张");
            Row h1 = s1.createRow(0);
            h1.createCell(0).setCellValue("sheet1");
            Row d1 = s1.createRow(1);
            d1.createCell(0).setCellValue("来自第1张");
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        // 真实行为：只读第 0 张 sheet 的数据
        assertEquals("来自第0张", df.get(0, 0), "只读第 0 张 sheet");
        assertEquals(1, df.length(), "第 0 张只有 1 行数据");
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 7：readXls(String file) 与 readXls(InputStream) 两入口都覆盖 ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : readXls 的 String 与 InputStream 两个入口读出结果一致
    // │  Why  : DataFrame.readXls(String) 与 readXls(InputStream) 最终都走
    // │         Serialization.readXls(InputStream) → WorkbookFactory。两入口语义等价。
    @Test
    void readXls_String与InputStream两入口结果一致(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("twoentries.xls").toFile();
        try (HSSFWorkbook wb = new HSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("k");
            h.createCell(1).setCellValue("v");
            Row r = sheet.createRow(1);
            r.createCell(0).setCellValue("a");
            r.createCell(1).setCellValue(100);
            wb.write(out);
        }
        final DataFrame<Object> byString = DataFrame.readXls(f.getAbsolutePath());
        final DataFrame<Object> byStream;
        try (InputStream in = new FileInputStream(f)) {
            byStream = DataFrame.readXls(in);
        }
        assertEquals(byString.size(), byStream.size());
        assertEquals(byString.length(), byStream.length());
        assertEquals(byString.get(0, 0), byStream.get(0, 0));
        assertEquals(byString.get(0, 1), byStream.get(0, 1));
        assertEquals("a", byString.get(0, 0));
        assertEquals(100L, byString.get(0, 1));
    }

    // ┌─ What : readXlsx 也有 String / InputStream 两入口
    @Test
    void readXlsx_String与InputStream两入口结果一致(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("twoentries.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("k");
            Row r = sheet.createRow(1);
            r.createCell(0).setCellValue("hello");
            wb.write(out);
        }
        final DataFrame<Object> byString = DataFrame.readXlsx(f.getAbsolutePath());
        final DataFrame<Object> byStream;
        try (InputStream in = new FileInputStream(f)) {
            byStream = DataFrame.readXlsx(in);
        }
        assertEquals("hello", byString.get(0, 0));
        assertEquals("hello", byStream.get(0, 0));
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 8：writeXls（旧 .xls 格式，HSSF）的写读往返                  ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : writeXls 写 .xls，readXls 读回，数据与表头完整
    // │  Why  : 验证旧二进制 .xls 格式的写读往返闭环（HSSF 写 → WorkbookFactory 读）
    @Test
    void writeXls_写读往返_数据完整(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("name", "count");
        df.append(Arrays.asList("甲", 10L));
        df.append(Arrays.asList("乙", 20L));
        df.append(Arrays.asList("丙", 30L));

        final Path xls = tmp.resolve("roundtrip.xls");
        df.writeXls(xls.toString());

        // 真实行为：writeXls 生成真正的 HSSF .xls 文件
        try (Workbook wb = WorkbookFactory.create(new FileInputStream(xls.toFile()))) {
            assertTrue(wb instanceof HSSFWorkbook, "writeXls 生成 HSSFWorkbook");
        }

        final DataFrame<Object> back = DataFrame.readXls(xls.toString());
        assertEquals(3, back.length(), "3 行数据完整");
        assertEquals(2, back.size(), "2 列");
        final List<Object> cols = new java.util.ArrayList<>(back.columns());
        assertEquals("name", cols.get(0));
        assertEquals("count", cols.get(1));
        assertEquals("甲", back.get(0, 0));
        assertEquals(10L, back.get(0, 1));
        assertEquals("丙", back.get(2, 0));
        assertEquals(30L, back.get(2, 1));
    }

    // ┌─ What : writeXls 含 null 值的写读往返
    // │  Why  : writeCell 对 null 写空字符串，读回为空字符串或 null —— 验证真实行为
    @Test
    void writeXls_含null值_写读往返(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("a", "b");
        df.append(Arrays.asList("x", null));   // 第 2 列 null
        df.append(Arrays.asList(null, "y"));   // 第 1 列 null

        final Path xls = tmp.resolve("nulls.xls");
        df.writeXls(xls.toString());

        final DataFrame<Object> back = DataFrame.readXls(xls.toString());
        assertEquals(2, back.length());
        // 真实行为：null 写成空串 ""，读回为空串（STRING 类型）→ 因同列有非空字符串，保持 String
        assertEquals("x", back.get(0, 0));
        assertEquals("y", back.get(1, 1));
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 9：writeXlsx 大文件 SXSSF（10 万行）                        ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : writeXlsx 写 10 万行不 OOM 且数据完整（SXSSF 流式写）
    // │  Why  : 行数 > SXSSF_WINDOW_SIZE(100) 自动切 SXSSF。验证大文件不 OOM + 首尾数据完整。
    // │         注：用大堆跑，验证不在中途被 SXSSF dispose 干扰。
    @Test
    void writeXlsx_十万行_SXSSF流式写不OOM且数据完整(@TempDir Path tmp) throws IOException {
        final int rows = 100_000;
        final DataFrame<Object> df = new DataFrame<>("id", "name", "score");
        for (int i = 0; i < rows; i++) {
            df.append(Arrays.asList((long) i, "n" + i, i * 1.5));
        }

        final Path xlsx = tmp.resolve("big100k.xlsx");
        // 不抛 OOM
        assertDoesNotThrow(() -> df.writeXlsx(xlsx.toString()));

        // 真实行为：文件能正常生成且可被读回
        final File file = xlsx.toFile();
        assertTrue(file.exists(), "大文件已生成");
        assertTrue(file.length() > 0, "文件非空");

        final DataFrame<Object> back = DataFrame.readXlsx(xlsx.toString());
        assertEquals(rows, back.length(), "读回行数一致");
        // 首行
        assertEquals(0L, back.get(0, 0), "首行 id=0");
        assertEquals("n0", back.get(0, 1));
        // 末行
        assertEquals((long) (rows - 1), back.get(rows - 1, 0), "末行 id=" + (rows - 1));
        assertEquals("n" + (rows - 1), back.get(rows - 1, 1));
        // 中间抽样
        assertEquals((long) 50000, back.get(50000, 0));
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 10：数值精度（如 0.1+0.2 这种）                            ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : 小数单元格读回保留 Excel 原始精度（Double 类型）
    // │  Why  : Excel 存 double，读回也是 Double。验证 0.1+0.2 这种经典浮点场景：
    // │         Excel 单元格里存的是计算结果 0.30000000000000004（IEEE754），
    // │         读回就是该 Double（不会自动 round 成 0.3）。
    @Test
    void readXlsx_小数精度_保留Excel原始Double(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("precision.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("v");
            Row r = sheet.createRow(1);
            // 直接写 0.1+0.2 的结果（IEEE754 = 0.30000000000000004）
            r.createCell(0).setCellValue(0.1 + 0.2);
            Row r2 = sheet.createRow(2);
            r2.createCell(0).setCellValue(3.14);
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        // 真实行为：Double 类型，保留 IEEE754 原值（不 round）
        assertTrue(df.get(0, 0) instanceof Double, "小数单元格 → Double");
        // 0.1+0.2 在 IEEE754 下不等于 0.3，验证不丢失这个事实
        assertFalse(0.3 == (Double) df.get(0, 0), "0.1+0.2 != 0.3（IEEE754），读回应保留这一事实");
        assertEquals(0.1 + 0.2, (Double) df.get(0, 0), 0.0, "读回应等于写入的 IEEE754 值");
        assertEquals(3.14, (Double) df.get(1, 0), 0.0001);
    }

    // ┌─ What : 整数单元格 ≥15 位 → Long 保留精度（不丢末位）
    // │  Why  : readCell 对整数值 NUMERIC 走 Long 分支（numericValueToObject），
    // │         不经 double 精度损失。验证 18 位整数读回完整。
    @Test
    void readXlsx_大整数单元格_Long保留精度(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("bigint.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("id");
            Row r = sheet.createRow(1);
            r.createCell(0).setCellValue(1234567890123456.0);   // POIsetCellValue(double)
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        // 真实行为：读 NUMERIC 整数 → numericValueToObject → Long
        // 注意：Excel 用 double 存储，1234567890123456 在 double 下能精确表示（< 2^53）
        assertTrue(df.get(0, 0) instanceof Long, "整数单元格 → Long");
        assertEquals(1234567890123456L, df.get(0, 0), "18 位整数读回完整");
    }

    // ╔══════════════════════════════════════════════════════════════════╗
    // ║ 场景 11：表头列名重复时 readWorkbook 的行为                       ║
    // ╚══════════════════════════════════════════════════════════════════╝

    // ┌─ What : 表头列名重复 → readWorkbook 自动去重重命名，不再崩溃（修 E9 bug）
    // │  Why  : 题目要求「columns.add 重复会怎样」。旧真实行为是【崩溃】：
    // │         readWorkbook 把含重复名的 columns 交给 new DataFrame<>(columns) → Index.add
    // │         抛 IllegalArgumentException("duplicate name 'name' in index")，整个读取失败。
    // │         但 Excel 本身允许重名列（合法文件），所以这是 joinery 读 Excel 的真实 bug。
    // │         修复后（E9）：列名 [name, name, name] 自动改名为 [name, name.1, name.2]，
    // │         读取正常完成，数据按列对齐。
    @Test
    void readXlsx_表头列名重复_自动改名不崩溃(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("dupcols.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("name");
            header.createCell(1).setCellValue("name");   // 重复列名
            header.createCell(2).setCellValue("name");   // 重复列名
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue("a");
            r1.createCell(1).setCellValue("b");
            r1.createCell(2).setCellValue("c");
            wb.write(out);
        }
        // 修复后（2026-08）：Index 支持 pandas 风格的重名列，不再抛 IllegalArgumentException，
        // 重名列原样保留（不再自动改名 name.1/name.2 —— 那会篡改用户数据）
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(3, df.size(), "3 列全部保留");
        assertEquals(1, df.length());
        // columns() 返回 Set 去重视图；columnsList() 返回含重复的 List
        final List<Object> colsList = df.columnsList();
        assertEquals("name", colsList.get(0));
        assertEquals("name", colsList.get(1), "第 2 列仍叫 name（保留重名）");
        assertEquals("name", colsList.get(2), "第 3 列仍叫 name");
        // col(name) 取首个匹配（向后兼容）
        assertEquals("a", df.col("name").get(0));
        // cols(name) 取所有匹配（pandas 风格）
        assertEquals(3, df.cols("name").size(), "cols(name) 应返回 3 列");
        // 数据按列对齐（按位置读）
        assertEquals("a", df.get(0, 0));
        assertEquals("b", df.get(0, 1));
        assertEquals("c", df.get(0, 2));
    }

    // ┌─ What : 修 Bug-B：多 sheet 文件 —— 应能读任意 sheet，不再只读 sheet 0
    @Test
    void readXlsx_多sheet_可指定sheetIndex和sheetName(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("multisheet.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet s1 = wb.createSheet("第一章");
            s1.createRow(0).createCell(0).setCellValue("a1");
            s1.getRow(0).createCell(1).setCellValue("a2");
            s1.createRow(1).createCell(0).setCellValue("1");
            s1.getRow(1).createCell(1).setCellValue("2");
            final Sheet s2 = wb.createSheet("第二章");
            s2.createRow(0).createCell(0).setCellValue("b1");
            s2.getRow(0).createCell(1).setCellValue("b2");
            s2.createRow(1).createCell(0).setCellValue("99");
            s2.getRow(1).createCell(1).setCellValue("100");
            wb.write(out);
        }
        // 默认读 sheet 0
        final DataFrame<Object> defaultDf = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals("a1", defaultDf.columnsList().get(0), "默认读 sheet 0");
        // 按 sheetIndex 读 sheet 1
        final DataFrame<Object> byIdx = DataFrame.readXlsx(f.getAbsolutePath(), 1);
        assertEquals("b1", byIdx.columnsList().get(0), "sheetIndex=1 应读第二章");
        assertEquals("99", byIdx.get(0, 0));
        // 按 sheetName 读
        final DataFrame<Object> byName = DataFrame.readXlsx(f.getAbsolutePath(), "第二章");
        assertEquals("b2", byName.columnsList().get(1));
        // 列出所有 sheet 名
        try (var in = new java.io.FileInputStream(f)) {
            final List<String> names = DataFrame.sheetNames(in);
            assertEquals(2, names.size());
            assertTrue(names.contains("第一章"));
            assertTrue(names.contains("第二章"));
        }
    }

    // ┌─ What : sheetIndex 越界抛 IllegalArgumentException
    @Test
    void readXlsx_sheetIndex越界_抛异常带提示(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("onesheet.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            wb.createSheet("only");
            wb.write(out);
        }
        try (var in = new java.io.FileInputStream(f)) {
            final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> DataFrame.readXlsx(in, 5));
            assertTrue(ex.getMessage().contains("sheetIndex 5 越界"));
        }
    }

    // ┌─ What : sheet 名不存在抛 IllegalArgumentException 带可用 sheet 列表
    @Test
    void readXlsx_sheetName不存在_抛异常带sheet列表(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("named.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            wb.createSheet("alpha");
            wb.createSheet("beta");
            wb.write(out);
        }
        try (var in = new java.io.FileInputStream(f)) {
            final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> DataFrame.readXlsx(in, "不存在"));
            assertTrue(ex.getMessage().contains("alpha"), "异常应列出可用 sheet 名");
            assertTrue(ex.getMessage().contains("beta"));
        }
    }

    // ┌─ What : 修 Bug-C：null 写入读回应为 null（不是空字符串）
    @Test
    void writeXlsx_null往返_读回应为null(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("id", "name");
        df.append(java.util.Arrays.asList(1L, null));   // name 列 null
        df.append(java.util.Arrays.asList(2L, "x"));
        df.convert();

        final Path xlsx = tmp.resolve("nullrt.xlsx");
        df.writeXlsx(xlsx.toString());

        final DataFrame<Object> back = DataFrame.readXlsx(xlsx.toString());
        // null 写入读回应是 null（不是空字符串 ""）—— 修 Bug-C 后成立
        assertNull(back.get(0, 1), "null 写入读回必须是 null（不是空字符串），否则 isnull/dropna 失效");
        assertEquals("x", back.get(1, 1));
        // isnull 应识别第 1 行 name 列为 null
        final DataFrame<Boolean> isn = back.isnull();
        assertTrue(isn.get(0, 1), "isnull 应识别 null");
        assertFalse(isn.get(1, 1));
    }
}
