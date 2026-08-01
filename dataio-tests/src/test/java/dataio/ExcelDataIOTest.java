package dataio;

import joinery.DataFrame;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : Excel 读写测试（XLS / XLSX），重点验证 poi-excel skill §10「数据错位铁律」修复
 * │  Why  : 复现并验证核心 bug —— 空单元格跳列 / 列错位 / 行尾空单元格截断 / 空行虚高
 * │  Who  : dataio-tests 测试套件
 * │  When : mvn test 阶段
 * │  Where: dataio-tests/src/test/java/dataio/ExcelDataIOTest.java
 *
 *  参考文档：poi-excel skill §10 与 pitfalls.md
 */
class ExcelDataIOTest {

    // ┌─ What : 构造一个含「中间空单元格」的 xlsx 文件
    // │  Why  : 复现 skill §10 经典错位场景：[姓名, 年龄, 邮箱, 电话] 表头下，
    // │         某行 [李四, , , 13900000000] —— 旧实现会把 13900000000 当成「年龄」
    // │  How  : 用 XSSFWorkbook 直接构造文件（不走 joinery 写，避免循环依赖）
    private File makeSparseXlsx(Path tmp) throws IOException {
        final File f = tmp.resolve("sparse.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            // 表头：姓名 | 年龄 | 邮箱 | 电话
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("姓名");
            header.createCell(1).setCellValue("年龄");
            header.createCell(2).setCellValue("邮箱");
            header.createCell(3).setCellValue("电话");
            // 数据行 0：完整 4 列
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue("张三");
            r1.createCell(1).setCellValue(25);
            r1.createCell(2).setCellValue("zhang@xx.com");
            r1.createCell(3).setCellValue("13800000000");
            // 数据行 1：中间 2 列空（年龄、邮箱为空），电话保留
            // ❌ 旧实现 bug：迭代器只返回 [李四, 13900000000] → 13900000000 错位到第 2 列（年龄）
            // ✅ 修复后：[李四, null, null, 13900000000] → 电话正确对应第 4 列
            Row r2 = sheet.createRow(2);
            r2.createCell(0).setCellValue("李四");
            // 不 createCell(1) / createCell(2) —— 模拟空单元格
            r2.createCell(3).setCellValue("13900000000");
            wb.write(out);
        }
        return f;
    }

    // ┌─ What : 验证空单元格不再跳列（核心回归断言）
    @Test
    void readXlsx空单元格不跳列_位置严格对应(@TempDir Path tmp) throws IOException {
        final File f = makeSparseXlsx(tmp);
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());

        // 表头：4 列
        assertEquals(4, df.size(), "应有 4 列");

        // 第 0 行（张三）完整
        assertEquals("张三", df.get(0, 0));
        // 注：setCellValue(25) 写的是整数，按新契约「Excel 整数单元格 → Long」，读回是 25L
        assertEquals(25L, df.get(0, 1));
        assertEquals("zhang@xx.com", df.get(0, 2));
        assertEquals("13800000000", String.valueOf(df.get(0, 3)));

        // 第 1 行（李四）：年龄、邮箱必须是 null（位置保留），电话必须在第 4 列
        assertEquals("李四", df.get(1, 0), "李四应在第 1 列");
        assertNull(df.get(1, 1), "年龄应为 null（位置保留，不跳列）");
        assertNull(df.get(1, 2), "邮箱应为 null（位置保留，不跳列）");
        assertEquals("13900000000", String.valueOf(df.get(1, 3)), "电话必须正确对应第 4 列 —— 不跳列的核心断言");
    }

    // ┌─ What : 验证行尾空单元格不被截断（getLastCellNum 比表头小的场景）
    @Test
    void readXlsx行尾空单元格_补齐到表头宽度(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("trailing.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("A");
            header.createCell(1).setCellValue("B");
            header.createCell(2).setCellValue("C");
            header.createCell(3).setCellValue("D");
            // 只填前 2 列，后 2 列为空（getLastCellNum=2 < 表头 4）
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue("a1");
            r1.createCell(1).setCellValue("b1");
            wb.write(out);
        }

        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(4, df.size(), "应按表头宽度 4 列");
        assertEquals("a1", df.get(0, 0));
        assertEquals("b1", df.get(0, 1));
        assertNull(df.get(0, 2), "C 列应补 null");
        assertNull(df.get(0, 3), "D 列应补 null");
    }

    // ┌─ What : 验证空行虚高不再带出全是 null 的废行
    @Test
    void readXlsx中间空行_应跳过(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("emptyrows.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("姓名");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue("张三");
            // 第 2 行：createRow 但不写值（POI 会标记该行存在但全空 → getLastRowNum 虚高）
            sheet.createRow(2);
            Row r3 = sheet.createRow(3);
            r3.createCell(0).setCellValue("王五");
            wb.write(out);
        }

        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(2, df.length(), "应只读到 2 行有效数据（空行被跳过），实际: " + df.length());
        assertEquals("张三", df.get(0, 0));
        assertEquals("王五", df.get(1, 0));
    }

    // ┌─ What : 公式单元格应读到求值结果（不是公式文本）
    @Test
    void readXlsx公式单元格_应读到求值结果(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("formula.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("单价");
            header.createCell(1).setCellValue("数量");
            header.createCell(2).setCellValue("小计");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(10.0);
            r1.createCell(1).setCellValue(100);
            r1.createCell(2).setCellFormula("A2*B2");
            // 写时触发求值，把结果缓存到 <v>（否则 readXls 拿不到值）
            wb.getCreationHelper().createFormulaEvaluator().evaluateAll();
            wb.write(out);
        }

        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        // 小计列应是数字 1000.0，不是 "=A2*B2" 字符串
        final Object v = df.get(0, 2);
        assertTrue(v instanceof Number, "公式列应读到求值后的数字，实际类型: " + (v == null ? "null" : v.getClass()));
        assertEquals(1000.0, ((Number) v).doubleValue(), 0.001, "A2*B2 = 10*100 = 1000");
    }

    // ┌─ What : 【硬性回归】无 <v> 缓存的公式 → 读入必须是计算后的结果值
    // │  Why  : 用户核心要求「Excel 公式列读入到 DataFrame 时要传入的计算后的结果」。
    // │         这里构造「写时不调 evaluateAll」（<v> 为空）的 xlsx，验证 joinery 仍能现场求值返回 1000.0
    @Test
    void readXlsx无缓存公式_仍能现场求值返回结果(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("nocache_formula.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("单价");
            header.createCell(1).setCellValue("数量");
            header.createCell(2).setCellValue("小计");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(10.0);
            r1.createCell(1).setCellValue(100);
            r1.createCell(2).setCellFormula("A2*B2");
            // ★ 故意不调 evaluateAll —— 文件里 <v> 为空，靠 readXlsx 现场求值
            wb.write(out);
        }

        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        final Object v = df.get(0, 2);
        // 必须是数值（求值后的结果），不是 "=A2*B2" 公式串、不是 null
        assertNotNull(v, "公式列读入不应为 null");
        assertTrue(v instanceof Number,
            "【硬性要求】无缓存公式列读入必须是计算后的数值，实际: "
                + v.getClass() + " = " + v);
        assertEquals(1000.0, ((Number) v).doubleValue(), 0.001,
            "A2*B2 现场求值应为 10*100 = 1000");
    }

    // ┌─ What : SUM/AVERAGE 等聚合公式读入即聚合结果
    @Test
    void readXlsx聚合公式_SUM_AVERAGE_读入即结果(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("agg_formula.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("值");
            header.createCell(1).setCellValue("求和");
            header.createCell(2).setCellValue("平均");
            // 3 行数据
            for (int i = 1; i <= 3; i++) {
                Row r = sheet.createRow(i);
                r.createCell(0).setCellValue(i * 10);   // 10, 20, 30
            }
            // SUM 和 AVERAGE 公式
            Row r4 = sheet.createRow(4);
            r4.createCell(1).setCellFormula("SUM(A2:A4)");   // 60
            r4.createCell(2).setCellFormula("AVERAGE(A2:A4)"); // 20
            wb.write(out);   // 不调 evaluateAll，靠读时求值
        }

        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        // 第 4 行（数据索引 4，含 SUM/AVERAGE）的求和列应是 60.0
        final Object sum = df.get(3, 1);   // 数据行 0~3，第 4 行索引 3
        final Object avg = df.get(3, 2);
        assertTrue(sum instanceof Number, "SUM 公式读入应为数值");
        assertEquals(60.0, ((Number) sum).doubleValue(), 0.001, "SUM(10,20,30) = 60");
        assertTrue(avg instanceof Number, "AVERAGE 公式读入应为数值");
        assertEquals(20.0, ((Number) avg).doubleValue(), 0.001, "AVERAGE(10,20,30) = 20");
    }

    // ┌─ What : 验证中文表头读写不乱码
    @Test
    void writeReadXlsx_中文表头数据完整(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("姓名", "城市", "金额");
        df.append(java.util.Arrays.asList("张三", "北京", 1234.56));
        df.append(java.util.Arrays.asList("李四", "上海", 7890.12));
        df.convert();

        final Path xlsx = tmp.resolve("中文.xlsx");
        df.writeXlsx(xlsx.toString());

        final DataFrame<Object> back = DataFrame.readXlsx(xlsx.toString());
        final java.util.List<Object> cols = new java.util.ArrayList<>(back.columns());
        assertEquals("姓名", cols.get(0));
        assertEquals("城市", cols.get(1));
        assertEquals("金额", cols.get(2));
        assertEquals("张三", back.get(0, 0));
        assertEquals("北京", back.get(0, 1));
    }

    // ┌─ What : 验证 SXSSF 大文件写不 OOM 且数据完整（10 万行）
    // │  Why  : 修 E3 超大 Excel 写 OOM；行数 > windowSize 自动切 SXSSF 流式写
    @Test
    void writeXlsx大文件_SXSSF流式写_数据完整(@TempDir Path tmp) throws IOException {
        final int rows = 100_000;
        final DataFrame<Object> df = new DataFrame<>("id", "value");
        for (int i = 0; i < rows; i++) {
            df.append(java.util.Arrays.asList((long) i, i * 1.5));
        }

        final Path xlsx = tmp.resolve("large.xlsx");
        // 不应抛 OOM
        assertDoesNotThrow(() -> df.writeXlsx(xlsx.toString()));

        // 读回首尾验证完整性
        final DataFrame<Object> back = DataFrame.readXlsx(xlsx.toString());
        assertEquals(rows, back.length(), "读回行数应一致");
        assertEquals(0.0, ((Number) back.get(0, 0)).doubleValue(), 0.001, "首行 id=0");
        assertEquals((double)(rows - 1), ((Number) back.get(rows - 1, 0)).doubleValue(), 0.001, "末行 id=" + (rows - 1));
    }

    // ┌─ What : Boolean 写入再读出不应丢值（修 E5）
    @Test
    void writeXlsx_Boolean值不丢失(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("flag", "name");
        df.append(java.util.Arrays.asList(true, "a"));
        df.append(java.util.Arrays.asList(false, "b"));
        df.convert();

        final Path xlsx = tmp.resolve("bool.xlsx");
        df.writeXlsx(xlsx.toString());

        final DataFrame<Object> back = DataFrame.readXlsx(xlsx.toString());
        assertEquals(Boolean.TRUE, back.get(0, 0), "第 1 行 flag 应为 true");
        assertEquals(Boolean.FALSE, back.get(1, 0), "第 2 行 flag 应为 false");
    }

    // ┌─ What : 长整数（≥15 位 Long）应按字符串保留精度（修 E6，身份证/银行卡场景）
    // │  Why  : 长整数是真实场景（不是泛指「长数字」）：身份证 18 位、银行卡 16~19 位、订单号等
    // │         都是 Long 类型。原 writeCell 把 Long 也 doubleValue() → 末尾几位变 0。
    // │         新逻辑只对 Long 类型做保护，Double 不处理（Double 本身就是浮点，无「精度保护」语义）
    @Test
    void writeXlsx长整数_按字符串写不丢精度(@TempDir Path tmp) throws IOException {
        final long idCardLong = 110101199001011234L;   // 18 位 Long（身份证）
        final DataFrame<Object> df = new DataFrame<>("身份证号");
        df.append(java.util.Collections.singletonList(idCardLong));

        final Path xlsx = tmp.resolve("longint.xlsx");
        df.writeXlsx(xlsx.toString());

        // 直接读 POI 看类型 & 数值
        try (XSSFWorkbook wb = new XSSFWorkbook(new java.io.FileInputStream(xlsx.toFile()))) {
            final Cell cell = wb.getSheetAt(0).getRow(1).getCell(0);
            // 修复后：18 位 Long 应是 STRING 类型（避免 double 精度丢失末尾几位）
            assertEquals(CellType.STRING, cell.getCellType(),
                "18 位 Long 应按 STRING 写入（避免 double 精度丢末尾）");
            assertEquals("110101199001011234", cell.getStringCellValue(),
                "Long 值原样写入字符串，末尾不为 0");
        }
    }

    // ┌─ What : 普通小整数（< 15 位 Long）仍按数值写（NUMERIC）
    // │  Why  : 不能矫枉过正 —— 短整数（如年龄、数量、id）仍应按数值写，否则 Excel 无法做 SUM/AVG
    @Test
    void writeXlsx短整数_仍按数值写(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("数量");
        df.append(java.util.Collections.singletonList(42L));   // 短整数 Long

        final Path xlsx = tmp.resolve("shortint.xlsx");
        df.writeXlsx(xlsx.toString());

        try (XSSFWorkbook wb = new XSSFWorkbook(new java.io.FileInputStream(xlsx.toFile()))) {
            final Cell cell = wb.getSheetAt(0).getRow(1).getCell(0);
            // 短整数应保持 NUMERIC（Excel 可参与计算）
            assertEquals(CellType.NUMERIC, cell.getCellType(),
                "短整数 Long 应按 NUMERIC 写（Excel 才能参与 SUM/AVG）");
            assertEquals(42.0, cell.getNumericCellValue(), 0.001);
        }
    }

    // ┌─ What : Double 类型不做长整数保护（Double 本身是浮点，没有「整数精度」语义）
    @Test
    void writeXlsx_Double类型_按数值写不走字符串分支(@TempDir Path tmp) throws IOException {
        final DataFrame<Object> df = new DataFrame<>("金额");
        df.append(java.util.Collections.singletonList(1234.56));   // Double

        final Path xlsx = tmp.resolve("double.xlsx");
        df.writeXlsx(xlsx.toString());

        try (XSSFWorkbook wb = new XSSFWorkbook(new java.io.FileInputStream(xlsx.toFile()))) {
            final Cell cell = wb.getSheetAt(0).getRow(1).getCell(0);
            assertEquals(CellType.NUMERIC, cell.getCellType(),
                "Double 应按 NUMERIC 写（非 Long 不走长整数保护）");
            assertEquals(1234.56, cell.getNumericCellValue(), 0.001);
        }
    }

    // ┌─ What : XLS（旧 .xls 格式）也能正确读
    @Test
    void readXls旧格式_可正常读取(@TempDir Path tmp) throws IOException {
        // 复用 joinery 自带的 serialization.xls
        final String resourcePath = "/serialization.xls";
        try (var in = getClass().getResourceAsStream(resourcePath)) {
            if (in == null) {
                // 资源不存在时跳过（不报失败）
                System.out.println("跳过：未找到资源 " + resourcePath);
                return;
            }
            final DataFrame<Object> df = DataFrame.readXls(in);
            // 表头：category/name/value 三列
            assertEquals(3, df.size());
            assertEquals("alpha", df.get(0, 1));
        }
    }
}
