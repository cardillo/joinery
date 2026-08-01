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
 * ┌─ What : Excel 单元格类型严格保留策略的回归测试（硬性要求）
 * │  Why  : 用户明确要求「Excel 单元格类型到 DataFrame 必须严格按 Excel 类型，不能自动匹配」：
 * │           - 整型单元格 → Long（保留精度，不丢位）
 * │           - 小数单元格 → Double（按 Excel 有效位数）
 * │           - 日期单元格 → Date（标准格式）
 * │           - 一列之中有多个类型 → 整列 String（整数完整、小数按位、日期标准格式）
 * │  Who  : dataio-tests 测试套件
 * │  When : mvn test 阶段
 * │  Where: dataio-tests/src/test/java/dataio/ExcelTypeStrictTest.java
 */
class ExcelTypeStrictTest {

    // ┌─ What : 整数单元格 → Long（不丢精度）
    @Test
    void readXlsx整数单元格_应转为Long(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("ints.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("id");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(100);          // 整数
            Row r2 = sheet.createRow(2);
            r2.createCell(0).setCellValue(9999999999L);  // 大整数（超 int 范围）
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(Long.class, df.get(0, 0).getClass(), "100 应为 Long");
        assertEquals(100L, df.get(0, 0));
        assertEquals(Long.class, df.get(1, 0).getClass(), "9999999999 应为 Long（保留精度）");
        assertEquals(9999999999L, df.get(1, 0), "大整数不应丢精度");
    }

    // ┌─ What : 小数单元格 → Double
    @Test
    void readXlsx小数单元格_应转为Double(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("doubles.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("price");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(3.14);   // 小数
            Row r2 = sheet.createRow(2);
            r2.createCell(0).setCellValue(2.5);    // 小数
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(Double.class, df.get(0, 0).getClass(), "3.14 应为 Double");
        assertEquals(3.14, (Double) df.get(0, 0), 0.001);
        assertEquals(Double.class, df.get(1, 0).getClass(), "2.5 应为 Double");
    }

    // ┌─ What : 日期单元格 → Date
    @Test
    void readXlsx日期单元格_应转为Date(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("dates.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("date");
            // 日期格式单元格
            CellStyle dateStyle = wb.createCellStyle();
            dateStyle.setDataFormat(wb.getCreationHelper().createDataFormat().getFormat("yyyy-MM-dd"));
            Row r1 = sheet.createRow(1);
            Cell dc = r1.createCell(0);
            dc.setCellValue(new Date());
            dc.setCellStyle(dateStyle);
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(Date.class, df.get(0, 0).getClass(), "日期单元格应为 Date");
    }

    // ┌─ What : 一列混合整数与小数 → 整列 String（保留语义）
    // │  Why  : 同列既有整数又有小数，类型不一致，按用户要求整列转字符串；
    // │         整数不带小数点（100 → "100"）、小数按精度（3.14 → "3.14"）
    @Test
    void readXlsx混合整数小数列_整列转为String(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("mixed.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("val");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(100);    // 整数
            Row r2 = sheet.createRow(2);
            r2.createCell(0).setCellValue(3.14);   // 小数
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        // 同列既有 Long(100) 又有 Double(3.14) → 混合类型 → 整列转 String
        assertEquals(String.class, df.get(0, 0).getClass(), "混合列第 1 行应为 String");
        assertEquals(String.class, df.get(1, 0).getClass(), "混合列第 2 行应为 String");
        assertEquals("100", df.get(0, 0), "整数转 String 不带小数点");
        assertEquals("3.14", df.get(1, 0), "小数按精度");
    }

    // ┌─ What : 一列混合数字与字符串 → 整列 String
    @Test
    void readXlsx混合数字字符串列_整列转为String(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("mixed2.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("note");
            Row r1 = sheet.createRow(1);
            r1.createCell(0).setCellValue(42);         // 数字
            Row r2 = sheet.createRow(2);
            r2.createCell(0).setCellValue("N/A");      // 字符串
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        assertEquals(String.class, df.get(0, 0).getClass());
        assertEquals("42", df.get(0, 0));
        assertEquals("N/A", df.get(1, 0));
    }

    // ┌─ What : 单一类型列（全整数）→ 不降级，保持 Long
    @Test
    void readXlsx全整数列_保持Long不降级(@TempDir Path tmp) throws IOException {
        final File f = tmp.resolve("allints.xlsx").toFile();
        try (XSSFWorkbook wb = new XSSFWorkbook();
             FileOutputStream out = new FileOutputStream(f)) {
            final Sheet sheet = wb.createSheet();
            Row h = sheet.createRow(0);
            h.createCell(0).setCellValue("count");
            for (int i = 1; i <= 3; i++) {
                sheet.createRow(i).createCell(0).setCellValue(i * 10);   // 10, 20, 30
            }
            wb.write(out);
        }
        final DataFrame<Object> df = DataFrame.readXlsx(f.getAbsolutePath());
        for (int r = 0; r < 3; r++) {
            assertEquals(Long.class, df.get(r, 0).getClass(),
                "全整数列不应降级，行 " + r + " 应保持 Long");
        }
    }
}
