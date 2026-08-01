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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.URL;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.supercsv.cellprocessor.ConvertNullTo;
import org.supercsv.cellprocessor.FmtDate;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import joinery.DataFrame;
import joinery.DataFrame.NumberDefault;

public class Serialization {

    private static final String EMPTY_DF_STRING = "[empty data frame]";
    private static final String ELLIPSES = "...";
    private static final String NEWLINE = "\n";
    private static final String DELIMITER = "\t";
    private static final Object INDEX_KEY = new Object();
    private static final int    MAX_COLUMN_WIDTH = 20;

    // ┌─ What : Excel 大文件/流式写参数与长整数精度阈值
    // │  Why  : 参见 poi-excel skill § pitfalls #1（≥15 位长整数末尾变 0）与「大数据量 SXSSF」节；
    // │         SXSSF 默认窗口 100 行已足够大多数场景，超过即自动刷盘避免 OOM。
    // │  Who  : writeXlsx / writeCell 使用
    // │  When : 写 Excel 时
    // │  Where: 本类常量
    // │  How  : writeXlsx 用 SXSSFWorkbook(SXSSF_WINDOW_SIZE)；writeCell 对位数 >= LONG_INTEGER_THRESHOLD 的长整数（Long）按字符串写
    private static final int SXSSF_WINDOW_SIZE = 100;
    private static final int LONG_INTEGER_THRESHOLD = 15;

    public static String toString(final DataFrame<?> df, final int limit) {
        final int len = df.length();

        if (len == 0) {
            return EMPTY_DF_STRING;
        }

        final StringBuilder sb = new StringBuilder();
        final Map<Object, Integer> width = new HashMap<>();
        final List<Class<?>> types = df.types();
        final List<Object> columns = new ArrayList<>(df.columns());

        // determine index width
        width.put(INDEX_KEY, 0);
        for (final Object row : df.index()) {
            Class<? extends Object> rowClass = row == null ? null : row.getClass();
            width.put(INDEX_KEY, clamp(
                    width.get(INDEX_KEY),
                    MAX_COLUMN_WIDTH,
                    fmt(rowClass, row).length()));
        }

        // determine column widths
        for (int c = 0; c < columns.size(); c++) {
            final Object column = columns.get(c);
            width.put(column, String.valueOf(column).length());
            for (int r = 0; r < df.length(); r++) {
                width.put(column, clamp(
                        width.get(column),
                        MAX_COLUMN_WIDTH,
                        fmt(types.get(c), df.get(r, c)).length()));
            }
        }

        // output column names
        sb.append(lpad("", width.get(INDEX_KEY)));
        for (int c = 0; c < columns.size(); c++) {
            sb.append(DELIMITER);
            final Object column = columns.get(c);
            sb.append(lpad(column, width.get(column)));
        }
        sb.append(NEWLINE);

        // output rows
        final Iterator<Object> names = df.index().iterator();
        for (int r = 0; r < len; r++) {
            // output row name
            int w = width.get(INDEX_KEY);
            final Object row = names.hasNext() ? names.next() : r;
            Class<? extends Object> rowClass = row == null ? null : row.getClass();
            sb.append(truncate(lpad(fmt(rowClass, row), w), w));

            // output rows
            for (int c = 0; c < df.size(); c++) {
                sb.append(DELIMITER);
                final Class<?> cls = types.get(c);
                w = width.get(columns.get(c));
                if (Number.class.isAssignableFrom(cls)) {
                    sb.append(lpad(fmt(cls, df.get(r, c)), w));
                } else {
                    sb.append(truncate(rpad(fmt(cls, df.get(r, c)), w), w));
                }
            }
            sb.append(NEWLINE);

            // skip rows if necessary to limit output
            if (limit - 3 < r && r < (limit << 1) && r < len - 4) {
                sb.append(NEWLINE).append(ELLIPSES)
                  .append(" ").append(len - limit)
                  .append(" rows skipped ").append(ELLIPSES)
                  .append(NEWLINE).append(NEWLINE);
                while (r < len - 2) {
                    if (names.hasNext()) {
                        names.next();
                    }
                    r++;
                }
            }
        }

        return sb.toString();
    }

    private static final int clamp(final int lower, final int upper, final int value) {
        return Math.max(lower, Math.min(upper, value));
    }

    private static final String lpad(final Object o, final int w) {
        final StringBuilder sb = new StringBuilder();
        final String value = String.valueOf(o);
        for (int i = value.length(); i < w; i++) {
            sb.append(' ');
        }
        sb.append(value);
        return sb.toString();
    }

    private static final String rpad(final Object o, final int w) {
        final StringBuilder sb = new StringBuilder();
        final String value = String.valueOf(o);
        sb.append(value);
        for (int i = value.length(); i < w; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    private static final String truncate(final Object o, final int w) {
        final String value = String.valueOf(o);
        return value.length() - ELLIPSES.length() > w ? value.substring(0, w - ELLIPSES.length()) + ELLIPSES : value;
    }

    private static final String fmt(final Class<?> cls, final Object o) {
        if(cls==null) return "null";
        String s;
        if (o instanceof Number) {
            if (Short.class.equals(cls) || Integer.class.equals(cls) ||
                    Long.class.equals(cls) || BigInteger.class.equals(cls)) {
                s = String.format("% d", Number.class.cast(o).longValue());
            } else {
                s = String.format("% .8f", Number.class.cast(o).doubleValue());
            }
        } else if (o instanceof Date) {
            final Date dt = Date.class.cast(o);
            final Calendar cal = Calendar.getInstance();
            cal.setTime(dt);
            final DateFormat fmt = new SimpleDateFormat(
                    cal.get(Calendar.HOUR_OF_DAY) == 0 &&
                        cal.get(Calendar.MINUTE) == 0 &&
                        cal.get(Calendar.SECOND) == 0 ?
                    "yyyy-MM-dd" : "yyyy-MM-dd'T'HH:mm:ssXXX"
                );
            s = fmt.format(dt);
        } else {
            s = o != null ? String.valueOf(o) : "";
        }
        return s;
    }

    public static DataFrame<Object> readCsv(final String file)
    throws IOException {
        return readCsv(file.contains("://") ?
                new URL(file).openStream() : new FileInputStream(file), ",", NumberDefault.LONG_DEFAULT, null);
    }

    public static DataFrame<Object> readCsv(final String file, final String separator, NumberDefault numDefault)
    throws IOException {
        return readCsv(file.contains("://") ?
                new URL(file).openStream() : new FileInputStream(file), separator, numDefault, null);
    }

    public static DataFrame<Object> readCsv(final String file, final String separator, NumberDefault numDefault, final String naString)
    throws IOException {
        return readCsv(file.contains("://") ?
                new URL(file).openStream() : new FileInputStream(file), separator, numDefault, naString);
    }

    public static DataFrame<Object> readCsv(final String file, final String separator, NumberDefault numDefault, final String naString, boolean hasHeader)
    throws IOException {
        return readCsv(file.contains("://") ?
                new URL(file).openStream() : new FileInputStream(file), separator, numDefault, naString, hasHeader);
    }

    public static DataFrame<Object> readCsv(final InputStream input) 
    throws IOException {
        return readCsv(input, ",", NumberDefault.LONG_DEFAULT, null);
    }

    public static DataFrame<Object> readCsv(final InputStream input, String separator, NumberDefault numDefault, String naString) 
    throws IOException {
    	return readCsv(input,separator, numDefault,naString, true);
    }

    public static DataFrame<Object> readCsv(final InputStream input, String separator, NumberDefault numDefault, String naString, boolean hasHeader)
    throws IOException {
        CsvPreference csvPreference;
        switch (separator) {
            case "\\t":
                csvPreference = CsvPreference.TAB_PREFERENCE;
                break;
            case ",":
                csvPreference = CsvPreference.STANDARD_PREFERENCE;
                break;
            case ";":
                csvPreference = CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE;
                break;
            case "|":
            	csvPreference  = new CsvPreference.Builder('"', '|', "\n").build();
                break;
            default:
                throw new IllegalArgumentException("Separator: " + separator + " is not currently supported");
        }
        // ┌─ What : 中文 CSV 支持：自动剥离 UTF-8 BOM + 显式 UTF-8 解码
        // │  Why  : Excel/Windows 中文环境导出的 CSV 多带 UTF-8 BOM（EF BB BF）或为 GBK；
        // │         旧实现裸用 new InputStreamReader(input) 走平台默认编码（Linux 下常为 UTF-8，
        // │         Windows 下为 GBK/Cp1252），且不去 BOM，导致首列列名前混入 BOM 字符 + 中文乱码
        // │  Who  : readCsv 所有重载最终走这里
        // │  When : 读任意 CSV 时
        // │  Where: 本类 readCsv
        // │  How  : 用 BOMInputStream 包装自动剥 BOM，再以 UTF-8 解码；GBK 文件由调用方先用流转换
        try (CsvListReader reader = new CsvListReader(
                new InputStreamReader(stripBom(input), java.nio.charset.StandardCharsets.UTF_8),
                csvPreference)) {
        	final List<String> header;
        	final DataFrame<Object> df;
        	final CellProcessor[] procs;
        	if(hasHeader) {
        		header = Arrays.asList(reader.getHeader(true));
        		procs = new CellProcessor[header.size()];
                df = new DataFrame<>(header);
        	} else {
        		// Read the first row to figure out how many columns we have
        		reader.read();
        		header = new ArrayList<String>();
        		for (int i = 0; i < reader.length(); i++) {
					header.add("V"+i);
				}
        		procs = new CellProcessor[header.size()];
        		df = new DataFrame<>(header);
        		// The following line executes the procs on the previously read row again
        		df.append(reader.executeProcessors(procs));
        	}
            for (List<Object> row = reader.read(procs); row != null; row = reader.read(procs)) {
                df.append(new ArrayList<>(row));
            }
            return df.convert(numDefault, naString);
        }
    }

    // ┌─ What : 剥离 UTF-8 BOM（如有）
    // │  Why  : 不引入 commons-io 依赖；CSV 读取前检测并跳过开头 3 字节 EF BB BF
    // │  Who  : readCsv 调用
    // │  When : 读 CSV 时
    // │  Where: 本类私有 stripBom
    // │  How  : 用 PushbackInputStream（支持 unread）读取前 3 字节检测，命中 BOM 则吞掉，
    // │         未命中则 unread 回退。**绝不用 mark/reset**——很多 InputStream
    // │         （如 Files.newInputStream 的 ChannelInputStream）不支持 mark/reset 会抛 IOException
    private static InputStream stripBom(final InputStream input) throws IOException {
        // PushbackInputStream 缓冲区 3 字节，足够回退 BOM
        final java.io.PushbackInputStream pb = new java.io.PushbackInputStream(input, 3);
        final byte[] head = new byte[3];
        final int read = pb.read(head);
        // BOM = EF BB BF
        final boolean isBom = read == 3
                && (head[0] & 0xFF) == 0xEF
                && (head[1] & 0xFF) == 0xBB
                && (head[2] & 0xFF) == 0xBF;
        if (isBom) {
            return pb;   // 已消费 BOM，剩余流交给 InputStreamReader
        }
        // 无 BOM：把读到的字节全部回退
        if (read > 0) {
            pb.unread(head, 0, read);
        }
        return pb;
    }

    public static <V> void writeCsv(final DataFrame<V> df, final String output)
    throws IOException {
        writeCsv(df, new FileOutputStream(output));
    }

    // ┌─ What : 写 CSV（UTF-8 + BOM，中文 Excel 打开不乱码）
    // │  Why  : Excel 在中文 Windows 下打开无 BOM 的 UTF-8 CSV 会把中文显示成乱码；
    // │         写入前先写 UTF-8 BOM（EF BB BF），让 Excel 正确识别编码（中文完美支持）
    // │  Who  : 由 DataFrame.writeCsv 调用
    // │  When : 写 CSV 时
    // │  Where: 本类 writeCsv
    // │  How  : 先 output.write(UTF-8 BOM)，再用 UTF-8 OutputStreamWriter 写内容
    public static <V> void writeCsv(final DataFrame<V> df, final OutputStream output)
    throws IOException {
        // 写 UTF-8 BOM 头（中文 Excel 兼容）
        output.write(new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF });
        try (CsvListWriter writer = new CsvListWriter(
                new OutputStreamWriter(output, java.nio.charset.StandardCharsets.UTF_8),
                CsvPreference.STANDARD_PREFERENCE)) {
            final String[] header = new String[df.size()];
            final Iterator<Object> it = df.columns().iterator();
            for (int c = 0; c < df.size(); c++) {
                header[c] = String.valueOf(it.hasNext() ? it.next() : c);
            }
            writer.writeHeader(header);
            final CellProcessor[] procs = new CellProcessor[df.size()];
            final List<Class<?>> types = df.types();
            for (int c = 0; c < df.size(); c++) {
                final Class<?> cls = types.get(c);
                if (Date.class.isAssignableFrom(cls)) {
                    procs[c] = new ConvertNullTo("", new FmtDate("yyyy-MM-dd'T'HH:mm:ssXXX"));
                } else {
                    procs[c] = new ConvertNullTo("");
                }
            }
            for (final List<V> row : df) {
                writer.write(row, procs);
            }
        }
    }

    public static DataFrame<Object> readXls(final String file)
    throws IOException {
        return readXls(file.contains("://") ?
                    new URL(file).openStream() : new FileInputStream(file));
    }

    /** 按路径 + sheetIndex 读 Excel（修 Bug-B） */
    public static DataFrame<Object> readXls(final String file, final int sheetIndex)
    throws IOException {
        return readXls(file.contains("://") ?
                    new URL(file).openStream() : new FileInputStream(file), sheetIndex);
    }

    /** 按路径 + sheetName 读 Excel（修 Bug-B） */
    public static DataFrame<Object> readXls(final String file, final String sheetName)
    throws IOException {
        return readXls(file.contains("://") ?
                    new URL(file).openStream() : new FileInputStream(file), sheetName);
    }

    /** 按路径列出所有 sheet 名 */
    public static List<String> sheetNames(final String file)
    throws IOException {
        return sheetNames(file.contains("://") ?
                new URL(file).openStream() : new FileInputStream(file));
    }

    // ┌─ What : 读取 Excel（.xls 与 .xlsx 自动识别）为 DataFrame
    // │  Why  : 旧实现硬编码 new HSSFWorkbook 只支持 .xls；用 WorkbookFactory.create 可按输入流自动识别两种格式
    // │  Who  : 由 DataFrame.readXls 调用
    // │  When : 用户读取 Excel 文件时
    // │  Where: 本类 readXls
    // │  How  : WorkbookFactory.create(input) → 抽公共 readWorkbook(wb, 0) 走统一逻辑
    public static DataFrame<Object> readXls(final InputStream input)
    throws IOException {
        return readXls(input, 0);
    }

    // ┌─ What : 读取 Excel 指定 sheet（修 Bug-B：旧实现硬编码 getSheetAt(0) 丢失其他 sheet 数据）
    // │  Why  : 真实 Excel 文件常有多个 sheet（年报各章节、各月份数据等），旧实现只读第 0 个 sheet
    // │         直接丢弃其他 sheet 数据 —— 严重的数据丢失 bug。pandas.read_excel 提供 sheet_name 参数
    // │         可指定读哪个 sheet，本方法对齐之
    // │  Who  : 由 DataFrame.readXls(file/input, sheetIndex) 调用
    // │  When : 用户需要读非首张 sheet 时
    public static DataFrame<Object> readXls(final InputStream input, final int sheetIndex)
    throws IOException {
        final Workbook wb = WorkbookFactory.create(input);
        try {
            return readWorkbook(wb, sheetIndex);
        }
        finally {
            try { wb.close(); } catch (final IOException ignored) { }
        }
    }

    // ┌─ What : 按 sheet 名读 Excel（修 Bug-B）
    public static DataFrame<Object> readXls(final InputStream input, final String sheetName)
    throws IOException {
        final Workbook wb = WorkbookFactory.create(input);
        try {
            final int idx = wb.getSheetIndex(sheetName);
            if (idx < 0) {
                throw new IllegalArgumentException(
                    "sheet '" + sheetName + "' 不存在，可用 sheet: " + sheetNamesOf(wb));
            }
            return readWorkbook(wb, idx);
        }
        finally {
            try { wb.close(); } catch (final IOException ignored) { }
        }
    }

    // ┌─ What : 列出所有 sheet 名（给用户选 sheet 时参考）
    public static List<String> sheetNames(final InputStream input)
    throws IOException {
        final Workbook wb = WorkbookFactory.create(input);
        try {
            return sheetNamesOf(wb);
        }
        finally {
            try { wb.close(); } catch (final IOException ignored) { }
        }
    }

    private static List<String> sheetNamesOf(final Workbook wb) {
        final List<String> names = new ArrayList<>(wb.getNumberOfSheets());
        for (int i = 0; i < wb.getNumberOfSheets(); i++) {
            names.add(wb.getSheetName(i));
        }
        return names;
    }

    // ┌─ What : 仅读取 .xlsx（XSSF）为 DataFrame
    // │  Why  : 显式入口，便于上层按扩展名分流；走 WorkbookFactory 同一逻辑
    // │  Who  : 由 DataFrame.readXlsx 调用
    // │  When : 用户明确读 .xlsx 时
    // │  Where: 本类 readXlsx
    public static DataFrame<Object> readXlsx(final String file)
    throws IOException {
        return readXlsx(file.contains("://") ?
                new URL(file).openStream() : new FileInputStream(file));
    }

    /** 按路径 + sheetIndex 读 .xlsx（修 Bug-B） */
    public static DataFrame<Object> readXlsx(final String file, final int sheetIndex)
    throws IOException {
        return readXlsx(file.contains("://") ?
                new URL(file).openStream() : new FileInputStream(file), sheetIndex);
    }

    /** 按路径 + sheetName 读 .xlsx（修 Bug-B） */
    public static DataFrame<Object> readXlsx(final String file, final String sheetName)
    throws IOException {
        return readXlsx(file.contains("://") ?
                new URL(file).openStream() : new FileInputStream(file), sheetName);
    }

    public static DataFrame<Object> readXlsx(final InputStream input)
    throws IOException {
        return readXlsx(input, 0);
    }

    // ┌─ What : 读 .xlsx 指定 sheet（修 Bug-B：旧实现只读 sheet 0）
    public static DataFrame<Object> readXlsx(final InputStream input, final int sheetIndex)
    throws IOException {
        final Workbook wb = new XSSFWorkbook(input);
        return readWorkbook(wb, sheetIndex);
    }

    // ┌─ What : 读 .xlsx 按 sheet 名（修 Bug-B）
    public static DataFrame<Object> readXlsx(final InputStream input, final String sheetName)
    throws IOException {
        final Workbook wb = new XSSFWorkbook(input);
        final int idx = wb.getSheetIndex(sheetName);
        if (idx < 0) {
            throw new IllegalArgumentException(
                "sheet '" + sheetName + "' 不存在，可用 sheet: " + sheetNamesOf(wb));
        }
        return readWorkbook(wb, idx);
    }

    // ┌─ What : 从 Workbook 第一张表读出 DataFrame（修 E1/E4/E7/E8 共 4 个 bug）
    // │  Why  : 参见 poi-excel skill §10「数据错位铁律」与 pitfalls #3/#7/#8。核心修复：
    // │           - E1 跳列：旧实现按每行 getLastCellNum() 遍历，行尾空单元格被截断导致列错位；
    // │             改为「以表头宽度 headerWidth 为权威列数」，所有数据行统一补齐到该宽度
    // │           - E7 空行虚高：旧用 for-each 迭代器信任 getLastRowNum，会带出全是 null 的废行；
    // │             改为索引遍历 + isRowEmpty() 跳过空行（遵循 skill §10 行遍历铁律）
    // │           - 列用索引遍历 row.getCell(i)，空位补 null（遵循 skill §10 列遍历铁律，禁用 for-each）
    // │  Who  : readXls / readXlsx 调用
    // │  When : 读取任意 Excel 时
    // │  Where: 本类私有 readWorkbook
    // │  How  : 伪代码见下方
    // 伪代码：
    //   1. sheet = wb.getSheetAt(0)
    //   2. 找表头行（第一行非空），headerWidth = headerRow.getLastCellNum()
    //   3. 收集表头列名（空单元格列名生成 "column_N"）
    //   4. 从表头下一行起按索引 r 遍历到 sheet.getLastRowNum()：
    //        a. 若该行 isRowEmpty → 跳过（避免空行虚高）
    //        b. 否则按 i = 0..headerWidth-1 取 row.getCell(i)：
    //             - null 或 BLANK → null
    //             - 否则 readCell(cell, evaluator)
    //        c. 行数据 append 到 data
    //   5. new DataFrame(columns)，逐行 append，最后 convert() 推断类型
    private static DataFrame<Object> readWorkbook(final Workbook wb, final int sheetIndex) {
        // sheetIndex 边界检查（防越界，给清晰提示）
        if (sheetIndex < 0 || sheetIndex >= wb.getNumberOfSheets()) {
            throw new IllegalArgumentException(
                "sheetIndex " + sheetIndex + " 越界，workbook 共 " + wb.getNumberOfSheets() + " 张 sheet");
        }
        final Sheet sheet = wb.getSheetAt(sheetIndex);
        final FormulaEvaluator evaluator = wb.getCreationHelper().createFormulaEvaluator();

        // —— 第 1 步：定位表头行（首个非空行）——
        int headerRowNum = -1;
        Row headerRow = null;
        for (int r = 0; r <= sheet.getLastRowNum(); r++) {
            final Row row = sheet.getRow(r);
            if (!isRowEmpty(row)) {
                headerRowNum = r;
                headerRow = row;
                break;
            }
        }
        // 空表 → 返回空 DataFrame
        if (headerRow == null) {
            return new DataFrame<>();
        }

        // —— 第 2 步：表头宽度为权威列数（修 E1 跳列 bug 的关键）——
        final int headerWidth = headerRow.getLastCellNum();   // getLastCellNum 返回 short，含末列+1
        final List<Object> columns = new ArrayList<>(headerWidth);
        for (int i = 0; i < headerWidth; i++) {
            final Cell cell = headerRow.getCell(i);
            final Object name = (cell == null || cell.getCellType() == CellType.BLANK)
                    ? "column_" + i
                    : readCell(cell, evaluator);
            columns.add(name);
        }

        // 注：Index 已支持重名列（pandas 风格，修 E9 架构 bug），不再需要 dedup。
        // Excel 表头若含重名列（如两个「值」列），原样保留；调用方可用 df.cols("值") 拿所有匹配列，
        // 或 df.col("值") 拿首个匹配列（向后兼容）。


        // —— 第 3 步：数据行按索引遍历，跳过空行（修 E7 空行虚高）——
        final DataFrame<Object> df = new DataFrame<>(columns);
        for (int r = headerRowNum + 1; r <= sheet.getLastRowNum(); r++) {
            final Row row = sheet.getRow(r);
            if (isRowEmpty(row)) {
                continue;   // 跳过用户编辑过的空行
            }
            final List<Object> values = new ArrayList<>(headerWidth);
            // —— 按索引取列，空位补 null（遵循 skill §10 列遍历铁律）——
            for (int i = 0; i < headerWidth; i++) {
                final Cell cell = row == null ? null : row.getCell(i);
                if (cell == null || cell.getCellType() == CellType.BLANK) {
                    values.add(null);
                }
                else {
                    values.add(readCell(cell, evaluator));
                }
            }
            df.append(values);
        }

        // —— 第 4 步：列级混合类型降级为 String ——
        // 【硬性要求】一列之中有多个类型时，采用字符型，把整数完整转换、小数按 Excel 有效位数转换、
        // 日期按标准格式转换。这一步保证同列数据可比较、可显示，不破坏单格语义。
        applyMixedTypeColumnToString(df);

        return df;
    }

    // ┌─ What : 列级混合类型降级 —— 同列出现多种非 null Java 类型时，整列转 String
    // │  Why  : 用户硬性要求「一列中有多个类型时，采用字符型」。规则：
    // │           - Long 与 Double 同列 → 都是数值，但语义不一（整数 vs 小数）→ 转 String
    // │           - 数值与 String 同列 → 转 String
    // │           - Boolean 与其他同列 → 转 String
    // │           - Date 与其他同列 → 转 String
    // │         转换规则（保留单格语义完整性）：
    // │           - Long → 原数字字符串（如 100 → "100"，不显示小数点）
    // │           - Double → 原值字符串（如 3.14 → "3.14"，按 Excel 有效位数）
    // │           - Date → 标准 ISO 格式 "yyyy-MM-dd HH:mm:ss"
    // │           - Boolean → "true"/"false"
    // │           - 其他 → String.valueOf
    // │  How  : 伪代码：
    //            1. 对每列扫描所有非 null 值，收集 distinct 类型集合
    //            2. 若类型集合 size > 1 → 该列需降级：遍历每行，非 null 值按上述规则转 String
    //            3. Long 同列（无 Double）不降级；Double 同列（无 Long）不降级
    private static void applyMixedTypeColumnToString(final DataFrame<Object> df) {
        final int cols = df.size();
        final int rows = df.length();
        if (cols == 0 || rows == 0) {
            return;
        }
        for (int c = 0; c < cols; c++) {
            // —— 扫描该列所有非 null 值的 distinct 类型 ——
            final java.util.Set<Class<?>> types = new java.util.HashSet<>();
            for (int r = 0; r < rows; r++) {
                final Object v = df.get(r, c);
                if (v != null) {
                    types.add(v.getClass());
                }
            }
            // —— 类型统一（单一类型）→ 不降级 ——
            if (types.size() <= 1) {
                continue;
            }
            // —— 混合类型：整列转 String（保留单格语义）——
            for (int r = 0; r < rows; r++) {
                final Object v = df.get(r, c);
                df.set(r, c, v == null ? null : toMixedTypeString(v));
            }
        }
    }

    // ┌─ What : 把单格值按「保留语义」规则转 String
    // │  How  : Long → 不带小数点的整数字符串（避免 100 变 "100.0"）
    // │         Double → String.valueOf（按 Excel 原始精度）
    // │         Date → ISO "yyyy-MM-dd HH:mm:ss"（无时间部分则 "yyyy-MM-dd"）
    // │         Boolean → "true"/"false"
    private static String toMixedTypeString(final Object v) {
        if (v instanceof Long) {
            return String.valueOf(v);   // Long → "100"，不带小数点
        }
        if (v instanceof Double) {
            return String.valueOf(v);   // Double → "3.14"
        }
        if (v instanceof Date) {
            return formatExcelDate((Date) v);   // 日期标准格式
        }
        if (v instanceof Boolean) {
            return String.valueOf(v);
        }
        return String.valueOf(v);
    }

    // ┌─ What : Date → 标准字符串（无时间部分 → "yyyy-MM-dd"；有 → "yyyy-MM-dd HH:mm:ss"）
    private static String formatExcelDate(final Date date) {
        final java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(date);
        final boolean hasTime = cal.get(java.util.Calendar.HOUR_OF_DAY) != 0
                || cal.get(java.util.Calendar.MINUTE) != 0
                || cal.get(java.util.Calendar.SECOND) != 0;
        final java.text.DateFormat fmt = hasTime
                ? new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                : new java.text.SimpleDateFormat("yyyy-MM-dd");
        return fmt.format(date);
    }

    // ┌─ What : 判断一行是否全空（所有单元格都是 null/BLANK/空白）
    // │  Why  : poi-excel skill pitfalls #8 —— getLastRowNum() 返回的是「曾编辑过的最大行索引」，
    // │         用户清空过的行仍会被记录，导致读出一堆全 null 行。必须用本方法重新探测真实空行
    // │  Who  : readWorkbook 遍历数据行时调用
    // │  When : 每读一行前
    // │  Where: 本类私有 isRowEmpty
    // │  How  : 遍历该行所有单元格，任一非空白即返回 false；否则 true
    private static boolean isRowEmpty(final Row row) {
        if (row == null) {
            return true;
        }
        for (int i = 0; i < row.getLastCellNum(); i++) {
            final Cell cell = row.getCell(i);
            if (cell == null) {
                continue;
            }
            if (cell.getCellType() != CellType.BLANK) {
                return false;
            }
        }
        return true;
    }

    public static <V> void writeXls(final DataFrame<V> df, final String output)
    throws IOException {
        writeXls(df, new FileOutputStream(output));
    }

    // ┌─ What : 写 .xls（HSSF，旧二进制格式）
    // │  Why  : 保留 .xls 出口（部分老系统仍需）；样式在循环外创建一次复用（已修 cell style 超 4000 bug）
    // │  Who  : 由 DataFrame.writeXls 调用
    // │  When : 用户写 .xls 时
    // │  Where: 本类 writeXls
    // │  How  : new HSSFWorkbook → writeWorkbook(wb, ...)
    public static <V> void writeXls(final DataFrame<V> df, final OutputStream output)
    throws IOException {
        final Workbook wb = new HSSFWorkbook();
        writeWorkbook(df, wb, output, false);
    }

    // ┌─ What : 写 .xlsx（XSSF 全内存或 SXSSF 流式，自动选择）
    // │  Why  : 新增 XLSX 支持；大数据量时用 SXSSF 流式写避免 OOM（修 E3 超大 Excel 写 OOM）。
    // │         当 df.length() > SXSSF_WINDOW_SIZE 时自动切 SXSSFWorkbook(windowSize)
    // │  Who  : 由 DataFrame.writeXlsx 调用
    // │  When : 用户写 .xlsx 时
    // │  Where: 本类 writeXlsx
    // │  How  : 伪代码：
    //            1. 行数 <= 窗口：new XSSFWorkbook（全内存，小文件开销低）
    //            2. 行数 > 窗口：new SXSSFWorkbook(SXSSF_WINDOW_SIZE)（流式刷盘）
    //            3. 走 writeWorkbook(wb, ...) 统一逻辑
    //            4. SXSSF 写完调 wb.dispose() 清理临时文件
    public static <V> void writeXlsx(final DataFrame<V> df, final String output)
    throws IOException {
        writeXlsx(df, new FileOutputStream(output));
    }

    public static <V> void writeXlsx(final DataFrame<V> df, final OutputStream output)
    throws IOException {
        final boolean useStream = df.length() > SXSSF_WINDOW_SIZE;
        final Workbook wb = useStream
                ? new SXSSFWorkbook(SXSSF_WINDOW_SIZE)
                : new XSSFWorkbook();
        try {
            writeWorkbook(df, wb, output, true);
        }
        finally {
            // SXSSF 必须调 dispose 清理 poi-sxssf-sheetXXXXX.xml 临时文件，否则磁盘泄漏
            if (wb instanceof SXSSFWorkbook) {
                ((SXSSFWorkbook) wb).dispose();
            }
        }
    }

    // ┌─ What : 把 DataFrame 写入 Workbook（HSSF/XSSF/SXSSF 通用）
    // │  Why  : 三种 Workbook 共用 ss.usermodel 接口，写逻辑一致；抽公共方法避免重复
    // │  Who  : writeXls / writeXlsx 调用
    // │  When : 写任意 Excel 格式时
    // │  Where: 本类私有 writeWorkbook
    // │  How  : 伪代码：
    //            1. 创建日期样式（dateStyle）与表头样式（headerStyle，含中文 CJK 字体）—— 循环外创建一次
    //            2. 写表头行：第 0 行，按列索引 createCell(c) + setCellStyle(headerStyle)
    //            3. 写数据行：第 1..N 行，按列索引 createCell(c) + writeCell
    //            4. wb.write(output) 后关 wb（SXSSF 关闭会刷盘）
    //            5. 中文支持：表头字体设为「微软雅黑」，写中文表头不乱码
    private static <V> void writeWorkbook(final DataFrame<V> df, final Workbook wb,
            final OutputStream output, final boolean xlsx) {
        final Sheet sheet = wb.createSheet();
        // —— 日期样式：循环外创建一次复用（poi-excel skill「样式复用」铁律）——
        final CellStyle dateStyle = wb.createCellStyle();
        dateStyle.setDataFormat(wb.getCreationHelper().createDataFormat()
                .getFormat("yyyy-MM-dd HH:mm:ss"));

        // —— 表头样式：加粗 + 中文 CJK 字体（中文完美支持）——
        final CellStyle headerStyle = wb.createCellStyle();
        final org.apache.poi.ss.usermodel.Font headerFont = wb.createFont();
        headerFont.setBold(true);
        // 中文支持：XSSF/HSSF 均可识别 CJK 字体名（仅写字体名，不嵌入字体文件，遵循字体铁律）
        headerFont.setFontName("微软雅黑");
        headerStyle.setFont(headerFont);

        // —— 写表头（按固定列索引，遵循 skill §10 写铁律）——
        Row row = sheet.createRow(0);
        final Iterator<Object> it = df.columns().iterator();
        for (int c = 0; c < df.size(); c++) {
            final Cell cell = row.createCell(c);
            final Object name = it.hasNext() ? it.next() : c;
            writeCell(cell, name, dateStyle);
            cell.setCellStyle(headerStyle);
        }

        // —— 写数据行 ——
        for (int r = 0; r < df.length(); r++) {
            row = sheet.createRow(r + 1);
            for (int c = 0; c < df.size(); c++) {
                final Cell cell = row.createCell(c);
                writeCell(cell, df.get(r, c), dateStyle);
            }
        }

        try {
            wb.write(output);
            output.close();
        }
        catch (IOException ex) {
            throw new RuntimeException("写入 Excel 失败: " + ex.getMessage(), ex);
        }
    }

    // ┌─ What : 通用单元格值读取（修 E4 类型不全 + 公式列「读入即结果」+ 严格类型保留）
    // │  Why  : 旧 readCell 缺 BLANK/FORMULA/ERROR 分支，公式格返回公式文本而非结果（参见 pitfalls #3）；
    // │         【硬性要求 1】读 Excel 时，公式列读进 DataFrame 必须是「计算后的结果值」，不是公式文本。
    // │         【硬性要求 2】Excel 单元格类型到 DataFrame 必须严格匹配，不自动推断/强转。规则：
    // │           - NUMERIC 单元格：日期 → Date；整数值 → Long（保留精度，不丢位）；
    // │                            小数值 → Double（按 Excel 有效位数）
    // │           - STRING/BOOLEAN: 原样
    // │           - FORMULA: evaluator.evaluate(cell) 求值后按结果类型走上述相同规则
    // │           - 求值失败/ERROR → "=" + 公式串（兜底，不静默丢数据）
    // │           - BLANK/_NONE: null
    // │  Who  : readWorkbook 遍历单元格时调用（单格层），列级混合类型降级在 readWorkbook 处理
    // │  When : 每读一个非空 cell
    // │  Where: 本类私有 readCell
    // │  How  : 伪代码：
    //            switch cellType:
    //              NUMERIC: isDate → Date；isInteger(d) → Long；否则 Double
    //              STRING/BOOLEAN: 原样
    //              FORMULA: evaluate → CellValue，按结果类型递归走 NUMERIC/STRING/BOOLEAN
    //              BLANK/ERROR/_NONE: null
    private static final Object readCell(final Cell cell, final FormulaEvaluator evaluator) {
        final CellType type = cell.getCellType();
        switch (type) {
            case NUMERIC:
                return readNumeric(cell);   // 日期/整数/小数严格分流
            case STRING:
                return cell.getStringCellValue();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case FORMULA:
                // 【公式列读入即结果】求值后按结果类型返回（不是公式文本）
                if (evaluator == null) {
                    return "=" + cell.getCellFormula();
                }
                try {
                    final org.apache.poi.ss.usermodel.CellValue cv = evaluator.evaluate(cell);
                    if (cv == null) {
                        return "=" + cell.getCellFormula();
                    }
                    switch (cv.getCellType()) {
                        case NUMERIC:
                            // 公式数值结果：同样按整数/小数严格分流（避免 10.0 当 Double）
                            return numericValueToObject(cv.getNumberValue());
                        case STRING:
                            return cv.getStringValue();
                        case BOOLEAN:
                            return cv.getBooleanValue();
                        default:
                            return "=" + cell.getCellFormula();   // ERROR / BLANK 兜底
                    }
                }
                catch (final RuntimeException ex) {
                    return "=" + cell.getCellFormula();   // 求值异常兜底
                }
            case BLANK:
            case ERROR:
            case _NONE:
            default:
                return null;
        }
    }

    // ┌─ What : NUMERIC 单元格读取：日期 → Date；整数 → Long；小数 → Double
    // │  Why  : 不能粗暴都转 Double —— 整数列（如 id、数量）转 Double 会丢精度（≥15 位变 0）且改变语义；
    // │         用 epsilon=1e-9 判定整数（避免浮点误差），整数转 Long，小数保留 Double
    private static Object readNumeric(final Cell cell) {
        if (DateUtil.isCellDateFormatted(cell)) {
            return cell.getDateCellValue();   // 日期 → Date
        }
        return numericValueToObject(cell.getNumericCellValue());
    }

    // ┌─ What : 把 double 数值按整数/小数分流为 Long 或 Double
    // │  How  : d == Math.floor(d) 且在 long 范围内 → Long；否则 Double
    // │         用 epsilon=1e-9 容差（参见 poi-excel pitfalls #4「整数判断浮点误差」）
    private static Object numericValueToObject(final double d) {
        final double eps = 1e-9;
        // 整数判定：与 floor 的差在 epsilon 内，且在 long 范围内
        if (Math.abs(d - Math.floor(d)) < eps && d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
            return (long) d;   // 整数 → Long
        }
        return d;   // 小数 → Double（按 Excel 原始精度）
    }

    // ┌─ What : 通用单元格值写入（修 E5 Boolean 丢数据 + E6 长整数精度）
    // │  Why  : 旧 writeCell 的 Boolean 分支只 setCellType 不 setCellValue（数据丢失）；
    // │         旧实现所有 Number 都 doubleValue() 写入，导致 ≥15 位的长整数（Long，如身份证/银行卡）
    // │         末尾变 0（double 浮点尾数限制，pitfalls #1）
    // │  Who  : writeWorkbook 写单元格时调用
    // │  When : 每写一个 cell
    // │  Where: 本类私有 writeCell
    // │  How  : 伪代码：
    //            if value == null → 空字符串
    //            else if value instanceof Date → setCellStyle(dateStyle) + setCellValue(Date)
    //            else if value instanceof Boolean → setCellValue(boolean)（修 E5）
    //            else if value instanceof Long:
    //              若位数 >= 15 → 当 STRING 写（修 E6 长整数精度，身份证/银行卡）
    //              否则 setCellValue(long)（普通整数按数值写）
    //            else if value instanceof Number → setCellValue(double)（Double/Float/Integer/Short 等）
    //            else → setCellValue(String.valueOf(value))（中文/普通文本走这里）
    private static final void writeCell(final Cell cell, final Object value, final CellStyle dateStyle) {
        if (value == null) {
            // 修 Bug-C：null 应写为 BLANK 单元格（读回仍是 null），保持 null 往返完整。
            // 旧实现写成空字符串 ""，读回变 "" 而非 null，破坏 isnull/dropna/fillna 等下游逻辑。
            // POI 中 createCell 创建的 cell 默认就是 BLANK 类型，不调 setCellValue 即保持 BLANK
            cell.setCellType(CellType.BLANK);
            return;
        }
        if (value instanceof Date) {
            cell.setCellStyle(dateStyle);
            cell.setCellValue((Date) value);
        }
        else if (value instanceof Boolean) {
            cell.setCellValue((Boolean) value);   // 修 E5：补 setCellValue，原实现漏写值
        }
        else if (value instanceof Long) {
            // 修 E6：【仅对 Long 类型做长整数保护】
            // ≥15 位的长整数（身份证号 18 位、银行卡号 16~19 位）当 STRING 写，避免 double 精度丢末尾
            final long lv = (Long) value;
            final String numStr = Long.toString(lv);
            final int digits = numStr.startsWith("-") ? numStr.length() - 1 : numStr.length();
            if (digits >= LONG_INTEGER_THRESHOLD) {
                cell.setCellType(CellType.STRING);
                cell.setCellValue(numStr);
            }
            else {
                cell.setCellValue(lv);   // 普通整数按 long 写，POI 会按 NUMERIC 存
            }
        }
        else if (value instanceof Number) {
            // Double/Float/Integer/Short/Byte —— 直接按 double 写（这些类型天然不在「长整数」场景）
            cell.setCellValue(((Number) value).doubleValue());
        }
        else {
            // String / 其他对象：中文按 String.valueOf 原样写入（POI 内部 UTF-16，无编码问题）
            cell.setCellType(CellType.STRING);
            cell.setCellValue(String.valueOf(value));
        }
    }

    public static DataFrame<Object> readSql(final ResultSet rs)
    throws SQLException {
        try {
            ResultSetMetaData md = rs.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                columns.add(md.getColumnLabel(i));
            }

            DataFrame<Object> df = new DataFrame<>(columns);
            List<Object> row = new ArrayList<>(columns.size());
            while (rs.next()) {
                for (String c : columns) {
                    row.add(rs.getString(c));
                }
                df.append(row);
                row.clear();
            }

            return df;
        } finally {
            rs.close();
        }
    }

    public static <V> void writeSql(final DataFrame<V> df, final PreparedStatement stmt)
    throws SQLException {
        try {
            ParameterMetaData md = stmt.getParameterMetaData();
            List<Integer> columns = new ArrayList<>();
            for (int i = 1; i <= md.getParameterCount(); i++) {
                columns.add(md.getParameterType(i));
            }

            for (int r = 0; r < df.length(); r++) {
                for (int c = 1; c <= df.size(); c++) {
                    stmt.setObject(c, df.get(r, c - 1));
                }
                stmt.addBatch();
            }

            stmt.executeBatch();
        } finally {
            stmt.close();
        }
    }
}
