/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
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

import java.io.*;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
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
    private static final int MAX_COLUMN_WIDTH = 20;

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
        if (cls == null) return "null";
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

    public static DataFrame<Object> readCsv(final String file, final Charset charsetName)
            throws IOException {
        String charset = "GBK";
        byte[] first3Bytes = new byte[3];
        try {
            boolean checked = false;
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
            bis.mark(100); // 读者注： bis.mark(0);修改为 bis.mark(100);我用过这段代码，需要修改上面标出的地方。
            int read = bis.read(first3Bytes, 0, 3);
            if (read == -1) {
                bis.close();  //ANSI
            } else if (first3Bytes[0] == (byte) 0xFF && first3Bytes[1] == (byte) 0xFE) {
                charset = "UTF-16LE"; // 文件编码为 Unicode
                checked = true;
            } else if (first3Bytes[0] == (byte) 0xFE && first3Bytes[1] == (byte) 0xFF) {
                charset = "UTF-16BE"; // 文件编码为 Unicode big endian
                checked = true;
            } else if (first3Bytes[0] == (byte) 0xEF && first3Bytes[1] == (byte) 0xBB
                    && first3Bytes[2] == (byte) 0xBF) {
                charset = "UTF-8"; // 文件编码为 UTF-8
                checked = true;
            }
            bis.reset();
            if (!checked) {
                while ((read = bis.read()) != -1) {
                    if (read >= 0xF0)
                        break;
                    if (0x80 <= read && read <= 0xBF) // 单独出现BF以下的，也算是GBK
                        break;
                    if (0xC0 <= read && read <= 0xDF) {
                        read = bis.read();
                        if (0x80 <= read && read <= 0xBF) // 双字节 (0xC0 - 0xDF)
                            // (0x80 - 0xBF),也可能在GB编码内
                            continue;
                        else
                            break;
                    } else if (0xE0 <= read && read <= 0xEF) { // 也有可能出错，但是几率较小
                        read = bis.read();
                        if (0x80 <= read && read <= 0xBF) {
                            read = bis.read();
                            if (0x80 <= read && read <= 0xBF) {
                                charset = "UTF-8";
                                break;
                            } else
                                break;
                        } else
                            break;
                    }
                }
            }
            bis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }


        if (charsetName.toString().equals(charset)) {
            return readCsv(file.contains("://") ?
                    new URL(file).openStream() : new FileInputStream(file), ",", NumberDefault.LONG_DEFAULT, null);

        } else {
            System.out.println("the encoding method of " + file + " is " + charset + ", not " + charsetName.toString());
            throw new IOException();
        }
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
        return readCsv(input, separator, numDefault, naString, true);
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
                csvPreference = new CsvPreference.Builder('"', '|', "\n").build();
                break;
            default:
                throw new IllegalArgumentException("Separator: " + separator + " is not currently supported");
        }
        try (CsvListReader reader = new CsvListReader(new InputStreamReader(input), csvPreference)) {
            final List<String> header;
            final DataFrame<Object> df;
            final CellProcessor[] procs;
            if (hasHeader) {
                header = Arrays.asList(reader.getHeader(true));
                procs = new CellProcessor[header.size()];
                df = new DataFrame<>(header);
            } else {
                // Read the first row to figure out how many columns we have
                reader.read();
                header = new ArrayList<String>();
                for (int i = 0; i < reader.length(); i++) {
                    header.add("V" + i);
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

    public static <V> void writeCsv(final DataFrame<V> df, final String output)
            throws IOException {
        writeCsv(df, new FileOutputStream(output));
    }

    public static <V> void writeCsv(final DataFrame<V> df, final OutputStream output)
            throws IOException {
        try (CsvListWriter writer = new CsvListWriter(new OutputStreamWriter(output), CsvPreference.STANDARD_PREFERENCE)) {
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

    public static DataFrame<Object> readXls(final InputStream input)
            throws IOException {
        final Workbook wb = new HSSFWorkbook(input);
        final Sheet sheet = wb.getSheetAt(0);
        final List<Object> columns = new ArrayList<>();
        final List<List<Object>> data = new ArrayList<>();

        for (final Row row : sheet) {
            if (row.getRowNum() == 0) {
                // read header
                for (final Cell cell : row) {
                    columns.add(readCell(cell));
                }
            } else {
                // read data values
                final List<Object> values = new ArrayList<>();
                for (final Cell cell : row) {
                    values.add(readCell(cell));
                }
                data.add(values);
            }
        }

        // create data frame
        final DataFrame<Object> df = new DataFrame<>(columns);
        for (final List<Object> row : data) {
            df.append(row);
        }

        return df.convert();
    }

    public static <V> void writeXls(final DataFrame<V> df, final String output)
            throws IOException {
        writeXls(df, new FileOutputStream(output));
    }

    public static <V> void writeXls(final DataFrame<V> df, final OutputStream output)
            throws IOException {
        final Workbook wb = new HSSFWorkbook();
        final Sheet sheet = wb.createSheet();

        // add header
        Row row = sheet.createRow(0);
        final Iterator<Object> it = df.columns().iterator();
        for (int c = 0; c < df.size(); c++) {
            final Cell cell = row.createCell(c);
            writeCell(cell, it.hasNext() ? it.next() : c);
        }

        // add data values
        for (int r = 0; r < df.length(); r++) {
            row = sheet.createRow(r + 1);
            for (int c = 0; c < df.size(); c++) {
                final Cell cell = row.createCell(c);
                writeCell(cell, df.get(r, c));
            }
        }

        //  write to stream
        wb.write(output);
        output.close();
    }

    private static final Object readCell(final Cell cell) {
        switch (cell.getCellType()) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return DateUtil.getJavaDate(cell.getNumericCellValue());
                }
                return cell.getNumericCellValue();
            case BOOLEAN:
                return cell.getBooleanCellValue();
            default:
                return cell.getStringCellValue();
        }
    }

    private static final void writeCell(final Cell cell, final Object value) {
        if (value instanceof Number) {
            cell.setCellType(CellType.NUMERIC);
            cell.setCellValue(Number.class.cast(value).doubleValue());
        } else if (value instanceof Date) {
            final CellStyle style = cell.getSheet().getWorkbook().createCellStyle();
            style.setDataFormat(HSSFDataFormat.getBuiltinFormat("m/d/yy h:mm"));
            cell.setCellStyle(style);
            cell.setCellType(CellType.NUMERIC);
            cell.setCellValue(Date.class.cast(value));
        } else if (value instanceof Boolean) {
            cell.setCellType(CellType.BOOLEAN);
        } else {
            cell.setCellType(CellType.STRING);
            cell.setCellValue(value != null ? String.valueOf(value) : "");
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
