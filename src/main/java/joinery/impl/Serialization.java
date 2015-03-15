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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.URL;
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

import joinery.DataFrame;
import joinery.DataFrame.NumberDefault;

import org.apache.poi.hssf.usermodel.HSSFDataFormat;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
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

public class Serialization {

    private static final String EMPTY_DF_STRING = "[empty data frame]";
    private static final String ELLIPSES = "...";
    private static final String NEWLINE = "\n";
    private static final String DELIMITER = "\t";
    private static final Object INDEX_KEY = new Object();
    private static final int    MAX_COLUMN_WIDTH = 20;

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
            width.put(INDEX_KEY, clamp(
                    width.get(INDEX_KEY),
                    MAX_COLUMN_WIDTH,
                    fmt(row.getClass(), row).length()));
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
            sb.append(truncate(lpad(fmt(row.getClass(), row), w), w));

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
    			new URL(file).openStream() : new FileInputStream(file), ",", NumberDefault.LONG_DEFAULT);
    }

    public static DataFrame<Object> readCsv(final String file, final String separator, NumberDefault numDefault)
    		throws IOException {
    	return readCsv(file.contains("://") ?
    			new URL(file).openStream() : new FileInputStream(file), separator, numDefault);
    }

    public static DataFrame<Object> readCsv(final InputStream input) 
    		throws IOException {
    	return readCsv(input, ",", NumberDefault.LONG_DEFAULT);
    }

    public static DataFrame<Object> readCsv(final InputStream input, String separator, NumberDefault numDefault)
    		throws IOException {
    	CsvPreference csvPreference;
    	switch (separator) {
    	case "\t":
    		csvPreference = CsvPreference.TAB_PREFERENCE;
    		break;
    	case ",":
    		csvPreference = CsvPreference.STANDARD_PREFERENCE;
    		break;
    	case ";":
    		csvPreference = CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE;
    		break;
    	default:
    		throw new IllegalArgumentException("Separator: " + separator + " is not currently supported");
    	}
    	try (CsvListReader reader = new CsvListReader(
    			new InputStreamReader(input), csvPreference)) {
    		final List<String> header = Arrays.asList(reader.getHeader(true));
    		final CellProcessor[] procs = new CellProcessor[header.size()];
    		final DataFrame<Object> df = new DataFrame<>(header);
    		for (List<Object> row = reader.read(procs); row != null; row = reader.read(procs)) {
    			df.append(new ArrayList<>(row));
    		}
    		return df.convert(numDefault);
    	}
    }

    public static <V> void writeCsv(final DataFrame<V> df, final String output)
    throws IOException {
        writeCsv(df, new FileOutputStream(output));
    }

    public static <V> void writeCsv(final DataFrame<V> df, final OutputStream output)
    throws IOException {
        try (CsvListWriter writer = new CsvListWriter(
                new OutputStreamWriter(output), CsvPreference.STANDARD_PREFERENCE)) {
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
            case Cell.CELL_TYPE_NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return DateUtil.getJavaDate(cell.getNumericCellValue());
                }
                return cell.getNumericCellValue();
            case Cell.CELL_TYPE_BOOLEAN:
                return cell.getBooleanCellValue();
            default:
                return cell.getStringCellValue();
        }
    }

    private static final void writeCell(final Cell cell, final Object value) {
        if (value instanceof Number) {
            cell.setCellType(Cell.CELL_TYPE_NUMERIC);
            cell.setCellValue(Number.class.cast(value).doubleValue());
        } else if (value instanceof Date) {
            final CellStyle style = cell.getSheet().getWorkbook().createCellStyle();
            style.setDataFormat(HSSFDataFormat.getBuiltinFormat("m/d/yy h:mm"));
            cell.setCellStyle(style);
            cell.setCellType(Cell.CELL_TYPE_NUMERIC);
            cell.setCellValue(Date.class.cast(value));
        } else if (value instanceof Boolean) {
            cell.setCellType(Cell.CELL_TYPE_BOOLEAN);
        } else {
            cell.setCellType(Cell.CELL_TYPE_STRING);
            cell.setCellValue(value != null ? String.valueOf(value) : "");
        }
    }
}
