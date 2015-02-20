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
            sb.append(center(column, width.get(column)));
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

    private static final String center(final Object o, final int w) {
        final StringBuilder sb = new StringBuilder();
        final String value = String.valueOf(o);
        for (int i = 0; i < (w >> 1); i ++) {
            sb.append(' ');
        }
        sb.append(value);
        for (int i = sb.length(); i < w; i++) {
            sb.append(' ');
        }
        return sb.toString();
    }

    private static final String truncate(final Object o, final int w) {
        final String value = String.valueOf(o);
        return value.length() > w ? value.substring(0, w - 3) + ELLIPSES : value;
    }

    private static final String fmt(final Class<?> cls, final Object o) {
        String s;
        if (o instanceof Number) {
            if (Short.class.equals(cls) || Integer.class.equals(cls) ||
                    Long.class.equals(cls) || BigInteger.class.equals(cls)) {
                s = String.format("%d", Number.class.cast(o).longValue());
            } else {
                s = String.format("%.8f", Number.class.cast(o).doubleValue());
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
            s = String.valueOf(o);
        }
        return s;
    }

    public static DataFrame<Object> readCsv(final String file)
    throws IOException {
        return readCsv(file.contains("://") ?
                    new URL(file).openStream() : new FileInputStream(file));
    }

    public static DataFrame<Object> readCsv(final InputStream input)
    throws IOException {
        try (CsvListReader reader = new CsvListReader(
                new InputStreamReader(input), CsvPreference.STANDARD_PREFERENCE)) {
            final List<Object> header = Arrays.<Object>asList((Object[])reader.getHeader(true));
            final CellProcessor[] procs = new CellProcessor[header.size()];
            final DataFrame<Object> df = new DataFrame<>(header);
            for (List<Object> row = reader.read(procs); row != null; row = reader.read(procs)) {
                df.append(new ArrayList<>(row));
            }
            return df.convert();
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
            writer.writeHeader(df.columns().toArray(new String[df.size()]));
            final CellProcessor[] procs = new CellProcessor[df.size()];
            Arrays.fill(procs, new ConvertNullTo(""));
            for (final List<V> row : df) {
                writer.write(row, procs);
            }
        }
    }

}
