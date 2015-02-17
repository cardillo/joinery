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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import joinery.DataFrame;

import org.supercsv.cellprocessor.ConvertNullTo;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

public class Serialization {
    public static String toString(final DataFrame<?> df, final int limit) {
        final int len = df.length();
        final StringBuilder sb = new StringBuilder();

        for (final Object column : df.columns()) {
            sb.append("\t");
            sb.append(String.valueOf(column));
        }
        sb.append("\n");

        final Iterator<Object> names = df.index().iterator();
        for (int r = 0; r < len; r++) {
            sb.append(String.valueOf(names.hasNext() ? names.next() : r));
            for (int c = 0; c < df.size(); c++) {
                sb.append("\t");
                sb.append(String.valueOf(df.get(r, c)));
            }
            sb.append("\n");

            if (limit - 3 < r && r < (limit << 1) && r < len - 4) {
                sb.append("\n... ");
                sb.append(len - limit);
                sb.append(" rows skipped ...\n\n");
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
