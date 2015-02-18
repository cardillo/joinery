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
import joinery.DataFrame.NumberDefault;

import org.supercsv.cellprocessor.ConvertNullTo;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

public class Serialization {
	
    /**
     * 
     * @param df
     * @param rowLimit limits how many rows are printed. -1 => no limit
     * @param colLimit limits the width (in no characters) of each column. -1 => no limit
     * @param usePadding flag to signal if values in each row should 
     * be padded with spaces to be at least as wide as column label
     * @return
     */
    public static String toString(final DataFrame<?> df, final int rowLimit, final int colLimit, boolean usePadding) {
    	final int len = df.length();
        final StringBuilder sb = new StringBuilder();

        int [] padLen = new int[df.size()];
        int padIdx = 0;
        for (final String column : df.columns()) {
            sb.append("\t");
            sb.append(column + ",");
            padLen[padIdx++] = column.length();
        }
        // Chop of last ","
        sb.deleteCharAt(sb.length()-1);
        sb.append("\n");

        final Iterator<String> names = df.index().iterator();
        for (int r = 0; r < len; r++) {
            sb.append(names.hasNext() ? names.next() : String.valueOf(r));
            padIdx = 0;
            for (int c = 0; c < df.size(); c++) {
                sb.append("\t");
                String out = String.valueOf(df.get(r, c));
                if(colLimit > 0 && out.length()>colLimit) {
                	out = out.substring(0, colLimit-3) + "...";
                }
                if(usePadding && out.length()<padLen[padIdx]) {
                	char[] chars = new char[padLen[padIdx]-out.length()];
                	Arrays.fill(chars, ' ');
                	String padding = new String(chars);
                	out += padding;
                }
                sb.append(out);
                padIdx++;
            }
            sb.append("\n");

            if(rowLimit>0) {
            	if (rowLimit - 3 < r && r < (rowLimit << 1) && r < len - 4) {
            		sb.append("\n... ");
            		sb.append(len - rowLimit);
            		sb.append(" rows skipped ...\n\n");
            		while (r < len - 2) {
            			if (names.hasNext()) {
            				names.next();
            			}
            			r++;
            		}
            	}
            }
        }

        return sb.toString();
    }
    
    public static String toString(final DataFrame<?> df, final int limit) {
    	return toString(df, limit, -1, true);
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
            writer.writeHeader(df.columns().toArray(new String[df.size()]));
            final CellProcessor[] procs = new CellProcessor[df.size()];
            Arrays.fill(procs, new ConvertNullTo(""));
            for (final List<V> row : df) {
                writer.write(row, procs);
            }
        }
    }

}
