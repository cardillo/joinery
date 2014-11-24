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

import java.util.Iterator;

import joinery.DataFrame;

public class Serialization {
    public static String toString(final DataFrame<?> df, final int limit) {
        final int len = df.length();
        final StringBuilder sb = new StringBuilder();

        for (final String column : df.columns()) {
            sb.append("\t");
            sb.append(column);
        }
        sb.append("\n");

        final Iterator<String> names = df.index().iterator();
        for (int r = 0; r < len; r++) {
            sb.append(names.hasNext() ? names.next() : String.valueOf(r));
            for (int c = 0; c < df.size(); c++) {
                sb.append("\t");
                sb.append(String.valueOf(df.get(c, r)));
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
}
