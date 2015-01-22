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

import java.util.LinkedHashSet;
import java.util.Set;

import joinery.DataFrame;

public class Comparison {
    public static final <V> DataFrame<String> compare(final DataFrame<V> df1, final DataFrame<V> df2) {
        // algorithm
        // 1. determine union of rows and columns
        final Set<String> rows = new LinkedHashSet<>();
        rows.addAll(df1.index());
        rows.addAll(df2.index());
        final Set<String> cols = new LinkedHashSet<>();
        cols.addAll(df1.columns());
        cols.addAll(df2.columns());

        // 2. reshape left to contain all rows and columns
        final DataFrame<V> left = df1.reshape(rows, cols);
        // 3. reshape right to contain all rows and columns
        final DataFrame<V> right = df2.reshape(rows, cols);

        final DataFrame<String> comp = new DataFrame<>(rows, cols);

        // 4. perform comparison cell by cell
        for (int c = 0; c < left.size(); c++) {
            for (int r = 0; r < left.length(); r++) {
                final V lval = left.get(r, c);
                final V rval = right.get(r, c);

                if (lval == null && rval == null) {
                    // equal but null
                    comp.set(r, c, "");
                } else if (lval != null && lval.equals(rval)) {
                    // equal
                    comp.set(r, c, String.valueOf(lval));
                } else if (lval == null) {
                    // missing from left
                    comp.set(r, c, String.valueOf(rval)); // + " (added from right)");
                } else if (rval == null) {
                    // missing from right
                    comp.set(r, c, String.valueOf(lval)); // + " (added from left)");
                } else {
                    // not equal
                    comp.set(r, c, String.format("%s | %s", lval, rval));
                }
            }
        }

        return comp;
    }
}
