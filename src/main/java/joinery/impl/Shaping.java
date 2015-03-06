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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import joinery.DataFrame;

public class Shaping {
    public static final <V> DataFrame<V> reshape(final DataFrame<V> df, final int rows, final int cols) {
        final DataFrame<V> reshaped = new DataFrame<>();
        Iterator<Object> it;

        it = df.columns().iterator();
        for (int c = 0; c < cols; c++) {
            final Object name = it.hasNext() ? it.next() : c;
            reshaped.add(name);
        }

        it = df.index().iterator();
        for (int r = 0; r < rows; r++) {
            final Object name = it.hasNext() ? it.next() : r;
            reshaped.append(name, Collections.<V>emptyList());
        }

        for (int c = 0; c < cols; c++) {
            for (int r = 0; r < rows; r++) {
                if (c < df.size() && r < df.length()) {
                    reshaped.set(r, c, df.get(r, c));
                }
            }
        }

        return reshaped;
    }

    public static final <V> DataFrame<V> reshape(final DataFrame<V> df, final Collection<?> rows, final Collection<?> cols) {
        final DataFrame<V> reshaped = new DataFrame<>();

        for (final Object name : cols) {
            reshaped.add(name);
        }

        for (final Object name: rows) {
            reshaped.append(name, Collections.<V>emptyList());
        }

        for (final Object c : cols) {
            for (final Object r : rows) {
                if (df.columns().contains(c) && df.index().contains(r)) {
                    reshaped.set(r, c, df.get(r, c));
                }
            }
        }

        return reshaped;
    }
}
