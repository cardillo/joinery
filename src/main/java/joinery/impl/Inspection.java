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

import java.util.ArrayList;
import java.util.List;

import joinery.DataFrame;

public class Inspection {
    public static List<Class<?>> types(final DataFrame<?> df) {
        final List<Class<?>> types = new ArrayList<>(df.size());
        for (int c = 0; c < df.size() && 0 < df.length(); c++) {
            final Object value = df.get(0, c);
            types.add(value != null ? value.getClass() : Object.class);
        }
        return types;
    }

    public static SparseBitSet numeric(final DataFrame<?> df) {
        final SparseBitSet numeric = new SparseBitSet();
        final List<Class<?>> types = types(df);
        for (int c = 0; c < types.size(); c++) {
            if (Number.class.isAssignableFrom(types.get(c))) {
                numeric.set(c);
            }
        }
        return numeric;
    }

    public static SparseBitSet nonnumeric(final DataFrame<?> df) {
        final SparseBitSet nonnumeric = numeric(df);
        nonnumeric.flip(0, df.size());
        return nonnumeric;
    }
}
