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
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import joinery.DataFrame;
import joinery.DataFrame.Predicate;

public class Selection {
    public static <V> BitSet select(final DataFrame<V> df, final Predicate<V> predicate) {
        final BitSet selected = new BitSet();
        final Iterator<List<V>> rows = df.iterator();
        for (int r = 0; rows.hasNext(); r++) {
            if (predicate.apply(rows.next())) {
                selected.set(r);
            }
        }
        return selected;
    }

    public static Index select(final Index index, final BitSet selected) {
        final List<Object> names = new ArrayList<>(index.names());
        final Index newidx = new Index();
        for (int r = selected.nextSetBit(0); r >= 0; r = selected.nextSetBit(r + 1)) {
            final Object name = names.get(r);
            newidx.add(name, index.get(name));
        }
        return newidx;
    }

    public static <V> BlockManager<V> select(final BlockManager<V> blocks, final BitSet selected) {
        final List<List<V>> data = new LinkedList<List<V>>();
        for (int c = 0; c < blocks.size(); c++) {
            final List<V> column = new ArrayList<>(selected.cardinality());
            for (int r = selected.nextSetBit(0); r >= 0; r = selected.nextSetBit(r + 1)) {
                column.add(blocks.get(c, r));
            }
            data.add(column);
        }
        return new BlockManager<>(data);
    }

    public static <V> BlockManager<V> select(final BlockManager<V> blocks, final BitSet rows, final BitSet cols) {
        final List<List<V>> data = new LinkedList<List<V>>();
        for (int c = cols.nextSetBit(0); c >= 0; c = cols.nextSetBit(c + 1)) {
            final List<V> column = new ArrayList<>(rows.cardinality());
            for (int r = rows.nextSetBit(0); r >= 0; r = rows.nextSetBit(r + 1)) {
                column.add(blocks.get(c, r));
            }
            data.add(column);
        }
        return new BlockManager<>(data);
    }

    public static <V> BitSet[] slice(final DataFrame<V> df,
            final Integer rowStart, final Integer rowEnd, final Integer colStart, final Integer colEnd) {
        final BitSet rows = new BitSet();
        final BitSet cols = new BitSet();
        rows.set(rowStart, rowEnd);
        cols.set(colStart, colEnd);
        return new BitSet[] { rows, cols };
    }
}
