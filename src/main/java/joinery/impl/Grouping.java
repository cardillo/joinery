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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import joinery.DataFrame;
import joinery.DataFrame.Aggregate;
import joinery.DataFrame.KeyFunction;

public class Grouping
implements Iterable<Map.Entry<Object, BitSet>> {

    private final Map<Object, BitSet> groups = new LinkedHashMap<>();

    public Grouping() { }

    public <V> Grouping(final DataFrame<V> df, final KeyFunction<V> function) {
        final Iterator<List<V>> iter = df.iterator();
        for (int r = 0; iter.hasNext(); r++) {
            final List<V> row = iter.next();
            final Object key = function.apply(row);
            BitSet group = groups.get(key);
            if (group == null) {
                group = new BitSet();
                groups.put(key, group);
            }
            group.set(r);
        }
    }

    public <V> Grouping(final DataFrame<V> df, final int ... columns) {
        this(df, columns.length == 1 ?
                new KeyFunction<V>() {
                    @Override
                    public Object apply(final List<V> value) {
                        return value.get(columns[0]);
                    }

                } :
                new KeyFunction<V>() {
                    @Override
                    public Object apply(final List<V> value) {
                        final List<Object> key = new ArrayList<>(columns.length);
                        for (final int column : columns) {
                            key.add(value.get(column));
                        }
                        return Collections.unmodifiableList(key);
                    }
            });
    }

    public <I, O> DataFrame<O> apply(final DataFrame<I> df, final Aggregate<I, O> function) {
        final List<List<O>> grouped = new ArrayList<>();
        final List<String> names = new ArrayList<>(df.columns());
        final List<String> newcols = new ArrayList<>();
        final List<String> labels = new ArrayList<String>();

        for (int c = 0; c < df.size(); c++) {
            final List<O> column = new ArrayList<>();
            if (groups.isEmpty()) {
                try {
                    column.add(function.apply(df.col(c)));
                } catch (final ClassCastException ignored) { }
            } else {
                for (final Map.Entry<Object, BitSet> entry : groups.entrySet()) {
                    final BitSet rows = entry.getValue();
                    final List<I> values = new ArrayList<>(rows.cardinality());
                    for (int r = rows.nextSetBit(0); r >= 0; r = rows.nextSetBit(r + 1)) {
                        values.add(df.col(c).get(r));
                    }
                    try {
                        column.add(function.apply(values));
                        if (grouped.isEmpty()) {
                            labels.add(String.valueOf(entry.getKey()));
                        }
                    } catch (final ClassCastException ignored) { }
                }
            }

            if (!column.isEmpty()) {
                grouped.add(column);
                newcols.add(names.get(c));
            }
        }

        if (newcols.isEmpty()) {
            throw new IllegalArgumentException("no results for aggregate function " + function.getClass().getSimpleName());
        }

        return new DataFrame<O>(labels, newcols, grouped);
    }

    @Override
    public Iterator<Map.Entry<Object, BitSet>> iterator() {
        return groups.entrySet().iterator();
    }
}
