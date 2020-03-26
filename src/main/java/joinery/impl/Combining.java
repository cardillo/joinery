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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joinery.DataFrame;
import joinery.DataFrame.JoinType;
import joinery.DataFrame.KeyFunction;

public class Combining {
    public static <V> DataFrame<V> join(final DataFrame<V> left, final DataFrame<V> right, final JoinType how, final KeyFunction<V> on) {
        final Iterator<Object> leftIt = left.index().iterator();
        final Iterator<Object> rightIt = right.index().iterator();
        final Map<Object, List<V>> leftMap = new LinkedHashMap<>();
        final Map<Object, List<V>> rightMap = new LinkedHashMap<>();

        for (final List<V> row : left) {
            final Object name = leftIt.next();
            final Object key = on == null ? name : on.apply(row);
            if (leftMap.put(key, row) != null) {
                throw new IllegalArgumentException("generated key is not unique: " + key);
            }
        }

        for (final List<V> row : right) {
            final Object name = rightIt.next();
            final Object key = on == null ? name : on.apply(row);
            if (rightMap.put(key, row) != null) {
                throw new IllegalArgumentException("generated key is not unique: " + key);
            }
        }

        final List<Object> columns = new ArrayList<>(how != JoinType.RIGHT ? left.columns() : right.columns());
        for (Object column : how != JoinType.RIGHT ? right.columns() : left.columns()) {
            final int index = columns.indexOf(column);
            if (index >= 0) {
                if (column instanceof List) {
                    @SuppressWarnings("unchecked")
                    final List<Object> l1 = List.class.cast(columns.get(index));
                    l1.add(how != JoinType.RIGHT ? "left" : "right");
                    @SuppressWarnings("unchecked")
                    final List<Object> l2= List.class.cast(column);
                    l2.add(how != JoinType.RIGHT ? "right" : "left");
                } else {
                    columns.set(index, String.format("%s_%s", columns.get(index), how != JoinType.RIGHT ? "left" : "right"));
                    column = String.format("%s_%s", column, how != JoinType.RIGHT ? "right" : "left");
                }
            }
            columns.add(column);
        }

        final DataFrame<V> df = new DataFrame<>(columns);
        for (final Map.Entry<Object, List<V>> entry : how != JoinType.RIGHT ? leftMap.entrySet() : rightMap.entrySet()) {
            final List<V> tmp = new ArrayList<>(entry.getValue());
            final List<V> row = how != JoinType.RIGHT ? rightMap.get(entry.getKey()) : leftMap.get(entry.getKey());
            if (row != null || how != JoinType.INNER) {
                tmp.addAll(row != null ? row : Collections.<V>nCopies(right.columns().size(), null));
                df.append(entry.getKey(), tmp);
            }
        }

        if (how == JoinType.OUTER) {
            for (final Map.Entry<Object, List<V>> entry : how != JoinType.RIGHT ? rightMap.entrySet() : leftMap.entrySet()) {
                final List<V> row = how != JoinType.RIGHT ? leftMap.get(entry.getKey()) : rightMap.get(entry.getKey());
                if (row == null) {
                    final List<V> tmp = new ArrayList<>(Collections.<V>nCopies(
                        how != JoinType.RIGHT ? left.columns().size() : right.columns().size(), null));
                    tmp.addAll(entry.getValue());
                    df.append(entry.getKey(), tmp);
                }
            }
        }

        return df;
    }

    public static <V> DataFrame<V> joinOn(final DataFrame<V> left, final DataFrame<V> right, final JoinType how, final Integer ... cols) {
        return join(left, right, how, new KeyFunction<V>() {
            @Override
            public Object apply(final List<V> value) {
                final List<V> key = new ArrayList<>(cols.length);
                for (final int col : cols) {
                    key.add(value.get(col));
                }
                return Collections.unmodifiableList(key);
            }
        });
    }

    public static <V> DataFrame<V> merge(final DataFrame<V> left, final DataFrame<V> right, final JoinType how) {
        final Set<Object> intersection = new LinkedHashSet<>(left.nonnumeric().columns());
        intersection.retainAll(right.nonnumeric().columns());
        final Object[] columns = intersection.toArray(new Object[intersection.size()]);
        return join(left.reindex(columns), right.reindex(columns), how, null);
    }

    @SafeVarargs
    public static <V> void update(final DataFrame<V> dest,  final boolean overwrite, final DataFrame<? extends V> ... others) {
        for (int col = 0; col < dest.size(); col++) {
            for (int row = 0; row < dest.length(); row++) {
                if (overwrite || dest.get(row, col) == null) {
                    for (final DataFrame<? extends V> other : others) {
                        if (col < other.size() && row < other.length()) {
                            final V value = other.get(row, col);
                            if (value != null) {
                                dest.set(row, col, value);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    @SafeVarargs
    public static <V> DataFrame<V> concat(
            final DataFrame<V> first, final DataFrame<? extends V> ... others) {
        List<DataFrame<? extends V>> dfs = new ArrayList<>(others.length + 1);
        dfs.add(first);
        dfs.addAll(Arrays.asList(others));

        int rows = 0;
        Set<Object> columns = new LinkedHashSet<>();
        for (DataFrame<? extends V> df : dfs) {
            rows += df.length();
            for (Object c : df.columns()) {
                columns.add(c);
            }
        }

        List<Object> newcols = new ArrayList<>(columns);
        DataFrame<V> combined = new DataFrame<V>(columns).reshape(rows, columns.size());
        int offset = 0;
        for (DataFrame<? extends V> df : dfs) {
            List<Object> cols = new ArrayList<>(df.columns());
            for (int c = 0; c < cols.size(); c++) {
                int newc = newcols.indexOf(cols.get(c));
                for (int r = 0; r < df.length(); r++) {
                    combined.set(offset + r, newc, df.get(r, c));
                }
            }
            offset += df.length();
        }

        return combined;
    }
}
