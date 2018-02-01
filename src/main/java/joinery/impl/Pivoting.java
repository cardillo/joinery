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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joinery.DataFrame;
import joinery.DataFrame.Aggregate;
import joinery.DataFrame.KeyFunction;
import joinery.LocalDataFrame;
import joinery.impl.Aggregation.Unique;

public class Pivoting {
    public static <V> DataFrame<V> pivot(
            final DataFrame<V> df, final Integer[] rows,
            final Integer[] cols, final Integer[] values) {
        final DataFrame<V> grouped = df.groupBy(rows);
        final Map<Object, DataFrame<V>> exploded = grouped.explode();
        final Map<Integer, Unique<V>> aggregates = new LinkedHashMap<>();
        for (final Map.Entry<Object, DataFrame<V>> entry : exploded.entrySet()) {
            exploded.put(entry.getKey(), entry.getValue().groupBy(cols));
        }
        for (final int v : values) {
            aggregates.put(v, new Unique<V>());
        }
        return pivot(exploded, aggregates, grouped.groups().columns());
    }

    public static <I, O> DataFrame<O> pivot(
            final DataFrame<I> df, final KeyFunction<I> rows,
            final KeyFunction<I> cols, final Map<Integer, ? extends Aggregate<I,O>> values) {
        final DataFrame<I> grouped = df.groupBy(rows);
        final Map<Object, DataFrame<I>> exploded = grouped.explode();
        for (final Map.Entry<Object, DataFrame<I>> entry : exploded.entrySet()) {
            exploded.put(entry.getKey(), entry.getValue().groupBy(cols));
        }
        return pivot(exploded, values, grouped.groups().columns());
    }

    @SuppressWarnings("unchecked")
    private static <I, O> DataFrame<O> pivot(
            final Map<Object, DataFrame<I>> grouped,
            final Map<Integer, ? extends Aggregate<I,O>> values,
            final Set<Integer> columns) {
        final Set<Object> pivotCols = new LinkedHashSet<>();
        final Map<Object, Map<Object, List<I>>> pivotData = new LinkedHashMap<>();
        final Map<Object, Aggregate<I, ?>> pivotFunctions = new LinkedHashMap<>();
        final List<Object> colNames = new ArrayList<>(grouped.values().iterator().next().columns());

        // allocate row -> column -> data maps
        for (final Map.Entry<Object, DataFrame<I>> rowEntry : grouped.entrySet()) {
            final Map<Object, List<I>> rowData = new LinkedHashMap<>();
            for (final int c : columns) {
                final Object colName = colNames.get(c);
                rowData.put(colName, new ArrayList<I>());
                pivotCols.add(colName);
            }
            for (final Object colKey : rowEntry.getValue().groups().keys()) {
                for (final int c : values.keySet()) {
                    final Object colName = name(colKey, colNames.get(c), values);
                    rowData.put(colName, new ArrayList<I>());
                    pivotCols.add(colName);
                    pivotFunctions.put(colName, values.get(c));
                }
            }
            pivotData.put(rowEntry.getKey(), rowData);
        }

        // collect data for row and column groups
        for (final Map.Entry<Object, DataFrame<I>> rowEntry : grouped.entrySet()) {
            final Object rowName = rowEntry.getKey();
            final Map<Object, List<I>> rowData = pivotData.get(rowName);
            final Map<Object, DataFrame<I>> byCol = rowEntry.getValue().explode();
            for (final Map.Entry<Object, DataFrame<I>> colEntry : byCol.entrySet()) {
                // add columns used as pivot rows
                for (final int c : columns) {
                    final Object colName = colNames.get(c);
                    final List<I> colData = rowData.get(colName);
                    // optimization, only add first value
                    // since the values are all the same (due to grouping)
                    colData.add(colEntry.getValue().get(0, c));
                }

                // add values for aggregation
                for (final int c : values.keySet()) {
                    final Object colName = name(colEntry.getKey(), colNames.get(c), values);
                    final List<I> colData = rowData.get(colName);
                    colData.addAll(colEntry.getValue().col(c));
                }
            }
        }

        // iterate over row, column pairs and apply aggregate functions
        final DataFrame<O> pivot = new LocalDataFrame<>(pivotData.keySet(), pivotCols);
        for (final Object col : pivot.columns()) {
            for (final Object row : pivot.index()) {
                final List<I> data = pivotData.get(row).get(col);
                if (data != null) {
                    final Aggregate<I, ?> func = pivotFunctions.get(col);
                    if (func != null) {
                        pivot.set(row, col, (O)func.apply(data));
                    } else {
                        pivot.set(row, col, (O)data.get(0));
                    }
                }
            }
        }

        return pivot;
    }

    private static Object name(final Object key, final Object name, final Map<?, ?> values) {
        Object colName = key;

        // if multiple value columns are requested the
        // value column name must be added to the pivot column name
        if (values.size() > 1) {
            final List<Object> tmp = new ArrayList<>();
            tmp.add(name);
            if (key instanceof List) {
                for (final Object col : List.class.cast(key)) {
                    tmp.add(col);
                }
            } else {
                tmp.add(key);
            }
            colName = tmp;
        }

        return colName;
    }
}
