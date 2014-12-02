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
import joinery.impl.Aggregation.Unique;

public class Pivoting {
    public static <V> DataFrame<V> pivot(
            final DataFrame<V> df, final int[] rows,
            final int[] cols, final int[] values) {
        final Map<Object, DataFrame<V>> grouped = df.groupBy(rows).groups();
        final Map<Integer, Unique<V>> aggregates = new LinkedHashMap<>();
        for (final Map.Entry<Object, DataFrame<V>> entry : grouped.entrySet()) {
            grouped.put(entry.getKey(), entry.getValue().groupBy(cols));
        }
        for (final int v : values) {
            aggregates.put(v, new Unique<V>());
        }
        return pivot(grouped, aggregates);
    }

    public static <I, O> DataFrame<O> pivot(
            final DataFrame<I> df, final KeyFunction<I> rows,
            final KeyFunction<I> cols, final Map<Integer, ? extends Aggregate<I,O>> values) {
        final Map<Object, DataFrame<I>> grouped = df.groupBy(rows).groups();
        for (final Map.Entry<Object, DataFrame<I>> entry : grouped.entrySet()) {
            grouped.put(entry.getKey(), entry.getValue().groupBy(cols));
        }
        return pivot(grouped, values);
    }

    private static <I, O> DataFrame<O> pivot(final Map<Object, DataFrame<I>> grouped, final Map<Integer, ? extends Aggregate<I,O>> values) {
        final Set<String> pivotRows = new LinkedHashSet<>();
        final Set<String> pivotCols = new LinkedHashSet<>();
        final Map<String, Map<String, List<I>>> pivotData = new LinkedHashMap<>();
        final Map<String, Aggregate<I, O>> pivotFunctions = new LinkedHashMap<>();
        List<String> colNames = null;

        for (final Map.Entry<Object, DataFrame<I>> rowEntry : grouped.entrySet()) {
            final String rowName = String.valueOf(rowEntry.getKey());
            Map<String, List<I>> rowData = pivotData.get(rowName);
            if (rowData == null) {
                pivotRows.add(rowName);
                rowData = new LinkedHashMap<>();
                pivotData.put(rowName, rowData);
            }

            final Map<Object, DataFrame<I>> byCol = rowEntry.getValue().groups();

            for (final Map.Entry<Object, DataFrame<I>> colEntry : byCol.entrySet()) {
                for (final int c : values.keySet()) {
                    String colName = String.valueOf(colEntry.getKey());
                    if (values.size() > 1) {
                        if (colNames == null) {
                            colNames = new ArrayList<>(rowEntry.getValue().columns());
                        }
                        final List<Object> tmp = new ArrayList<>();
                        tmp.add(colNames.get(c));
                        if (colEntry.getKey() instanceof List) {
                            for (final Object col : List.class.cast(colEntry.getKey())) {
                                tmp.add(col);
                            }
                        } else {
                            tmp.add(colEntry.getKey());
                        }
                        colName = String.valueOf(tmp);
                    }

                    List<I> colData = rowData.get(colName);
                    if (colData == null) {
                        pivotCols.add(colName);
                        colData = new ArrayList<>();
                        rowData.put(colName, colData);
                    }

                    colData.addAll(colEntry.getValue().col(c));
                    pivotFunctions.put(colName, values.get(c));
                }
            }
        }

        final DataFrame<O> pivot = new DataFrame<>(pivotRows, pivotCols);
        for (final String col : pivot.columns()) {
            for (final String row : pivot.index()) {
                pivot.set(row, col, pivotFunctions.get(col).apply(pivotData.get(row).get(col)));
            }
        }

        return pivot;
    }
}
