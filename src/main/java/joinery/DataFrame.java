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

package joinery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import joinery.impl.BlockManager;

/**
 * A minimal data frame implementation in the spirit
 * of <a href="http://pandas.pydata.org">Pandas</a> or
 * <a href="http://cran.r-project.org/doc/manuals/r-release/R-intro.html#Data-frames">
 * R</a> data frames.
 *
 * @param <V> the type of values in this data frame
 */
public class DataFrame<V>
implements Iterable<List<V>> {
    private final Map<String, Integer> index;
    private final Map<String, Integer> columns;
    private final BlockManager<V> data;

    public DataFrame() {
        this(new ArrayList<String>());
    }

    public DataFrame(final Collection<String> columns) {
        this(Collections.<String>emptyList(), columns, DataFrame.<V>alloc(0, columns.size()));
    }

    public DataFrame(final Collection<String> rows, final Collection<String> columns) {
        this(rows, columns, DataFrame.<V>alloc(rows.size(), columns.size()));
    }

    public DataFrame(final Collection<String> rows, final Collection<String> columns, final List<List<V>> data) {
        this.data = new BlockManager<>(data);

        this.columns = new LinkedHashMap<String, Integer>();
        int c = 0;
        for (final String column : columns) {
            if (this.columns.put(column, c++) != null) {
                throw new IllegalArgumentException("duplicate column " + column);
            }
        }

        final int len = length();
        this.index = new LinkedHashMap<String, Integer>();
        final Iterator<String> it = rows.iterator();
        for (int r = 0; r < len; r++) {
            final String row = it.hasNext() ? it.next() : String.valueOf(r);
            if (this.index.put(row, r) != null) {
                throw new IllegalArgumentException("duplicate row label " + row);
            }
        }
    }

    public DataFrame<V> add(final String column) {
        final List<V> values = new ArrayList<V>(length());
        for (int r = 0; r < values.size(); r++) {
            values.add(null);
        }
        return add(column, values);
    }

    public DataFrame<V> add(final String column, final List<V> values) {
        if (columns.put(column, data.size()) != null) {
            throw new IllegalArgumentException("column " + column + " already exists");
        }
        data.add(values);
        return this;
    }

    public DataFrame<V> drop(final String ... cols) {
        final Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            indices[i] = columns.get(cols[i]);
        }
        return drop(indices);
    }

    public DataFrame<V> drop(final Integer ... cols) {
        final List<String> columns = new ArrayList<String>(this.columns.keySet());
        final List<String> rows = new ArrayList<String>(this.index.keySet());
        final Set<Integer> removeCols = new HashSet<Integer>(Arrays.asList(cols));
        final List<List<V>> keep = new ArrayList<List<V>>(data.size() - cols.length);
        final List<String> keepCols = new ArrayList<String>(data.size() - cols.length);
        for (int i = 0; i < data.size(); i++) {
            if (!removeCols.contains(i)) {
                keep.add(data.list(i));
                keepCols.add(columns.get(i));
            }
        }
        return new DataFrame<V>(rows, keepCols, keep);
    }

    public DataFrame<V> retain(final String ... cols) {
        final Set<String> keep = new HashSet<String>(Arrays.asList(cols));
        final Set<String> todrop = new HashSet<String>();
        for (final String col : columns()) {
            if (!keep.contains(col)) {
                todrop.add(col);
            }
        }
        return drop(todrop.toArray(new String[todrop.size()]));
    }

    public DataFrame<V> retain(final Integer ... cols) {
        final Set<Integer> keep = new HashSet<Integer>(Arrays.asList(cols));
        final Set<Integer> todrop = new HashSet<Integer>();
        for (final Integer col : cols) {
            if (!keep.contains(col)) {
                todrop.add(col);
            }
        }
        return drop(todrop.toArray(new Integer[todrop.size()]));
    }

    public DataFrame<V> append(final List<V> row) {
        return append(String.valueOf(length()), row);
    }

    public DataFrame<V> append(final String name, final List<V> row) {
        if (index.put(name, length()) != null) {
            throw new IllegalArgumentException("row " + name + " already exists.");
        }

        final int len = length();
        data.reshape(data.size(), len + 1);
        for (int c = 0; c < data.size(); c++) {
            data.set(c < row.size() ? row.get(c) : null, c, len);
        }
        return this;
    }

    public int size() {
        return data.size();
    }

    public int length() {
        return data.length();
    }

    public Set<String> rows() {
        return index.keySet();
    }

    public Set<String> columns() {
        return columns.keySet();
    }

    public List<V> col(final String column) {
        return col(columns.get(column));
    }

    public List<V> col(final int c) {
        return data.list(c);
    }

    public List<V> row(final String row) {
        return row(index.get(row));
    }

    public List<V> row(final int r) {
        final List<V> row = new ArrayList<V>(data.size());
        for (int c = 0; c < data.size(); c++) {
            row.add(data.get(c, r));
        }
        return row;
    }

    public DataFrame<V> groupBy(final String ... colnames) {
        final Integer[] indices = new Integer[colnames.length];
        for (int i = 0; i < colnames.length; i++) {
            indices[i] = columns.get(colnames[i]);
        }
        return groupBy(indices);
    }

    public DataFrame<V> groupBy(final Integer ... cols) {
        final Map<Integer, Aggregator<V>> aggregators = Collections.emptyMap();
        return groupBy(new LinkedHashSet<Integer>(Arrays.asList(cols)), aggregators);
    }

    public DataFrame<V> groupBy(final Set<Integer> cols) {
        final Map<Integer, Aggregator<V>> aggregators = Collections.emptyMap();
        return groupBy(cols, aggregators);
    }

    public DataFrame<V> groupBy(final Set<Integer> cols, final Map<Integer, ? extends Aggregator<V>> aggregators) {
        final DataFrame<V> grouped = new DataFrame<V>();
        final List<String>  columnNames = new ArrayList<>(columns.keySet());
        // add key columns to new data frame column list
        for (int c = 0; c < data.size(); c++) {
            if (cols.contains(c)) {
                grouped.add(columnNames.get(c));
            }
        }

        // add value columns to new data frame column list
        final Set<Integer> valCols = new LinkedHashSet<>();
        for (int c = 0; c < data.size(); c++) {
            if (!cols.contains(c)) {
                valCols.add(c);
                grouped.add(columnNames.get(c));
            }
        }

        // determine groupings
        final Map<List<V>, List<Integer>> groups = new LinkedHashMap<List<V>, List<Integer>>();
        final int len = length();
        for (int r = 0; r < len; r++) {
            List<V> key = new ArrayList<V>(cols.size());
            for (final Integer c : cols) {
                key.add(data.get(c, r));
            }
            key = Collections.unmodifiableList(key);

            List<Integer> group = groups.get(key);
            if (group == null) {
                group = new ArrayList<Integer>();
                groups.put(key, group);
            }

            group.add(r);
        }

        // for each group
        for (final Map.Entry<List<V>, List<Integer>> entry : groups.entrySet()) {
            final List<V> row = new ArrayList<V>(entry.getKey());

            // for each value column
            for (final Integer c : valCols) {
                // collect all values
                final List<V> values = new ArrayList<V>(entry.getValue().size());
                for (final Integer r : entry.getValue()) {
                    final V value = data.get(c, r);
                    if (value != null) {
                        values.add(value);
                    }
                }

                // determine aggregator
                Aggregator<V> aggregator = aggregators.get(c);
                if (aggregator == null) {
                    if (!values.isEmpty() && values.get(0) instanceof Integer) {
                        aggregator = new Count<V>();
                    } else if (!values.isEmpty() && values.get(0) instanceof Boolean) {
                        aggregator = new CountTrue<V>();
                    } else {
                        aggregator = new Unique<V>();
                    }
                }

                // add aggregate value to the row
                row.add(aggregator.aggregate(values));
            }

            // add aggregate values row to the data frame
            final String label = String.valueOf(entry.getKey().size() == 1 ?
                                    entry.getKey().get(0) : entry.getKey());
            grouped.append(label, row);
        }

        return grouped;
    }

    public DataFrame<V> sum(final Integer ... cols) {
        final Map<Integer, Sum<V>> functions = new HashMap<>();
        for (final int c : cols) {
            functions.put(c, new Sum<V>());
        }

        final LinkedHashSet<Integer> keyCols = new LinkedHashSet<>(data.size() - cols.length);
        for (int c = 0; c < data.size(); c++) {
            if (!functions.containsKey(c)) {
                keyCols.add(c);
            }
        }

        return groupBy(keyCols, functions);
    }

    public DataFrame<V> sum(final String ... cols) {
        final Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            indices[i] = columns.get(cols[i]);
        }
        return sum(indices);
    }

    public DataFrame<V> prod(final Integer ... cols) {
        final Map<Integer, Product<V>> functions = new HashMap<>();
        for (final int c : cols) {
            functions.put(c, new Product<V>());
        }

        final LinkedHashSet<Integer> keyCols = new LinkedHashSet<>(data.size() - cols.length);
        for (int c = 0; c < data.size(); c++) {
            if (!functions.containsKey(c)) {
                keyCols.add(c);
            }
        }

        return groupBy(keyCols, functions);
    }

    public DataFrame<V> prod(final String ... cols) {
        final Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            indices[i] = columns.get(cols[i]);
        }
        return prod(indices);
    }

    public DataFrame<V> count(final Integer ... cols) {
        final Map<Integer, Count<V>> functions = new HashMap<>();
        for (final int c : cols) {
            functions.put(c, new Count<V>());
        }

        final LinkedHashSet<Integer> keyCols = new LinkedHashSet<>(data.size() - cols.length);
        for (int c = 0; c < data.size(); c++) {
            if (!functions.containsKey(c)) {
                keyCols.add(c);
            }
        }

        return groupBy(keyCols, functions);
    }

    public DataFrame<V> count(final String ... cols) {
        final Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            indices[i] = columns.get(cols[i]);
        }
        return count(indices);
    }

    public DataFrame<V> sortBy(final String ... cols) {
        final Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            if (cols[i].startsWith("-")) {
                indices[i] = -columns.get(cols[i].substring(1));
            } else {
                indices[i] = columns.get(cols[i]);
            }
        }
        return sortBy(indices);
    }

    public DataFrame<V> sortBy(final Integer ... cols) {
        final DataFrame<V> sorted = new DataFrame<V>(index.keySet(), columns.keySet());
        final Comparator<Integer> cmp = new Comparator<Integer>() {
            @Override
            @SuppressWarnings("unchecked")
            public int compare(final Integer r1, final Integer r2) {
                int result = 0;
                for (final int i : cols) {
                    final int c = Math.abs(i);
                    final Comparable<Object> o1 = Comparable.class.cast(data.get(c, r1));
                    final Comparable<Object> o2 = Comparable.class.cast(data.get(c, r2));
                    result = o1.compareTo(o2);
                    if (result != 0) {
                        result *= Integer.signum(i != 0 ? i : 1);
                        break;
                    }
                }
                return result;
            }
        };

        final List<Integer> rows = new ArrayList<>(length());
        for (int r = 0; r < length(); r++) {
            rows.add(r);
        }
        Collections.sort(rows, cmp);

        final List<String> labels = new ArrayList<>(this.index.keySet());
        for (final Integer r : rows) {
            final String label = r < labels.size() ? labels.get(r) : String.valueOf(r);
            sorted.append(label, row(r));
        }

        return sorted;
    }

    @Override
    public ListIterator<List<V>> iterator() {
        return new ListIterator<List<V>>() {
            private int r = 0;
            @Override
            public boolean hasNext() { return r < length(); }
            @Override
            public List<V> next() { return row(r++); }
            @Override
            public boolean hasPrevious() { return r > 0; }
            @Override
            public List<V> previous() { return row(--r); }
            @Override
            public int nextIndex() { return r + 1; }
            @Override
            public int previousIndex() { return r - 1; }
            @Override
            public void remove() { throw new UnsupportedOperationException(); }
            @Override
            public void set(final List<V> e) { throw new UnsupportedOperationException(); }
            @Override
            public void add(final List<V> e) { append(e); }
        };
    }

    @SuppressWarnings("unchecked")
    public <T> DataFrame<T> cast(final Class<T> cls) {
        return (DataFrame<T>)this;
    }

    public Map<String, List<V>> map() {
        final Map<String, List<V>> m = new LinkedHashMap<String, List<V>>();

        final int len = length();
        final Iterator<String> names = index.keySet().iterator();
        for (int r = 0; r < len; r++) {
            final String name = names.hasNext() ? names.next() : String.valueOf(r);
            m.put(name, row(r));
        }

        return m;
    }

    public Map<V, List<V>> map(final String key, final String value) {
        return map(columns.get(key), columns.get(value));
    }

    public Map<V, List<V>> map(final int key, final int value) {
        final Map<V, List<V>> m = new LinkedHashMap<V, List<V>>();

        final int len = length();
        for (int r = 0; r < len; r++) {
            final V name = data.get(key, r);
            List<V> values = m.get(name);
            if (values == null) {
                values = new ArrayList<V>();
                m.put(name, values);
            }
            values.add(data.get(value, r));
        }

        return m;
    }

    public DataFrame<V> unique(final String ... cols) {
        final int[] indices = new int[cols.length];
        for (int c = 0; c < cols.length; c++) {
            indices[c] = columns.get(cols[c]);
        }
        return unique(indices);
    }

    public DataFrame<V> unique(final int ... cols) {
        final DataFrame<V> unique = new DataFrame<V>(columns.keySet());
        final Set<List<V>> seen = new HashSet<List<V>>();

        final List<V> key = new ArrayList<V>(cols.length);
        final int len = length();
        for (int r = 0; r < len; r++) {
            for (final int c : cols) {
                key.add(data.get(c, r));
            }
            if (!seen.contains(key)) {
                unique.append(row(r));
                seen.add(key);
            }
            key.clear();
        }

        return unique;
    }

    @Override
    public String toString() {
        return toString(10);
    }

    public String toString(final int limit) {
        final int len = length();

        final StringBuilder sb = new StringBuilder();
        for (final String column : columns.keySet()) {
            sb.append("\t");
            sb.append(column);
        }
        sb.append("\n");

        final Iterator<String> names = index.keySet().iterator();
        for (int r = 0; r < len; r++) {
            sb.append(names.hasNext() ? names.next() : String.valueOf(r));
            for (int c = 0; c < size(); c++) {
                sb.append("\t");
                sb.append(String.valueOf(data.get(c, r)));
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

    private static <V> List<List<V>> alloc(final int rows, final int columns) {
        final List<List<V>> data = new ArrayList<List<V>>(columns);
        for (int i = 0; i < columns; i++) {
            data.add(new ArrayList<V>(rows));
        }
        return data;
    }

    public static interface Aggregator<V> {
        public V aggregate(List<V> values);
    }

    public static class Sum<V>
    implements Aggregator<V> {
        @Override
        @SuppressWarnings("unchecked")
        public V aggregate(final List<V> values) {
            V result = (V)new Double(0);
            for (final V value : values) {
                if (result instanceof Double) {
                    result = (V)new Double(Number.class.cast(result).doubleValue() + Number.class.cast(value).doubleValue());
                }
            }
            return result;
        }
    }

    public static class Product<V>
    implements Aggregator<V> {
        @Override
        @SuppressWarnings("unchecked")
        public V aggregate(final List<V> values) {
            V result = (V)new Double(1);
            for (final V value : values) {
                if (result instanceof Number) {
                    result = (V)new Double(Number.class.cast(result).doubleValue() * Number.class.cast(value).doubleValue());
                }
            }
            return result;
        }
    }

    public static class Count<V>
    implements Aggregator<V> {
        @Override
        @SuppressWarnings("unchecked")
        public V aggregate(final List<V> values) {
            return (V)new Integer(values.size());
        }
    }

    public static class CountTrue<V>
    implements Aggregator<V> {
        @Override
        @SuppressWarnings("unchecked")
        public V aggregate(final List<V> values) {
            int positive = 0;
            for (final V value : values) {
                if (value != null) {
                    if (Boolean.class.cast(value)) {
                        positive++;
                    }
                }
            }
            return (V)new Integer(positive);
        }
    }

    public static class Unique<V>
    implements Aggregator<V> {
        @Override
        public V aggregate(final List<V> values) {
            final Set<V> unique = new HashSet<V>(values);
            if (unique.size() > 1) {
                throw new IllegalArgumentException("values not unique: " + unique);
            }
            return values.isEmpty() ? null : values.get(0);
        }
    }
}
