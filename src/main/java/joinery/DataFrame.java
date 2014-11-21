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

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
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
    private final Map<String, Integer> rows;
    private final Map<String, Integer> columns;
    private final List<List<V>> data;

    public DataFrame() {
        this(new ArrayList<String>());
    }

    public DataFrame(Collection<String> columns) {
        this(Collections.<String>emptyList(), columns, DataFrame.<V>alloc(0, columns.size()));
    }

    public DataFrame(Collection<String> rows, Collection<String> columns) {
        this(rows, columns, DataFrame.<V>alloc(rows.size(), columns.size()));
    }

    public DataFrame(Collection<String> rows, Collection<String> columns, List<List<V>> data) {
        this.data = data;

        this.columns = new LinkedHashMap<String, Integer>();
        int c = 0;
        for (String column : columns) {
            if (this.columns.put(column, c++) != null) {
                throw new IllegalArgumentException("duplicate column " + column);
            }
        }

        int len = length();
        this.rows = new LinkedHashMap<String, Integer>();
        Iterator<String> it = rows.iterator();
        for (int r = 0; r < len; r++) {
            String row = it.hasNext() ? it.next() : String.valueOf(r);
            if (this.rows.put(row, r) != null) {
                throw new IllegalArgumentException("duplicate row label " + row);
            }
        }
    }

    public DataFrame<V> add(String column) {
        List<V> values = new ArrayList<V>(length());
        for (int r = 0; r < values.size(); r++) {
            values.add(null);
        }
        return add(column, values);
    }

    public DataFrame<V> add(String column, List<V> values) {
        if (columns.put(column, data.size()) != null) {
            throw new IllegalArgumentException("column " + column + " already exists");
        }
        data.add(values);
        return this;
    }

    public DataFrame<V> drop(String ... cols) {
        Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            indices[i] = columns.get(cols[i]);
        }
        return drop(indices);
    }

    public DataFrame<V> drop(Integer ... cols) {
        List<String> columns = new ArrayList<String>(this.columns.keySet());
        List<String> rows = new ArrayList<String>(this.rows.keySet());
        Set<Integer> removeCols = new HashSet<Integer>(Arrays.asList(cols));
        List<List<V>> keep = new ArrayList<List<V>>(data.size() - cols.length);
        List<String> keepCols = new ArrayList<String>(data.size() - cols.length);
        for (int i = 0; i < data.size(); i++) {
            if (!removeCols.contains(i)) {
                keep.add(data.get(i));
                keepCols.add(columns.get(i));
            }
        }
        return new DataFrame<V>(rows, keepCols, keep);
    }

    public DataFrame<V> retain(String ... cols) {
        Set<String> keep = new HashSet<String>(Arrays.asList(cols));
        Set<String> todrop = new HashSet<String>();
        for (String col : columns()) {
            if (!keep.contains(col)) {
                todrop.add(col);
            }
        }
        return drop(todrop.toArray(new String[todrop.size()]));
    }

    public DataFrame<V> retain(Integer ... cols) {
        Set<Integer> keep = new HashSet<Integer>(Arrays.asList(cols));
        Set<Integer> todrop = new HashSet<Integer>();
        for (Integer col : cols) {
            if (!keep.contains(col)) {
                todrop.add(col);
            }
        }
        return drop(todrop.toArray(new Integer[todrop.size()]));
    }

    public DataFrame<V> append(List<V> row) {
        return append(String.valueOf(length()), row);
    }

    public DataFrame<V> append(String name, List<V> row) {
        if (rows.put(name, length()) != null) {
            throw new IllegalArgumentException("row " + name + " already exists.");
        }
        for (int i = 0; i < data.size(); i++) {
            data.get(i).add(i < row.size() ? row.get(i) : null);
        }
        return this;
    }

    public int size() {
        return data.size();
    }

    public int length() {
        return data.isEmpty() ? 0 : data.get(0).size();
    }

    public Set<String> rows() {
        return rows.keySet();
    }

    public Set<String> columns() {
        return columns.keySet();
    }

    public List<V> col(String column) {
        return col(columns.get(column));
    }

    public List<V> col(int c) {
        return data.get(c);
    }

    public List<V> row(String row) {
        return row(rows.get(row));
    }

    public List<V> row(int r) {
        List<V> row = new ArrayList<V>(data.size());
        for (int c = 0; c < data.size(); c++) {
            row.add(data.get(c).get(r));
        }
        return row;
    }

    public DataFrame<V> groupBy(String ... colnames) {
        Integer[] indices = new Integer[colnames.length];
        for (int i = 0; i < colnames.length; i++) {
            indices[i] = columns.get(colnames[i]);
        }
        return groupBy(indices);
    }

    public DataFrame<V> groupBy(Integer ... cols) {
        Map<Integer, Aggregator<V>> aggregators = Collections.emptyMap();
        return groupBy(new LinkedHashSet<Integer>(Arrays.asList(cols)), aggregators);
    }

    public DataFrame<V> groupBy(Set<Integer> cols) {
        Map<Integer, Aggregator<V>> aggregators = Collections.emptyMap();
        return groupBy(cols, aggregators);
    }

    public DataFrame<V> groupBy(Set<Integer> cols, Map<Integer, ? extends Aggregator<V>> aggregators) {
        DataFrame<V> grouped = new DataFrame<V>();
        List<String>  columnNames = new ArrayList<>(columns.keySet());
        // add key columns to new data frame column list
        for (int c = 0; c < data.size(); c++) {
            if (cols.contains(c)) {
                grouped.add(columnNames.get(c));
            }
        }

        // add value columns to new data frame column list
        Set<Integer> valCols = new LinkedHashSet<>();
        for (int c = 0; c < data.size(); c++) {
            if (!cols.contains(c)) {
                valCols.add(c);
                grouped.add(columnNames.get(c));
            }
        }

        // determine groupings
        Map<List<V>, List<Integer>> groups = new LinkedHashMap<List<V>, List<Integer>>();
        int len = length();
        for (int r = 0; r < len; r++) {
            List<V> key = new ArrayList<V>(cols.size());
            for (Integer c : cols) {
                key.add(data.get(c).get(r));
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
        for (Map.Entry<List<V>, List<Integer>> entry : groups.entrySet()) {
            List<V> row = new ArrayList<V>(entry.getKey());

            // for each value column
            for (Integer c : valCols) {
                // collect all values
                List<V> values = new ArrayList<V>(entry.getValue().size());
                for (Integer r : entry.getValue()) {
                    V value = data.get(c).get(r);
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
            String label = String.valueOf(entry.getKey().size() == 1 ?
                                    entry.getKey().get(0) : entry.getKey());
            grouped.append(label, row);
        }

        return grouped;
    }

    public DataFrame<V> sum(final Integer ... cols) {
        Map<Integer, Sum<V>> functions = new HashMap<>();
        for (int c : cols) {
            functions.put(c, new Sum<V>());
        }

        LinkedHashSet<Integer> keyCols = new LinkedHashSet<>(data.size() - cols.length);
        for (int c = 0; c < data.size(); c++) {
            if (!functions.containsKey(c)) {
                keyCols.add(c);
            }
        }

        return groupBy(keyCols, functions);
    }

    public DataFrame<V> sum(String ... cols) {
        Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            indices[i] = columns.get(cols[i]);
        }
        return sum(indices);
    }

    public DataFrame<V> prod(final Integer ... cols) {
        Map<Integer, Product<V>> functions = new HashMap<>();
        for (int c : cols) {
            functions.put(c, new Product<V>());
        }

        LinkedHashSet<Integer> keyCols = new LinkedHashSet<>(data.size() - cols.length);
        for (int c = 0; c < data.size(); c++) {
            if (!functions.containsKey(c)) {
                keyCols.add(c);
            }
        }

        return groupBy(keyCols, functions);
    }

    public DataFrame<V> prod(String ... cols) {
        Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            indices[i] = columns.get(cols[i]);
        }
        return prod(indices);
    }

    public DataFrame<V> count(final Integer ... cols) {
        Map<Integer, Count<V>> functions = new HashMap<>();
        for (int c : cols) {
            functions.put(c, new Count<V>());
        }

        LinkedHashSet<Integer> keyCols = new LinkedHashSet<>(data.size() - cols.length);
        for (int c = 0; c < data.size(); c++) {
            if (!functions.containsKey(c)) {
                keyCols.add(c);
            }
        }

        return groupBy(keyCols, functions);
    }

    public DataFrame<V> count(String ... cols) {
        Integer[] indices = new Integer[cols.length];
        for (int i = 0; i < cols.length; i++) {
            indices[i] = columns.get(cols[i]);
        }
        return count(indices);
    }

    public DataFrame<V> sortBy(String ... cols) {
        Integer[] indices = new Integer[cols.length];
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
        DataFrame<V> sorted = new DataFrame<V>(rows.keySet(), columns.keySet());
        Comparator<Integer> cmp = new Comparator<Integer>() {
            @Override
            @SuppressWarnings("unchecked")
            public int compare(Integer r1, Integer r2) {
                int result = 0;
                for (int i : cols) {
                    int c = Math.abs(i);
                    Comparable<Object> o1 = Comparable.class.cast(data.get(c).get(r1));
                    Comparable<Object> o2 = Comparable.class.cast(data.get(c).get(r2));
                    result = o1.compareTo(o2);
                    if (result != 0) {
                        result *= Integer.signum(i != 0 ? i : 1);
                        break;
                    }
                }
                return result;
            }
        };

        List<Integer> rows = new ArrayList<>(length());
        for (int r = 0; r < length(); r++) {
            rows.add(r);
        }
        Collections.sort(rows, cmp);

        List<String> labels = new ArrayList<>(this.rows.keySet());
        for (Integer r : rows) {
            String label = r < labels.size() ? labels.get(r) : String.valueOf(r);
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
            public void set(List<V> e) { throw new UnsupportedOperationException(); }
            @Override
            public void add(List<V> e) { append(e); }
        };
    }

    @SuppressWarnings("unchecked")
    public <T> DataFrame<T> cast(Class<T> cls) {
        return (DataFrame<T>)this;
    }

    public Map<String, List<V>> map() {
        Map<String, List<V>> m = new LinkedHashMap<String, List<V>>();

        int len = length();
        Iterator<String> names = rows.keySet().iterator();
        for (int r = 0; r < len; r++) {
            String name = names.hasNext() ? names.next() : String.valueOf(r);
            m.put(name, row(r));
        }

        return m;
    }

    public Map<V, List<V>> map(String key, String value) {
        return map(columns.get(key), columns.get(value));
    }

    public Map<V, List<V>> map(int key, int value) {
        Map<V, List<V>> m = new LinkedHashMap<V, List<V>>();

        int len = length();
        for (int r = 0; r < len; r++) {
            V name = data.get(key).get(r);
            List<V> values = m.get(name);
            if (values == null) {
                values = new ArrayList<V>();
                m.put(name, values);
            }
            values.add(data.get(value).get(r));
        }

        return m;
    }

    public DataFrame<V> unique(String ... cols) {
        int[] indices = new int[cols.length];
        for (int c = 0; c < cols.length; c++) {
            indices[c] = columns.get(cols[c]);
        }
        return unique(indices);
    }

    public DataFrame<V> unique(int ... cols) {
        DataFrame<V> unique = new DataFrame<V>(columns.keySet());
        Set<List<V>> seen = new HashSet<List<V>>();

        List<V> key = new ArrayList<V>(cols.length);
        int len = length();
        for (int r = 0; r < len; r++) {
            for (int c : cols) {
                key.add(data.get(c).get(r));
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

    public String toString(int limit) {
        int len = length();

        StringBuilder sb = new StringBuilder();
        for (String column : columns.keySet()) {
            sb.append("\t");
            sb.append(column);
        }
        sb.append("\n");

        Iterator<String> names = rows.keySet().iterator();
        for (int r = 0; r < len; r++) {
            sb.append(names.hasNext() ? names.next() : String.valueOf(r));
            for (int c = 0; c < size(); c++) {
                sb.append("\t");
                sb.append(String.valueOf(data.get(c).get(r)));
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

    private static <V> List<List<V>> alloc(int rows, int columns) {
        List<List<V>> data = new ArrayList<List<V>>(columns);
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
        public V aggregate(List<V> values) {
            V result = (V)new Double(0);
            for (V value : values) {
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
        public V aggregate(List<V> values) {
            V result = (V)new Double(1);
            for (V value : values) {
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
        public V aggregate(List<V> values) {
            return (V)new Integer(values.size());
        }
    }

    public static class CountTrue<V>
    implements Aggregator<V> {
        @Override
        @SuppressWarnings("unchecked")
        public V aggregate(List<V> values) {
            int positive = 0;
            for (V value : values) {
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
        public V aggregate(List<V> values) {
            Set<V> unique = new HashSet<V>(values);
            if (unique.size() > 1) {
                throw new IllegalArgumentException("values not unique: " + unique);
            }
            return values.isEmpty() ? null : values.get(0);
        }
    }
}
