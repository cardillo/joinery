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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import joinery.impl.Aggregation;
import joinery.impl.BlockManager;
import joinery.impl.Conversion;
import joinery.impl.Grouping;
import joinery.impl.Index;
import joinery.impl.Inspection;
import joinery.impl.Selection;
import joinery.impl.Serialization;
import joinery.impl.Views;

import com.codahale.metrics.annotation.Timed;

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
    private final Index index;
    private final Index columns;
    private final BlockManager<V> data;
    private final Grouping groups;

    /**
     * Construct an empty data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>();
     * > df.isEmpty();
     * true }</pre>
     */
    public DataFrame() {
        this(Collections.<String>emptyList());
    }

    public DataFrame(final String ... columns) {
        this(Arrays.asList(columns));
    }

    public DataFrame(final Collection<String> columns) {
        this(Collections.<String>emptyList(), columns, Collections.<List<V>>emptyList());
    }

    public DataFrame(final Collection<String> rows, final Collection<String> columns) {
        this(rows, columns, Collections.<List<V>>emptyList());
    }

    public DataFrame(final List<List<V>> data) {
        this(Collections.<String>emptyList(), Collections.<String>emptyList(), data);
    }

    public DataFrame(final Collection<String> rows, final Collection<String> columns, final List<List<V>> data) {
        final BlockManager<V> mgr = new BlockManager<>(data);
        mgr.reshape(
                Math.max(mgr.size(), columns.size()),
                Math.max(mgr.length(), rows.size())
            );

        this.data = mgr;
        this.columns = new Index(columns, mgr.size());
        this.index = new Index(rows, mgr.length());
        this.groups = new Grouping();
    }

    private DataFrame(final Index index, final Index columns, final BlockManager<V> data, final Grouping groups) {
        this.index = index;
        this.columns = columns;
        this.data = data;
        this.groups = groups;
    }

    public DataFrame<V> add(final String column) {
        final List<V> values = new ArrayList<V>(length());
        for (int r = 0; r < values.size(); r++) {
            values.add(null);
        }
        return add(column, values);
    }

    public DataFrame<V> add(final String column, final List<V> values) {
        columns.add(column, data.size());
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
        final List<String> columns = new ArrayList<String>(this.columns.names());
        final List<String> rows = new ArrayList<String>(this.index.names());
        final Set<Integer> removeCols = new HashSet<Integer>(Arrays.asList(cols));
        final List<List<V>> keep = new ArrayList<List<V>>(data.size() - cols.length);
        final List<String> keepCols = new ArrayList<String>(data.size() - cols.length);
        for (int i = 0; i < data.size(); i++) {
            if (!removeCols.contains(i)) {
                keep.add(col(i));
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

    @Timed
    public DataFrame<V> append(final String name, final List<V> row) {
        final int len = length();
        index.add(name, len);
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

    public boolean isEmpty() {
        return length() == 0;
    }

    public Set<String> index() {
        return index.names();
    }

    public Set<String> columns() {
        return columns.names();
    }

    public V get(final String row, final String col) {
        return get(index.get(row), columns.get(col));
    }

    public V get(final int row, final int col) {
        return data.get(col, row);
    }

    public void set(final String row, final String col, final V value) {
        set(index.get(row), columns.get(col), value);
    }

    public void set(final int row, final int col, final V value) {
        data.set(value, col, row);
    }

    public List<V> col(final String column) {
        return col(columns.get(column));
    }

    public List<V> col(final int c) {
        return new Views.SeriesListView<>(this, c, true);
    }

    public List<V> row(final String row) {
        return row(index.get(row));
    }

    public List<V> row(final int r) {
        return new Views.SeriesListView<>(this, r, false);
    }

    public DataFrame<V> select(final Predicate<V> predicate) {
        final BitSet selected = Selection.select(this, predicate);
        return new DataFrame<>(
                Selection.select(index, selected),
                columns,
                Selection.select(data, selected),
                new Grouping()
            );
    }

    public DataFrame<V> transpose() {
        return new DataFrame<>(
                index.names(),
                columns.names(),
                new Views.ListView<>(this, true)
            );
    }

    public <U> DataFrame<U> transform(final Function<V, U> transform) {
        return new DataFrame<>(
                index.names(),
                columns.names(),
                new Views.TransformedView<V, U>(this, transform, false)
            );
    }

    public DataFrame<V> convert() {
        Conversion.convert(this);
        return this;
    }

    @SafeVarargs
    public final DataFrame<V> convert(final Class<? extends V> ... columnTypes) {
        Conversion.convert(this, columnTypes);
        return this;
    }

    public DataFrame<Boolean> isnull() {
        return Conversion.isnull(this);
    }

    public DataFrame<Boolean> notnull() {
        return Conversion.notnull(this);
    }

    public Object[] toArray() {
        return toArray(new Object[size() * length()]);
    }

    public <U> U[] toArray(final U[] array) {
        return new Views.FlatView<>(this).toArray(array);
    }

    public Object toArray(final Class<?> cls) {
        if (cls.getComponentType() == null) {
            throw new IllegalArgumentException("class must be an array class");
        }

        final int size = size();
        final int len = length();
        final Object a = Array.newInstance(cls.getComponentType(), size * len);

        for (int i = 0; i < size * len; i++) {
            Array.set(a, i, data.get(i / size, i % len));
        }

        return a;
    }

    @Timed
    public DataFrame<V> groupBy(final String ... colnames) {
        final int[] indices = new int[colnames.length];
        for (int i = 0; i < colnames.length; i++) {
            indices[i] = columns.get(colnames[i]);
        }
        return groupBy(indices);
    }

    @Timed
    public DataFrame<V> groupBy(final int ... cols) {
        return new DataFrame<>(
                index,
                columns,
                data,
                new Grouping(this, cols)
            );
    }

    @Timed
    public DataFrame<V> groupBy(final KeyFunction<V> function) {
        return new DataFrame<>(
                index,
                columns,
                data,
                new Grouping(this, function)
            );
    }

    public Map<Object, DataFrame<V>> groups() {
        final Map<Object, DataFrame<V>> grouped = new LinkedHashMap<>();
        for (final Map.Entry<Object, BitSet> entry : groups) {
            final BitSet selected = entry.getValue();
            grouped.put(entry.getKey(), new DataFrame<V>(
                    Selection.select(index, selected),
                    columns,
                    Selection.select(data, selected),
                    new Grouping()
                ));
        }
        return grouped;
    }

    public <U> DataFrame<U> apply(final Aggregate<V, U> function) {
        return groups.apply(this, function);
    }

    @Timed
    public DataFrame<V> count() {
        return groups.apply(this, new Aggregation.Count<V>());
    }

    @Timed
    public DataFrame<? super Double> sum() {
        return groups.apply(this, new Aggregation.Sum<V>());
    }

    @Timed
    public DataFrame<? super Double> prod() {
        return groups.apply(this, new Aggregation.Product<V>());
    }

    @Timed
    public DataFrame<? super Double> mean() {
        return groups.apply(this, new Aggregation.Mean<V>());
    }

    @Timed
    public DataFrame<? super Double> stddev() {
        return groups.apply(this, new Aggregation.StdDev<V>());
    }

    @Timed
    public DataFrame<? super Double> var() {
        return groups.apply(this, new Aggregation.Variance<V>());
    }

    @Timed
    public DataFrame<? super Double> skew() {
        return groups.apply(this, new Aggregation.Skew<V>());
    }

    @Timed
    public DataFrame<? super Double> kurt() {
        return groups.apply(this, new Aggregation.Kurtosis<V>());
    }

    @Timed
    public DataFrame<? super Double> min() {
        return groups.apply(this, new Aggregation.Min<V>());
    }

    @Timed
    public DataFrame<? super Double> max() {
        return groups.apply(this, new Aggregation.Max<V>());
    }

    @Timed
    public DataFrame<? super Double> median() {
        return groups.apply(this, new Aggregation.Median<V>());
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

    @Timed
    public DataFrame<V> sortBy(final Integer ... cols) {
        final DataFrame<V> sorted = new DataFrame<V>(columns.names());
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

        final List<String> labels = new ArrayList<>(this.index.names());
        for (final Integer r : rows) {
            final String label = r < labels.size() ? labels.get(r) : String.valueOf(r);
            sorted.append(label, row(r));
        }

        return sorted;
    }

    public List<Class<?>> types() {
        return Inspection.types(this);
    }

    public DataFrame<V> numeric() {
        final BitSet numeric = Inspection.numeric(this);
        final Set<String> keep = Selection.select(columns, numeric).names();
        return retain(keep.toArray(new String[keep.size()]));
    }

    public DataFrame<V> nonnumeric() {
        final BitSet nonnumeric = Inspection.nonnumeric(this);
        final Set<String> keep = Selection.select(columns, nonnumeric).names();
        return retain(keep.toArray(new String[keep.size()]));
    }

    @Override
    public ListIterator<List<V>> iterator() {
        return iterrows();
    }

    public ListIterator<List<V>> iterrows() {
        return new Views.ListView<>(this, true).listIterator();
    }

    public ListIterator<List<V>> itercols() {
        return new Views.ListView<>(this, false).listIterator();
    }

    public ListIterator<Map<String, V>> itermap() {
        return new Views.MapView<>(this, true).listIterator();
    }

    public ListIterator<V> itervalues() {
        return new Views.FlatView<>(this).listIterator();
    }

    @SuppressWarnings("unchecked")
    public <T> DataFrame<T> cast(final Class<T> cls) {
        return (DataFrame<T>)this;
    }

    public Map<String, List<V>> map() {
        final Map<String, List<V>> m = new LinkedHashMap<String, List<V>>();

        final int len = length();
        final Iterator<String> names = index.names().iterator();
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
        final DataFrame<V> unique = new DataFrame<V>(columns.names());
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

    public static final DataFrame<Object> readCsv(final String file)
    throws IOException {
        return Serialization.readCsv(file);
    }

    public static final DataFrame<Object> readCsv(final InputStream input)
    throws IOException {
        return Serialization.readCsv(input);
    }

    public final void writeCsv(final String file)
    throws IOException {
        Serialization.writeCsv(this, new FileOutputStream(file));
    }

    public final void writeCsv(final OutputStream output)
    throws IOException {
        Serialization.writeCsv(this, output);
    }

    public final String toString(final int limit) {
        return Serialization.toString(this, limit);
    }

    @Override
    public String toString() {
        return toString(10);
    }

    public interface Function<I, O> {
        O apply(I value);
    }

    public interface KeyFunction<I>
    extends Function<List<I>, Object> { }

    public interface Aggregate<I, O>
    extends Function<List<I>, O> { }

    public interface Predicate<I>
    extends Function<List<I>, Boolean> { }
}
