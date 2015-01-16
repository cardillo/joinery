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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import joinery.impl.Aggregation;
import joinery.impl.BlockManager;
import joinery.impl.Comparing;
import joinery.impl.Conversion;
import joinery.impl.Grouping;
import joinery.impl.Index;
import joinery.impl.Inspection;
import joinery.impl.Pivoting;
import joinery.impl.Plotting;
import joinery.impl.Selection;
import joinery.impl.Serialization;
import joinery.impl.Shaping;
import joinery.impl.Sorting;
import joinery.impl.Views;

import com.codahale.metrics.annotation.Timed;

/**
 * A data frame implementation in the spirit
 * of <a href="http://pandas.pydata.org">Pandas</a> or
 * <a href="http://cran.r-project.org/doc/manuals/r-release/R-intro.html#Data-frames">
 * R</a> data frames.
 *
 * <p>Below is a simple motivating example.  When working in Java,
 * data operations like the following should be easy.  The code
 * below retrieves the S&P 500 daily market data for 2008 from
 * Yahoo! Finance and returns the average monthly close for
 * the three top months of the year.</p>
 *
 * <pre> {@code
 * > DataFrame.readCsv(String.format(
 * >         "%s?s=%s&a=%d&b=%d&c=%d&d=%d&e=%d&f=%d",
 * >         "http://real-chart.finance.yahoo.com/table.csv",
 * >         "^GSPC",           // symbol for S&P 500
 * >         0, 2, 2008,        // start date
 * >         11, 31, 2008       // end date
 * >     ))
 * >     .retain("Date", "Close")
 * >     .groupBy(new KeyFunction<Object>() {
 * >         public Object apply(List<Object> row) {
 * >             return Date.class.cast(row.get(0)).getMonth();
 * >         }
 * >     })
 * >     .mean()
 * >     .sortBy("Close")
 * >     .tail(3)
 * >     .transform(new Function<Object, Number>() {
 * >         public Number apply(Object value) {
 * >             return Number.class.cast(value).intValue();
 * >         }
 * >     })
 * >     .col("Close");
 * [1370, 1378, 1403] }</pre>
 *
 * <p>Taking each step in turn:
 *   <ol>
 *     <li>{@link #readCsv(String)} reads csv data from files and urls</li>
 *     <li>{@link #retain(String...)} is used to
 *         eliminate columns that are not needed</li>
 *     <li>{@link #groupBy(KeyFunction)} with a key function
 *         is used to group the rows by month</li>
 *     <li>{@link #mean()} calculates the average close for each month</li>
 *     <li>{@link #sortBy(String...)} orders the rows according
 *         to average closing price</li>
 *     <li>{@link #tail(int)} returns the last three rows
 *         (alternatively, sort in descending order and use head)</li>
 *     <li>{@link #transform(Function)} is used to convert the
 *         closing prices to integers (this is purely to ease
 *         comparisons for verifying the results</li>
 *     <li>finally, {@link #col(String)} is used to
 *         extract the values as a list</li>
 *   </ol>
 * </p>
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

    /**
     * Construct an empty data frame with the specified columns.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.columns();
     * [name, value] }</pre>
     *
     * @param columns the data frame column names.
     */
    public DataFrame(final String ... columns) {
        this(Arrays.asList(columns));
    }

    /**
     * Construct an empty data frame with the specified columns.
     *
     * <pre> {@code
     * > List<String> columns = new ArrayList<>();
     * > columns.add("name");
     * > columns.add("value");
     * > DataFrame<Object> df = new DataFrame<>(columns);
     * > df.columns();
     * [name, value] }</pre>
     *
     * @param columns the data frame column names.
     */
    public DataFrame(final Collection<String> columns) {
        this(Collections.<String>emptyList(), columns, Collections.<List<V>>emptyList());
    }

    /**
     * Construct a data frame containing the specified rows and columns.
     *
     * <pre> {@code
     * > List<String> rows = Arrays.asList("row1", "row2", "row3");
     * > List<String> columns = Arrays.asList("col1", "col2");
     * > DataFrame<Object> df = new DataFrame<>(rows, columns);
     * > df.get("row1", "col1");
     * null }</pre>
     *
     * @param index the row names
     * @param columns the column names
     */
    public DataFrame(final Collection<String> index, final Collection<String> columns) {
        this(index, columns, Collections.<List<V>>emptyList());
    }

    /**
     * Construct a data frame from the specified list of columns.
     *
     * <pre> {@code
     * > List<List<Object>> data = Arrays.asList(
     * >       Arrays.<Object>asList("alpha", "bravo", "charlie"),
     * >       Arrays.<Object>asList(1, 2, 3)
     * > );
     * > DataFrame<Object> df = new DataFrame<>(data);
     * > df.row(0);
     * [alpha, 1] }</pre>
     *
     * @param data a list of columns containing the data elements.
     */
    public DataFrame(final List<List<V>> data) {
        this(Collections.<String>emptyList(), Collections.<String>emptyList(), data);
    }

    /**
     * Construct a new data frame using the specified data and indices.
     *
     * @param index the row names
     * @param columns the column names
     * @param data the data
     */
    public DataFrame(final Collection<String> index, final Collection<String> columns, final List<List<V>> data) {
        final BlockManager<V> mgr = new BlockManager<>(data);
        mgr.reshape(
                Math.max(mgr.size(), columns.size()),
                Math.max(mgr.length(), index.size())
            );

        this.data = mgr;
        this.columns = new Index(columns, mgr.size());
        this.index = new Index(index, mgr.length());
        this.groups = new Grouping();
    }

    private DataFrame(final Index index, final Index columns, final BlockManager<V> data, final Grouping groups) {
        this.index = index;
        this.columns = columns;
        this.data = data;
        this.groups = groups;
    }

    /**
     * Add new columns to the data frame.
     *
     * Any existing rows will have {@code null} values for the new columns.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>();
     * > df.add("value");
     * > df.columns();
     * [value] }</pre>
     *
     * @param columns the new column names
     * @return the data frame with the columns added
     */
    public DataFrame<V> add(final String ... columns) {
        for (final String column : columns) {
            final List<V> values = new ArrayList<V>(length());
            for (int r = 0; r < values.size(); r++) {
                values.add(null);
            }
            add(column, values);
        }
        return this;
    }

    /**
     * Add a new column to the data frame containing the value provided.
     *
     * Any existing rows with indices greater than the size of the
     * specified column data will have {@code null} values for the new column.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>();
     * > df.add("value", Arrays.<Object>asList(1));
     * > df.columns();
     * [value] }</pre>
     *
     * @param column the new column names
     * @param values the new column values
     * @return the data frame with the column added
     */
    public DataFrame<V> add(final String column, final List<V> values) {
        columns.add(column, data.size());
        data.add(values);
        return this;
    }

    /**
     * Create a new data frame by leaving out the specified columns.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value", "category");
     * > df.drop("category").columns();
     * [name, value] }</pre>
     *
     * @param cols the names of columns to be removed
     * @return a shallow copy of the data frame with the columns removed
     */
    public DataFrame<V> drop(final String ... cols) {
        return drop(columns.indices(cols));
    }

    /**
     * Create a new data frame by leaving out the specified columns.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value", "category");
     * > df.drop(2).columns();
     * [name, value] }</pre>
     *
     * @param cols the indices of the columns to be removed
     * @return a shallow copy of the data frame with the columns removed
     */
    public DataFrame<V> drop(final int ... cols) {
        final List<String> colnames = new ArrayList<>(columns.names());
        final List<String> todrop = new ArrayList<>(cols.length);
        for (final int col : cols) {
            todrop.add(colnames.get(col));
        }
        colnames.removeAll(todrop);

        final List<List<V>> keep = new ArrayList<>(colnames.size());
        for (final String col : colnames) {
            keep.add(col(col));
        }

        return new DataFrame<>(
                index.names(),
                colnames,
                keep
            );
    }

    /**
     * Create a new data frame containing only the specified columns.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value", "category");
     * > df.retain("name", "category").columns();
     * [name, category] }</pre>
     *
     * @param cols the columns to include in the new data frame
     * @return a new data frame containing only the specified columns
     */
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

    /**
     * Create a new data frame containing only the specified columns.
     *
     * <pre> {@code
     *  DataFrame<Object> df = new DataFrame<>("name", "value", "category");
     *  df.retain(0, 2).columns();
     * [name, category] }</pre>
     *
     * @param cols the columns to include in the new data frame
     * @return a new data frame containing only the specified columns
     */
    public DataFrame<V> retain(final int ... cols) {
        final Set<Integer> keep = new HashSet<Integer>();
        for (final int c : cols) keep.add(c);
        final int[] todrop = new int[size() - keep.size()];
        int i = 0;
        for (final Integer col : cols) {
            if (!keep.contains(col)) {
                todrop[i++] = col;
            }
        }
        return drop(todrop);
    }

    public DataFrame<V> reindex(final int ... cols) {
        return Index.reindex(this, cols);
    }

    public DataFrame<V> reindex(final String ... cols) {
        return reindex(columns.indices(cols));
    }

    /**
     * Append rows to the data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append(Arrays.<Object>asList("alpha", 1));
     * > df.append(Arrays.<Object>asList("bravo", 2));
     * > df.length();
     * 2 }</pre>
     *
     * @param row the row to append
     * @return the data frame with the new data appended
     */
    public DataFrame<V> append(final List<V> row) {
        return append(String.valueOf(length()), row);
    }

    /**
     * Append rows indexed by the the specified name to the data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append("row1", Arrays.<Object>asList("alpha", 1));
     * > df.append("row2", Arrays.<Object>asList("bravo", 2));
     * > df.index();
     * [row1, row2] }</pre>
     *
     * @param name the row name to add to the index
     * @param row the row to append
     * @return the data frame with the new data appended
     */
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

    public DataFrame<V> reshape(final int rows, final int cols) {
        return Shaping.reshape(this, rows, cols);
    }

    public DataFrame<V> reshape(final Collection<String> rows, final Collection<String> cols) {
        return Shaping.reshape(this, rows, cols);
    }

    /**
     * Return the size (number of columns) of the data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.size();
     * 2 }</pre>
     *
     * @return the number of columns
     */
    public int size() {
        return data.size();
    }

    /**
     * Return the length (number of rows) of the data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append(Arrays.<Object>asList("alpha", 1));
     * > df.append(Arrays.<Object>asList("bravo", 2));
     * > df.append(Arrays.<Object>asList("charlie", 3));
     * > df.length();
     * 3 }</pre>
     *
     * @return the number of columns
     */
    public int length() {
        return data.length();
    }

    /**
     * Return {@code true} if the data frame contains no data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>();
     * > df.isEmpty();
     * true }</pre>
     *
     * @return the number of columns
     */
    public boolean isEmpty() {
        return length() == 0;
    }

    /**
     * Return the index names for the data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append("row1", Arrays.<Object>asList("one", 1));
     * > df.index();
     * [row1] }</pre>
     *
     * @return the index names
     */
    public Set<String> index() {
        return index.names();
    }

    /**
     * Return the column names for the data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.columns();
     * [name, value] }</pre>
     *
     * @return the column names
     */
    public Set<String> columns() {
        return columns.names();
    }

    /**
     * Return the value located by the (row, column) names.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<Object>(
     * >     Arrays.asList("row1", "row2", "row3"),
     * >     Arrays.asList("name", "value"),
     * >     Arrays.asList(
     * >         Arrays.<Object>asList("alpha", "bravo", "charlie"),
     * >         Arrays.<Object>asList(10, 20, 30)
     * >     )
     * > );
     * > df.get("row2", "name");
     * bravo }</pre>
     *
     * @param row the row name
     * @param col the column name
     * @return the value
     */
    public V get(final String row, final String col) {
        return get(index.get(row), columns.get(col));
    }

    /**
     * Return the value located by the (row, column) coordinates.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<Object>(
     * >     Collections.<String>emptyList(),
     * >     Arrays.asList("name", "value"),
     * >     Arrays.asList(
     * >         Arrays.<Object>asList("alpha", "bravo", "charlie"),
     * >         Arrays.<Object>asList(10, 20, 30)
     * >     )
     * > );
     * > df.get(1, 0);
     * bravo }</pre>
     *
     * @param row the row index
     * @param col the column index
     * @return the value
     */
    public V get(final int row, final int col) {
        return data.get(col, row);
    }

    /**
     * Set the value located by the names (row, column).
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >        Arrays.asList("row1", "row2"),
     * >        Arrays.asList("col1", "col2")
     * >     );
     * > df.set("row1", "col2", new Integer(7));
     * > df.col(1);
     * [7, null] }</pre>
     *
     * @param row the row name
     * @param col the column name
     * @param value the new value
     */
    public void set(final String row, final String col, final V value) {
        set(index.get(row), columns.get(col), value);
    }

    /**
     * Set the value located by the coordinates (row, column).
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >        Arrays.asList("row1", "row2"),
     * >        Arrays.asList("col1", "col2")
     * >     );
     * > df.set(1, 0, new Integer(7));
     * > df.col(0);
     * [null, 7] }</pre>
     *
     * @param row the row index
     * @param col the column index
     * @param value the new value
     */
    public void set(final int row, final int col, final V value) {
        data.set(value, col, row);
    }

    /**
     * Return a data frame column as a list.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.<String>emptyList(),
     * >         Arrays.asList("name", "value"),
     * >         Arrays.asList(
     * >             Arrays.<Object>asList("alpha", "bravo", "charlie"),
     * >             Arrays.<Object>asList(1, 2, 3)
     * >         )
     * >     );
     * > df.col("value");
     * [1, 2, 3] }</pre>
     *
     * @param column the column name
     * @return the list of values
     */
    public List<V> col(final String column) {
        return col(columns.get(column));
    }

    /**
     * Return a data frame column as a list.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.<String>emptyList(),
     * >         Arrays.asList("name", "value"),
     * >         Arrays.asList(
     * >             Arrays.<Object>asList("alpha", "bravo", "charlie"),
     * >             Arrays.<Object>asList(1, 2, 3)
     * >         )
     * >     );
     * > df.col(1);
     * [1, 2, 3] }</pre>
     *
     * @param column the column index
     * @return the list of values
     */
    public List<V> col(final int column) {
        return new Views.SeriesListView<>(this, column, true);
    }

    /**
     * Return a data frame row as a list.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Arrays.asList("row1", "row2", "row3"),
     * >         Collections.<String>emptyList(),
     * >         Arrays.asList(
     * >             Arrays.<Object>asList("alpha", "bravo", "charlie"),
     * >             Arrays.<Object>asList(1, 2, 3)
     * >         )
     * >     );
     * > df.row("row2");
     * [bravo, 2] }</pre>
     *
     * @param row the row name
     * @return the list of values
     */
    public List<V> row(final String row) {
        return row(index.get(row));
    }

    /**
     * Return a data frame row as a list.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.<String>emptyList(),
     * >         Collections.<String>emptyList(),
     * >         Arrays.asList(
     * >             Arrays.<Object>asList("alpha", "bravo", "charlie"),
     * >             Arrays.<Object>asList(1, 2, 3)
     * >         )
     * >     );
     * > df.row(1);
     * [bravo, 2] }</pre>
     *
     * @param row the row index
     * @return the list of values
     */
    public List<V> row(final int row) {
        return new Views.SeriesListView<>(this, row, false);
    }

    /**
     * Select a subset of the data frame using a predicate function.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > for (int i = 0; i < 10; i++)
     * >     df.append(Arrays.<Object>asList("name" + i, i));
     * > df.select(new Predicate<Object>() {
     * >         @Override
     * >         public Boolean apply(List<Object> values) {
     * >             return Integer.class.cast(values.get(1)).intValue() % 2 == 0;
     * >         }
     * >     })
     * >   .col(1);
     * [0, 2, 4, 6, 8] } </pre>
     *
     * @param predicate a function returning true for rows to be included in the subset
     * @return a subset of the data frame
     */
    public DataFrame<V> select(final Predicate<V> predicate) {
        final BitSet selected = Selection.select(this, predicate);
        return new DataFrame<>(
                Selection.select(index, selected),
                columns,
                Selection.select(data, selected),
                new Grouping()
            );
    }

    /**
     * Return a data frame containing the first ten rows of this data frame.
     *
     * <pre> {@code
     * > DataFrame<Integer> df = new DataFrame<>("value");
     * > for (int i = 0; i < 20; i++)
     * >     df.append(Arrays.asList(i));
     * > df.head()
     * >   .col("value");
     * [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] }</pre>
     *
     * @return the new data frame
     */
    public DataFrame<V> head() {
        return head(10);
    }

    /**
     * Return a data frame containing the first {@code limit} rows of this data frame.
     *
     * <pre> {@code
     * > DataFrame<Integer> df = new DataFrame<>("value");
     * > for (int i = 0; i < 20; i++)
     * >     df.append(Arrays.asList(i));
     * > df.head(3)
     * >   .col("value");
     * [0, 1, 2] }</pre>
     *
     * @param limit the number of rows to include in the result
     * @return the new data frame
     */
    public DataFrame<V> head(final int limit) {
        final BitSet selected = new BitSet();
        selected.set(0, limit);
        return new DataFrame<>(
                Selection.select(index, selected),
                columns,
                Selection.select(data,  selected),
                new Grouping()
            );
    }

    /**
     * Return a data frame containing the last ten rows of this data frame.
     *
     * <pre> {@code
     * > DataFrame<Integer> df = new DataFrame<>("value");
     * > for (int i = 0; i < 20; i++)
     * >     df.append(Arrays.asList(i));
     * > df.tail()
     * >   .col("value");
     * [10, 11, 12, 13, 14, 15, 16, 17, 18, 19] }</pre>
     *
     * @return the new data frame
     */
    public DataFrame<V> tail() {
        return tail(10);
    }

    /**
     * Return a data frame containing the last {@code limit} rows of this data frame.
     *
     * <pre> {@code
     * > DataFrame<Integer> df = new DataFrame<>("value");
     * > for (int i = 0; i < 20; i++)
     * >     df.append(Arrays.asList(i));
     * > df.tail(3)
     * >   .col("value");
     * [17, 18, 19] }</pre>
     *
     * @param limit the number of rows to include in the result
     * @return the new data frame
     */
    public DataFrame<V> tail(final int limit) {
        final BitSet selected = new BitSet();
        final int len = length();
        selected.set(len - limit, len);
        return new DataFrame<>(
                Selection.select(index, selected),
                columns,
                Selection.select(data,  selected),
                new Grouping()
            );
    }

    /**
     * Return the values of the data frame as a flat list.
     *
     * <pre> {@code
     * > DataFrame<String> df = new DataFrame<>(
     * >         Arrays.asList(
     * >                 Arrays.asList("one", "two"),
     * >                 Arrays.asList("alpha", "bravo")
     * >             )
     * >     );
     * > df.flatten();
     * [one, two, alpha, bravo] }</pre>
     *
     * @return the list of values
     */
    public List<V> flatten() {
        return new Views.FlatView<>(this);
    }

    /**
     * Transpose the rows and columns of the data frame.
     *
     * <pre> {@code
     * > DataFrame<String> df = new DataFrame<>(
     * >         Arrays.asList(
     * >                 Arrays.asList("one", "two"),
     * >                 Arrays.asList("alpha", "bravo")
     * >             )
     * >     );
     * > df.transpose().flatten();
     * [one, alpha, two, bravo] }</pre>
     *
     * @return a new data frame with the rows and columns transposed
     */
    public DataFrame<V> transpose() {
        return new DataFrame<>(
                columns.names(),
                index.names(),
                new Views.ListView<>(this, true)
            );
    }

    /**
     * Apply a function to each value in the data frame.
     *
     * <pre> {@code
     * > DataFrame<Number> df = new DataFrame<>(
     * >         Arrays.<List<Number>>asList(
     * >                 Arrays.<Number>asList(1, 2),
     * >                 Arrays.<Number>asList(3, 4)
     * >             )
     * >     );
     * > df = df.transform(new Function<Number, Number>() {
     * >         public Number apply(Number value) {
     * >             return value.intValue() * value.intValue();
     * >         }
     * >     });
     * > df.flatten();
     * [1, 4, 9, 16] }</pre>
     *
     * @param transform the function to apply
     * @return a new data frame with the function results
     */
    public <U> DataFrame<U> transform(final Function<V, U> transform) {
        return new DataFrame<>(
                index.names(),
                columns.names(),
                new Views.TransformedView<V, U>(this, transform, false)
            );
    }

    public <U> DataFrame<U> transform(final RowFunction<V, U> transform) {
        final DataFrame<U> transformed = new DataFrame<>(columns.names());
        for (final List<V> row : this) {
            for (final List<U> trans : transform.apply(row)) {
                transformed.append(trans);
            }
        }
        return transformed;
    }

    /**
     * Attempt to infer better types for object columns.
     *
     * <p>The following conversions are performed where applicable:
     * <ul>
     *     <li>Floating point numbers are converted to {@code Double} values</li>
     *     <li>Whole numbers are converted to {@code Long} values</li>
     *     <li>True, false, yes, and no are converted to {@code Boolean} values</li>
     *     <li>Date strings in the following formats are converted to {@code Date} values:<br>
     *         {@literal 2000-01-01T00:00:00+1, 2000-01-01T00:00:00EST, 2000-01-01}</li>
     *     <li>Time strings in the following formats are converted to {@code Date} values:<br>
     *         {@literal 2000/01/01, 1/01/2000, 12:01:01 AM, 23:01:01, 12:01 AM, 23:01}</li>
     *     </li>
     *   </ul>
     * </p>
     *
     * <p>Note, the conversion process replaces existing values
     * with values of the converted type.</p>
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value", "date");
     * > df.append(Arrays.<Object>asList("one", "1", new Date()));
     * > df.convert();
     * > df.types();
     * [class java.lang.String, class java.lang.Long, class java.util.Date] }</pre>
     *
     * @return the data frame with the converted values
     */
    public DataFrame<V> convert() {
        Conversion.convert(this);
        return this;
    }

    /**
     * Convert columns based on the requested types.
     *
     * <p>Note, the conversion process replaces existing values
     * with values of the converted type.</p>
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("a", "b", "c");
     * > df.append(Arrays.<Object>asList("one", 1, 1.0));
     * > df.append(Arrays.<Object>asList("two", 2, 2.0));
     * > df.convert(
     * >     null,         // leave column "a" as is
     * >     Long.class,   // convert column "b" to Long
     * >     Number.class  // convert column "c" to Double
     * > );
     * > df.types();
     * [class java.lang.String, class java.lang.Long, class java.lang.Double] }</pre>
     *
     * @param columnTypes
     * @return the data frame with the converted values
     */
    @SafeVarargs
    public final DataFrame<V> convert(final Class<? extends V> ... columnTypes) {
        Conversion.convert(this, columnTypes);
        return this;
    }

    /**
     * Create a new data frame containing boolean values such that
     * {@code null} object references in the original data frame
     * yield {@code true} and valid references yield {@code false}.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<Object>(
     * >     Arrays.asList(
     * >         Arrays.<Object>asList("alpha", "bravo", null),
     * >         Arrays.<Object>asList(null, 2, 3)
     * >     )
     * > );
     * > df.isnull().row(0);
     * [false, true] }</pre>
     *
     * @return the new boolean data frame
     */
    public DataFrame<Boolean> isnull() {
        return Conversion.isnull(this);
    }

    /**
     * Create a new data frame containing boolean values such that
     * valid object references in the original data frame yield {@code true}
     * and {@code null} references yield {@code false}.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >     Arrays.asList(
     * >         Arrays.<Object>asList("alpha", "bravo", null),
     * >         Arrays.<Object>asList(null, 2, 3)
     * >     )
     * > );
     * > df.notnull().row(0);
     * [true, false] }</pre>
     *
     * @return the new boolean data frame
     */
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

    /**
     * Group the data frame rows by the specified column names.
     *
     * @param cols the column names
     * @return the grouped data frame
     */
    @Timed
    public DataFrame<V> groupBy(final String ... cols) {
        return groupBy(columns.indices(cols));
    }

    /**
     * Group the data frame rows by the specified columns.
     *
     * @param cols the column indices
     * @return the grouped data frame
     */
    @Timed
    public DataFrame<V> groupBy(final int ... cols) {
        return new DataFrame<>(
                index,
                columns,
                data,
                new Grouping(this, cols)
            );
    }

    /**
     * Group the data frame rows using the specified key function.
     *
     * @param function the function to reduce rows to grouping keys
     * @return the grouped data frame
     */
    @Timed
    public DataFrame<V> groupBy(final KeyFunction<V> function) {
        return new DataFrame<>(
                index,
                columns,
                data,
                new Grouping(this, function)
            );
    }

    public Grouping groups() {
        return groups;
    }

    /**
     * Return a map of group names to data frame for grouped
     * data frames.
     *
     * @return a map of group names to data frames
     */
    public Map<Object, DataFrame<V>> explode() {
        final Map<Object, DataFrame<V>> exploded = new LinkedHashMap<>();
        for (final Map.Entry<Object, BitSet> entry : groups) {
            final BitSet selected = entry.getValue();
            exploded.put(entry.getKey(), new DataFrame<V>(
                    Selection.select(index, selected),
                    columns,
                    Selection.select(data, selected),
                    new Grouping()
                ));
        }
        return exploded;
    }

    /**
     * Apply an aggregate function to each group or the entire
     * data frame if the data is not grouped.
     *
     * @param function the aggregate function
     * @return the new data frame
     */
    public <U> DataFrame<V> apply(final Aggregate<V, U> function) {
        return groups.apply(this, function);
    }

    @Timed
    public DataFrame<V> count() {
        return groups.apply(this, new Aggregation.Count<V>());
    }

    public DataFrame<V> collapse() {
        return groups.apply(this, new Aggregation.Collapse<V>());
    }

    public DataFrame<V> unique() {
        return groups.apply(this, new Aggregation.Unique<V>());
    }

    /**
     * Compute the sum of the numeric columns for each group
     * or the entire data frame if the data is not grouped.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.<String>emptyList(),
     * >         Arrays.asList("name", "value"),
     * >         Arrays.asList(
     * >                 Arrays.<Object>asList("alpha", "alpha", "alpha", "bravo", "bravo"),
     * >                 Arrays.<Object>asList(1, 2, 3, 4, 5)
     * >             )
     * >     );
     * > df.groupBy("name")
     * >   .sum()
     * >   .col("value");
     * [6.0, 9.0]} </pre>
     *
     * @return the new data frame
     */
    @Timed
    public DataFrame<V> sum() {
        return groups.apply(this, new Aggregation.Sum<V>());
    }

    /**
     * Compute the product of the numeric columns for each group
     * or the entire data frame if the data is not grouped.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.<String>emptyList(),
     * >         Arrays.asList("name", "value"),
     * >         Arrays.asList(
     * >                 Arrays.<Object>asList("alpha", "alpha", "alpha", "bravo", "bravo"),
     * >                 Arrays.<Object>asList(1, 2, 3, 4, 5)
     * >             )
     * >     );
     * > df.groupBy("name")
     * >   .prod()
     * >   .col("value");
     * [6.0, 20.0]} </pre>
     *
     * @return the new data frame
     */
    @Timed
    public DataFrame<V> prod() {
        return groups.apply(this, new Aggregation.Product<V>());
    }

    /**
     * Compute the mean of the numeric columns for each group
     * or the entire data frame if the data is not grouped.
     *
     * <pre> {@code
     * > DataFrame<Integer> df = new DataFrame<>("value");
     * > df.append("one", Arrays.asList(1));
     * > df.append("two", Arrays.asList(5));
     * > df.append("three", Arrays.asList(3));
     * > df.append("four",  Arrays.asList(7));
     * > df.mean().col(0);
     * [4.0] }</pre>
     *
     * @return the new data frame
     */
    @Timed
    public DataFrame<V> mean() {
        return groups.apply(this, new Aggregation.Mean<V>());
    }

    /**
     * Compute the standard deviation of the numeric columns for each group
     * or the entire data frame if the data is not grouped.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.<String>emptyList(),
     * >         Arrays.asList("name", "value"),
     * >         Arrays.asList(
     * >                 Arrays.<Object>asList("alpha", "alpha", "alpha", "bravo", "bravo", "bravo"),
     * >                 Arrays.<Object>asList(1, 2, 3, 4, 5, 6)
     * >             )
     * >     );
     * > df.groupBy("name")
     * >   .stddev()
     * >   .col("value");
     * [1.0, 1.0]} </pre>
     *
     * @return the new data frame
     */
    @Timed
    public DataFrame<V> stddev() {
        return groups.apply(this, new Aggregation.StdDev<V>());
    }

    @Timed
    public DataFrame<V> var() {
        return groups.apply(this, new Aggregation.Variance<V>());
    }

    @Timed
    public DataFrame<V> skew() {
        return groups.apply(this, new Aggregation.Skew<V>());
    }

    @Timed
    public DataFrame<V> kurt() {
        return groups.apply(this, new Aggregation.Kurtosis<V>());
    }

    @Timed
    public DataFrame<V> min() {
        return groups.apply(this, new Aggregation.Min<V>());
    }

    @Timed
    public DataFrame<V> max() {
        return groups.apply(this, new Aggregation.Max<V>());
    }

    @Timed
    public DataFrame<V> median() {
        return groups.apply(this, new Aggregation.Median<V>());
    }

    public DataFrame<V> pivot(final String row, final String col, final String ... values) {
        return pivot(Collections.singletonList(row), Collections.singletonList(col), Arrays.asList(values));
    }

    public DataFrame<V> pivot(final List<String> rows, final List<String> cols, final List<String> values) {
        return pivot(columns.indices(rows), columns.indices(cols), columns.indices(values));
    }

    public DataFrame<V> pivot(final int row, final int col, final int ... values) {
        return pivot(new int[] { row }, new int[] { col }, values);
    }

    @Timed
    public DataFrame<V> pivot(final int[] rows, final int[] cols, final int[] values) {
        return Pivoting.pivot(this, rows, cols, values);
    }

    @Timed
    public <U> DataFrame<U> pivot(final KeyFunction<V> rows, final KeyFunction<V> cols, final Map<Integer, Aggregate<V,U>> values) {
        return Pivoting.pivot(this, rows, cols, values);
    }

    public DataFrame<V> sortBy(final String ... cols) {
        final Map<Integer, SortDirection> sortCols = new LinkedHashMap<>();
        for (final String col : cols) {
            final SortDirection dir = col.startsWith("-") ?
                    SortDirection.DESCENDING : SortDirection.ASCENDING;
            final int c = columns.get(
                    col.startsWith("-") ? col.substring(1) : col);
            sortCols.put(c, dir);
        }
        return Sorting.sort(this, sortCols);
    }

    @Timed
    public DataFrame<V> sortBy(final int ... cols) {
        final Map<Integer, SortDirection> sortCols = new LinkedHashMap<>();
        for (final int c : cols) {
            final SortDirection dir = c < 0 ?
                    SortDirection.DESCENDING : SortDirection.ASCENDING;
            sortCols.put(Math.abs(c), dir);
        }
        return Sorting.sort(this, sortCols);
    }

    /**
     * Return the types for each of the data frame columns.
     *
     * @return the list of column types
     */
    public List<Class<?>> types() {
        return Inspection.types(this);
    }

    /**
     * Return a data frame containing only columns with numeric data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append(Arrays.<Object>asList("one", 1));
     * > df.append(Arrays.<Object>asList("two", 2));
     * > df.numeric().columns();
     * [value] }</pre>
     *
     * @return a data frame containing only the numeric columns
     */
    public DataFrame<Number> numeric() {
        final BitSet numeric = Inspection.numeric(this);
        final Set<String> keep = Selection.select(columns, numeric).names();
        return retain(keep.toArray(new String[keep.size()]))
                .cast(Number.class);
    }

    /**
     * Return a data frame containing only columns with non-numeric data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append(Arrays.<Object>asList("one", 1));
     * > df.append(Arrays.<Object>asList("two", 2));
     * > df.nonnumeric().columns();
     * [name] }</pre>
     *
     * @return a data frame containing only the non-numeric columns
     */
    public DataFrame<V> nonnumeric() {
        final BitSet nonnumeric = Inspection.nonnumeric(this);
        final Set<String> keep = Selection.select(columns, nonnumeric).names();
        return retain(keep.toArray(new String[keep.size()]));
    }

    /**
     * Return an iterator over the rows of the data frame.  Also used
     * implicitly with {@code foreach} loops.
     *
     * <pre> {@code
     * > DataFrame<Integer> df = new DataFrame<>(
     * >         Arrays.asList(
     * >             Arrays.asList(1, 2),
     * >             Arrays.asList(3, 4)
     * >         )
     * >     );
     * > List<Integer> results = new ArrayList<>();
     * > for (List<Integer> row : df)
     * >     results.add(row.get(0));
     * > results;
     * [1, 2] }</pre>
     *
     * @return an iterator over the rows of the data frame.
     */
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

    /**
     * Cast this data frame to the specified type.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append(Arrays.<Object>asList("one", "1"));
     * > DataFrame<String> dfs = df.cast(String.class);
     * > dfs.get(0, 0).getClass().getName();
     * java.lang.String }</pre>
     *
     * @param cls
     * @return the data frame cast to the specified type
     */
    @SuppressWarnings("unchecked")
    public <T> DataFrame<T> cast(final Class<T> cls) {
        return (DataFrame<T>)this;
    }

    /**
     * Return a map of index names to rows.
     *
     * <pre> {@code
     * > DataFrame<Integer> df = new DataFrame<>("value");
     * > df.append("alpha", Arrays.asList(1));
     * > df.append("bravo", Arrays.asList(2));
     * > df.map();
     * {alpha=[1], bravo=[2]}}</pre>
     *
     * @return a map of index names to rows.
     */
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

    /**
     * Display the numeric columns of this data frame
     * as a line chart in a new swing frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<Object>(
     * >     Collections.<String>emptyList(),
     * >     Arrays.asList("name", "value"),
     * >     Arrays.asList(
     * >         Arrays.<Object>asList("alpha", "bravo", "charlie"),
     * >         Arrays.<Object>asList(10, 20, 30)
     * >     )
     * > );
     * > df.plot();
     * } </pre>
     *
     */
    public final void plot() {
        Plotting.display(this);
    }

    public static final <V> DataFrame<String> compare(final DataFrame<V> df1, final DataFrame<V> df2) {
        return Comparing.compare(df1, df2);
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

    /**
     * A function that is applied to objects (rows or values)
     * in a {@linkplain DataFrame data frame}.
     *
     * <p>Implementors define {@link #apply(Object)} to perform
     * the desired calculation and return the result.</p>
     *
     * @param <I> the type of the input values
     * @param <O> the type of the output values
     * @see DataFrame#transform(Function)
     * @see DataFrame#apply(Aggregate)
     */
    public interface Function<I, O> {
        /**
         * Perform computation on the specified
         * input value and return the result.
         *
         * @param value the input value
         * @return the result
         */
        O apply(I value);
    }

    public interface RowFunction<I, O> {
        List<List<O>> apply(List<I> values);
    }

    /**
     * A function that converts {@linkplain DataFrame data frame}
     * rows to index or group keys.
     *
     * <p>Implementors define {@link #apply(Object)} to accept
     * a data frame row as input and return a key value, most
     * commonly used by {@link DataFrame#groupBy(KeyFunction)}.</p>
     *
     * @param <I> the type of the input values
     * @see DataFrame#groupBy(KeyFunction)
     */
    public interface KeyFunction<I>
    extends Function<List<I>, Object> { }

    /**
     * A function that converts lists of {@linkplain DataFrame data frame}
     * values to aggregate results.
     *
     * <p>Implementors define {@link #apply(Object)} to accept
     * a list of data frame values as input and return an aggregate
     * result.</p>
     *
     * @param <I> the type of the input values
     * @param <O> the type of the result
     * @see DataFrame#apply(Aggregate)
     */
    public interface Aggregate<I, O>
    extends Function<List<I>, O> { }

    /**
     * An interface used to filter a {@linkplain DataFrame data frame}.
     *
     * <p>Implementors define {@link #apply(Object)} to
     * return {@code true} for rows that should be included
     * in the filtered data frame.</p>
     *
     * @param <I> the type of the input values
     * @see DataFrame#select(Predicate)
     */
    public interface Predicate<I>
    extends Function<List<I>, Boolean> { }

    public enum SortDirection {
        ASCENDING,
        DESCENDING
    }

    public static final void main(final String[] args)
    throws IOException {
        final List<DataFrame<Object>> frames = new ArrayList<>(args.length - 1);
        for (int i = 1; i < args.length; i++) {
            frames.add(DataFrame.readCsv(args[i]));
        }

        if (args.length > 0 && "plot".equalsIgnoreCase(args[0])) {
            if (frames.size() == 1) {
                frames.get(0).plot();
                return;
            }
        }

        if (args.length > 0 && "compare".equalsIgnoreCase(args[0])) {
            if (frames.size() == 2) {
                System.out.println(DataFrame.compare(frames.get(0), frames.get(1)));
                return;
            }
        }

        System.err.printf(
                "usage: %s [plot|compare] [csv-file ...]\n",
                DataFrame.class.getCanonicalName()
            );
        System.exit(255);
    }
}
