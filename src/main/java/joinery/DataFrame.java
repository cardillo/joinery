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

import java.awt.Container;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.codahale.metrics.annotation.Timed;

import joinery.impl.Aggregation;
import joinery.impl.BlockManager;
import joinery.impl.Combining;
import joinery.impl.Comparison;
import joinery.impl.Conversion;
import joinery.impl.Display;
import joinery.impl.Grouping;
import joinery.impl.Index;
import joinery.impl.Inspection;
import joinery.impl.Pivoting;
import joinery.impl.Selection;
import joinery.impl.Serialization;
import joinery.impl.Shaping;
import joinery.impl.Shell;
import joinery.impl.Sorting;
import joinery.impl.SparseBitSet;
import joinery.impl.Timeseries;
import joinery.impl.Transforms;
import joinery.impl.Views;

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
 * > DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("gspc.csv"))
 * >     .retain("Date", "Close")
 * >     .groupBy(row -> Date.class.cast(row.get(0)).getMonth())
 * >     .mean()
 * >     .sortBy("Close")
 * >     .tail(3)
 * >     .apply(value -> Number.class.cast(value).intValue())
 * >     .col("Close");
 * [1370, 1378, 1403] }</pre>
 *
 * <p>Taking each step in turn:
 *   <ol>
 *     <li>{@link #readCsv(String)} reads csv data from files and urls</li>
 *     <li>{@link #retain(Object...)} is used to
 *         eliminate columns that are not needed</li>
 *     <li>{@link #groupBy(KeyFunction)} with a key function
 *         is used to group the rows by month</li>
 *     <li>{@link #mean()} calculates the average close for each month</li>
 *     <li>{@link #sortBy(Object...)} orders the rows according
 *         to average closing price</li>
 *     <li>{@link #tail(int)} returns the last three rows
 *         (alternatively, sort in descending order and use head)</li>
 *     <li>{@link #apply(Function)} is used to convert the
 *         closing prices to integers (this is purely to ease
 *         comparisons for verifying the results</li>
 *     <li>finally, {@link #col(Object)} is used to
 *         extract the values as a list</li>
 *   </ol>
 * </p>
 *
 * <p>Find more details on the
 * <a href="http://github.com/cardillo/joinery">github</a>
 * project page.</p>
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
        this(Collections.<List<V>>emptyList());
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
        this(Arrays.asList((Object[])columns));
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
    public DataFrame(final Collection<?> columns) {
        this(Collections.emptyList(), columns, Collections.<List<V>>emptyList());
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
    public DataFrame(final Collection<?> index, final Collection<?> columns) {
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
    public DataFrame(final List<? extends List<? extends V>> data) {
        this(Collections.emptyList(), Collections.emptyList(), data);
    }

    /**
     * Construct a new data frame using the specified data and indices.
     *
     * @param index the row names
     * @param columns the column names
     * @param data the data
     */
    public DataFrame(final Collection<?> index, final Collection<?> columns,
            final List<? extends List<? extends V>> data) {
        final BlockManager<V> mgr = new BlockManager<V>(data);
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
    public DataFrame<V> add(final Object ... columns) {
        for (final Object column : columns) {
            final List<V> values = new ArrayList<V>(length());
            for (int r = 0; r < values.size(); r++) {
                values.add(null);
            }
            add(column, values);
        }
        return this;
    }

    /**
     * Add the list of values as a new column.
     *
     * @param values the new column values
     * @return the data frame with the column added
     */
    public DataFrame<V> add(final List<V> values) {
        return add(length(), values);
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
    public DataFrame<V> add(final Object column, final List<V> values) {
        columns.add(column, data.size());
        index.extend(values.size());
        data.add(values);
        return this;
    }

    /**
     * Add the results of applying a row-wise
     * function to the data frame as a new column.
     *
     * @param column the new column name
     * @param function the function to compute the new column values
     * @return the data frame with the column added
     */
    public DataFrame<V> add(final Object column, final Function<List<V>, V> function) {
        final List<V> values = new ArrayList<>();
        for (final List<V> row : this) {
            values.add(function.apply(row));
        }
        return add(column, values);
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
    public DataFrame<V> drop(final Object ... cols) {
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
    public DataFrame<V> drop(final Integer ... cols) {
        final List<Object> colnames = new ArrayList<>(columns.names());
        final List<Object> todrop = new ArrayList<>(cols.length);
        for (final int col : cols) {
            todrop.add(colnames.get(col));
        }
        colnames.removeAll(todrop);

        final List<List<V>> keep = new ArrayList<>(colnames.size());
        for (final Object col : colnames) {
            keep.add(col(col));
        }

        return new DataFrame<>(
                index.names(),
                colnames,
                keep
            );
    }

    public DataFrame<V> dropna() {
        return dropna(Axis.ROWS);
    }

    public DataFrame<V> dropna(final Axis direction) {
        switch (direction) {
            case ROWS:
                return select(new Selection.DropNaPredicate<V>());
            default:
                return transpose()
                       .select(new Selection.DropNaPredicate<V>())
                       .transpose();
        }
    }

    /**
     * Returns a view of the of data frame with NA's replaced with {@code fill}.
     *
     * @param fill the value used to replace missing values
     * @return the new data frame
     */
    public DataFrame<V> fillna(final V fill) {
        return apply(new Views.FillNaFunction<V>(fill));
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
    public DataFrame<V> retain(final Object ... cols) {
        return retain(columns.indices(cols));
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
    public DataFrame<V> retain(final Integer ... cols) {
        final Set<Integer> keep = new HashSet<Integer>(Arrays.asList(cols));
        final Integer[] todrop = new Integer[size() - keep.size()];
        for (int i = 0, c = 0; c < size(); c++) {
            if (!keep.contains(c)) {
                todrop[i++] = c;
            }
        }
        return drop(todrop);
    }

    /**
     * Re-index the rows of the data frame using the specified column index,
     * optionally dropping the column from the data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("one", "two");
     * > df.append("a", Arrays.asList("alpha", 1));
     * > df.append("b", Arrays.asList("bravo", 2));
     * > df.reindex(0, true)
     * >   .index();
     * [alpha, bravo] }</pre>
     *
     * @param col the column to use as the new index
     * @param drop true to remove the index column from the data, false otherwise
     * @return a new data frame with index specified
     */
    public DataFrame<V> reindex(final Integer col, final boolean drop) {
        final DataFrame<V> df = Index.reindex(this, col);
        return drop ? df.drop(col) : df;
    }

    /**
     * Re-index the rows of the data frame using the specified column indices,
     * optionally dropping the columns from the data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("one", "two", "three");
     * > df.append("a", Arrays.asList("alpha", 1, 10));
     * > df.append("b", Arrays.asList("bravo", 2, 20));
     * > df.reindex(new Integer[] { 0, 1 }, true)
     * >   .index();
     * [[alpha, 1], [bravo, 2]] }</pre>
     *
     * @param cols the column to use as the new index
     * @param drop true to remove the index column from the data, false otherwise
     * @return a new data frame with index specified
     */
    public DataFrame<V> reindex(final Integer[] cols, final boolean drop) {
        final DataFrame<V> df = Index.reindex(this, cols);
        return drop ? df.drop(cols) : df;
    }

    /**
     * Re-index the rows of the data frame using the specified column indices
     * and dropping the columns from the data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("one", "two");
     * > df.append("a", Arrays.asList("alpha", 1));
     * > df.append("b", Arrays.asList("bravo", 2));
     * > df.reindex(0)
     * >   .index();
     * [alpha, bravo] }</pre>
     *
     * @param cols the column to use as the new index
     * @return a new data frame with index specified
     */
    public DataFrame<V> reindex(final Integer ... cols) {
        return reindex(cols, true);
    }

    /**
     * Re-index the rows of the data frame using the specified column name,
     * optionally dropping the row from the data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("one", "two");
     * > df.append("a", Arrays.asList("alpha", 1));
     * > df.append("b", Arrays.asList("bravo", 2));
     * > df.reindex("one", true)
     * >   .index();
     * [alpha, bravo] }</pre>
     *
     * @param col the column to use as the new index
     * @param drop true to remove the index column from the data, false otherwise
     * @return a new data frame with index specified
     */
    public DataFrame<V> reindex(final Object col, final boolean drop) {
        return reindex(columns.get(col), drop);
    }

    /**
     * Re-index the rows of the data frame using the specified column names,
     * optionally dropping the columns from the data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("one", "two", "three");
     * > df.append("a", Arrays.asList("alpha", 1, 10));
     * > df.append("b", Arrays.asList("bravo", 2, 20));
     * > df.reindex(new String[] { "one", "two" }, true)
     * >   .index();
     * [[alpha, 1], [bravo, 2]] }</pre>
     *
     * @param cols the column to use as the new index
     * @param drop true to remove the index column from the data, false otherwise
     * @return a new data frame with index specified
     */
    public DataFrame<V> reindex(final Object[] cols, final boolean drop) {
        return reindex(columns.indices(cols), drop);
    }

    /**
     * Re-index the rows of the data frame using the specified column names
     * and removing the columns from the data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("one", "two");
     * > df.append("a", Arrays.asList("alpha", 1));
     * > df.append("b", Arrays.asList("bravo", 2));
     * > df.reindex("one", true)
     * >   .index();
     * [alpha, bravo] }</pre>
     *
     * @param cols the column to use as the new index
     * @return a new data frame with index specified
     */
    public DataFrame<V> reindex(final Object ... cols) {
        return reindex(columns.indices(cols), true);
    }

    /**
     * Return a new data frame with the default index, rows names will
     * be reset to the string value of their integer index.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("one", "two");
     * > df.append("a", Arrays.asList("alpha", 1));
     * > df.append("b", Arrays.asList("bravo", 2));
     * > df.resetIndex()
     * >   .index();
     * [0, 1] }</pre>
     *
     * @return a new data frame with the default index.
     */
    public DataFrame<V> resetIndex() {
        return Index.reset(this);
    }

    public DataFrame<V> rename(final Object old, final Object name) {
        return rename(Collections.singletonMap(old, name));
    }

    public DataFrame<V> rename(final Map<Object, Object> names) {
        columns.rename(names);
        return this;
    }

    public DataFrame<V> append(final Object name, final V[] row) {
        return append(name, Arrays.asList(row));
    }

    /**
     * Append rows to the data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append(Arrays.asList("alpha", 1));
     * > df.append(Arrays.asList("bravo", 2));
     * > df.length();
     * 2 }</pre>
     *
     * @param row the row to append
     * @return the data frame with the new data appended
     */
    public DataFrame<V> append(final List<? extends V> row) {
        return append(length(), row);
    }

    /**
     * Append rows indexed by the the specified name to the data frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append("row1", Arrays.asList("alpha", 1));
     * > df.append("row2", Arrays.asList("bravo", 2));
     * > df.index();
     * [row1, row2] }</pre>
     *
     * @param name the row name to add to the index
     * @param row the row to append
     * @return the data frame with the new data appended
     */
    @Timed
    public DataFrame<V> append(final Object name, final List<? extends V> row) {
        final int len = length();
        index.add(name, len);
        columns.extend(row.size());
        data.reshape(columns.names().size(), len + 1);
        for (int c = 0; c < data.size(); c++) {
            data.set(c < row.size() ? row.get(c) : null, c, len);
        }
        return this;
    }

    /**
     * Concatenate two dataframes, either vertically or horizontally (Developed by Dongming Xia)
     *
     * @param df2 - The dataframe to be concatenated after the main dataframe
     * @param axis - The axis to concatenate along, default is 0
     */

    // more potential functionalities to add:
    public final DataFrame<V> concatenate(final DataFrame<V> df2, final int axis){
        // check if df2 has the same number of columns as the main df
        if (axis == 0){
            return this.verticalConcat(df2);
        }
        else if (axis == 1){
            return this.horizontalConcat(df2);
        }
        else{
            System.out.println("Please put 0 or 1 for the value of axis");
            return this;
        }
    }

    public final DataFrame<V> concatenate(final DataFrame<V> df2){
        return this.concatenate(df2, 0);
    }

    private DataFrame<V> verticalConcat(final DataFrame<V> df2) {
        // check if df2 has the same number of column as the main df, if not, return the main dataframe
        if (this.size() != df2.size()){
            System.out.println("The numbers of columns between two dataframes does not match");
            return this;
        }
        // check if df2 has the same column types as the main df, if not, return the main dataframe
        else if (!Arrays.equals(this.types().toArray(), df2.types().toArray())){
            System.out.println("The column types between two dataframes does not match");
            return this;
        }

        // the main body of the function, Time Complexity is O(n) where n is the number of rows in df2
        for(List<V> row: df2){
            this.append(row);
        }

        return this;
    }

    private DataFrame<V> horizontalConcat(final DataFrame<V> df2) {
        // check if df2 has the same number of rows as the main df, if not, return the main dataframe
        if (this.length() != df2.length()) {
            return this;
        }

        DataFrame<V> temp_df1 = new DataFrame<>();
        DataFrame<V> temp_df2 = new DataFrame<>();

        temp_df1 = this.resetIndex();
        temp_df2 = df2.resetIndex();

        return temp_df1.join(temp_df2);
    }

    /**
     * Reshape a data frame to the specified dimensions.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("0", "1", "2");
     * > df.append("0", Arrays.asList(10, 20, 30));
     * > df.append("1", Arrays.asList(40, 50, 60));
     * > df.reshape(3, 2)
     * >   .length();
     * 3 }</pre>
     *
     * @param rows the number of rows the new data frame will contain
     * @param cols the number of columns the new data frame will contain
     * @return a new data frame with the specified dimensions
     */
    public DataFrame<V> reshape(final Integer rows, final Integer cols) {
        return Shaping.reshape(this, rows, cols);
    }

    /**
     * Reshape a data frame to the specified indices.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("0", "1", "2");
     * > df.append("0", Arrays.asList(10, 20, 30));
     * > df.append("1", Arrays.asList(40, 50, 60));
     * > df.reshape(Arrays.asList("0", "1", "2"), Arrays.asList("0", "1"))
     * >   .length();
     * 3 }</pre>
     *
     * @param rows the names of rows the new data frame will contain
     * @param cols the names of columns the new data frame will contain
     * @return a new data frame with the specified indices
     */
    public DataFrame<V> reshape(final Collection<?> rows, final Collection<?> cols) {
        return Shaping.reshape(this, rows, cols);
    }

    /**
     * Return a new data frame created by performing a left outer join
     * of this data frame with the argument and using the row indices
     * as the join key.
     *
     * <pre> {@code
     * > DataFrame<Object> left = new DataFrame<>("a", "b");
     * > left.append("one", Arrays.asList(1, 2));
     * > left.append("two", Arrays.asList(3, 4));
     * > left.append("three", Arrays.asList(5, 6));
     * > DataFrame<Object> right = new DataFrame<>("c", "d");
     * > right.append("one", Arrays.asList(10, 20));
     * > right.append("two", Arrays.asList(30, 40));
     * > right.append("four", Arrays.asList(50, 60));
     * > left.join(right)
     * >     .index();
     * [one, two, three] }</pre>
     *
     * @param other the other data frame
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> join(final DataFrame<V> other) {
        return join(other, JoinType.LEFT, null);
    }

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * using the row indices as the join key.
     *
     * @param other the other data frame
     * @param join the join type
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> join(final DataFrame<V> other, final JoinType join) {
        return join(other, join, null);
    }

    /**
     * Return a new data frame created by performing a left outer join of this
     * data frame with the argument using the specified key function.
     *
     * @param other the other data frame
     * @param on the function to generate the join keys
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> join(final DataFrame<V> other, final KeyFunction<V> on) {
        return join(other, JoinType.LEFT, on);
    }

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * the specified key function.
     *
     * @param other the other data frame
     * @param join the join type
     * @param on the function to generate the join keys
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> join(final DataFrame<V> other, final JoinType join, final KeyFunction<V> on) {
        return Combining.join(this, other, join, on);
    }

    /**
     * Return a new data frame created by performing a left outer join of
     * this data frame with the argument using the column values as the join key.
     *
     * @param other the other data frame
     * @param cols the indices of the columns to use as the join key
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> joinOn(final DataFrame<V> other, final Integer ... cols) {
        return joinOn(other, JoinType.LEFT, cols);
    }

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * the column values as the join key.
     *
     * @param other the other data frame
     * @param join the join type
     * @param cols the indices of the columns to use as the join key
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> joinOn(final DataFrame<V> other, final JoinType join, final Integer ... cols) {
        return Combining.joinOn(this, other, join, cols);
    }

    /**
     * Return a new data frame created by performing a left outer join of
     * this data frame with the argument using the column values as the join key.
     *
     * @param other the other data frame
     * @param cols the names of the columns to use as the join key
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> joinOn(final DataFrame<V> other, final Object ... cols) {
        return joinOn(other, JoinType.LEFT, cols);
    }

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * the column values as the join key.
     *
     * @param other the other data frame
     * @param join the join type
     * @param cols the names of the columns to use as the join key
     * @return the result of the join operation as a new data frame
     */
    public final DataFrame<V> joinOn(final DataFrame<V> other, final JoinType join, final Object ... cols) {
        return joinOn(other, join, columns.indices(cols));
    }

    /**
     * Return a new data frame created by performing a left outer join of this
     * data frame with the argument using the common, non-numeric columns
     * from each data frame as the join key.
     *
     * @param other the other data frame
     * @return the result of the merge operation as a new data frame
     */
    public final DataFrame<V> merge(final DataFrame<V> other) {
        return merge(other, JoinType.LEFT);
    }

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * the common, non-numeric columns from each data frame as the join key.
     *
     * @param other the other data frame
     * @return the result of the merge operation as a new data frame
     */
    public final DataFrame<V> merge(final DataFrame<V> other, final JoinType join) {
        return Combining.merge(this, other, join);
    }

    /**
     * Update the data frame in place by overwriting the any values
     * with the non-null values provided by the data frame arguments.
     *
     * @param others the other data frames
     * @return this data frame with the overwritten values
     */
    @SafeVarargs
    public final DataFrame<V> update(final DataFrame<? extends V> ... others) {
        Combining.update(this, true, others);
        return this;
    }

    /**
     * Concatenate the specified data frames with this data frame
     * and return the result.
     *
     * <pre> {@code
     * > DataFrame<Object> left = new DataFrame<>("a", "b", "c");
     * > left.append("one", Arrays.asList(1, 2, 3));
     * > left.append("two", Arrays.asList(4, 5, 6));
     * > left.append("three", Arrays.asList(7, 8, 9));
     * > DataFrame<Object> right = new DataFrame<>("a", "b", "d");
     * > right.append("one", Arrays.asList(10, 20, 30));
     * > right.append("two", Arrays.asList(40, 50, 60));
     * > right.append("four", Arrays.asList(70, 80, 90));
     * > left.concat(right).length();
     * 6 }</pre>
     *
     * @param others the other data frames
     * @return the data frame containing all the values
     */
    @SafeVarargs
    public final DataFrame<V> concat(final DataFrame<? extends V> ... others) {
        return Combining.concat(this, others);
    }

    /**
     * Update the data frame in place by overwriting any null values with
     * any non-null values provided by the data frame arguments.
     *
     * @param others the other data frames
     * @return this data frame with the overwritten values
     */
    @SafeVarargs
    public final DataFrame<V> coalesce(final DataFrame<? extends V> ... others) {
        Combining.update(this, false, others);
        return this;
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
     * > df.append(Arrays.asList("alpha", 1));
     * > df.append(Arrays.asList("bravo", 2));
     * > df.append(Arrays.asList("charlie", 3));
     * > df.length();
     * 3 }</pre>
     *
     * @return the number of rows
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
     * > df.append("row1", Arrays.asList("one", 1));
     * > df.index();
     * [row1] }</pre>
     *
     * @return the index names
     */
    public Set<Object> index() {
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
    public Set<Object> columns() {
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
     * >         Arrays.asList("alpha", "bravo", "charlie"),
     * >         Arrays.asList(10, 20, 30)
     * >     )
     * > );
     * > df.get("row2", "name");
     * bravo }</pre>
     *
     * @param row the row name
     * @param col the column name
     * @return the value
     */
    public V get(final Object row, final Object col) {
        return get(index.get(row), columns.get(col));
    }

    /**
     * Return the value located by the (row, column) coordinates.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<Object>(
     * >     Collections.emptyList(),
     * >     Arrays.asList("name", "value"),
     * >     Arrays.asList(
     * >         Arrays.asList("alpha", "bravo", "charlie"),
     * >         Arrays.asList(10, 20, 30)
     * >     )
     * > );
     * > df.get(1, 0);
     * bravo }</pre>
     *
     * @param row the row index
     * @param col the column index
     * @return the value
     */
    public V get(final Integer row, final Integer col) {
        return data.get(col, row);
    }

    public DataFrame<V> slice(final Object rowStart, final Object rowEnd) {
        return slice(index.get(rowStart), index.get(rowEnd), 0, size());
    }

    public DataFrame<V> slice(final Object rowStart, final Object rowEnd, final Object colStart, final Object colEnd) {
        return slice(index.get(rowStart), index.get(rowEnd), columns.get(colStart), columns.get(colEnd));
    }

    public DataFrame<V> slice(final Integer rowStart, final Integer rowEnd) {
        return slice(rowStart, rowEnd, 0, size());
    }

    public DataFrame<V> slice(final Integer rowStart, final Integer rowEnd, final Integer colStart, final Integer colEnd) {
        final SparseBitSet[] slice = Selection.slice(this, rowStart, rowEnd, colStart, colEnd);
        return new DataFrame<>(
                Selection.select(index, slice[0]),
                Selection.select(columns, slice[1]),
                Selection.select(data, slice[0], slice[1]),
                new Grouping()
            );
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
    public void set(final Object row, final Object col, final V value) {
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
    public void set(final Integer row, final Integer col, final V value) {
        data.set(value, col, row);
    }

    /**
     * Return a data frame column as a list.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.emptyList(),
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
    public List<V> col(final Object column) {
        return col(columns.get(column));
    }

    /**
     * Return a data frame column as a list.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.emptyList(),
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
    public List<V> col(final Integer column) {
        return new Views.SeriesListView<>(this, column, true);
    }

    /**
     * Return a data frame row as a list.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Arrays.asList("row1", "row2", "row3"),
     * >         Collections.emptyList(),
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
    public List<V> row(final Object row) {
        return row(index.get(row));
    }

    /**
     * Return a data frame row as a list.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.emptyList(),
     * >         Collections.emptyList(),
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
    public List<V> row(final Integer row) {
        return new Views.SeriesListView<>(this, row, false);
    }

    /**
     * Select a subset of the data frame using a predicate function.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > for (int i = 0; i < 10; i++)
     * >     df.append(Arrays.asList("name" + i, i));
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
        final SparseBitSet selected = Selection.select(this, predicate);
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
        final SparseBitSet selected = new SparseBitSet();
        selected.set(0, Math.min(limit, length()));
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
        final SparseBitSet selected = new SparseBitSet();
        final int len = length();
        selected.set(Math.max(len - limit, 0), len);
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
     * > df = df.apply(new Function<Number, Number>() {
     * >         public Number apply(Number value) {
     * >             return value.intValue() * value.intValue();
     * >         }
     * >     });
     * > df.flatten();
     * [1, 4, 9, 16] }</pre>
     *
     * @param function the function to apply
     * @return a new data frame with the function results
     */
    public <U> DataFrame<U> apply(final Function<V, U> function) {
        return new DataFrame<>(
                index.names(),
                columns.names(),
                new Views.TransformedView<V, U>(this, function, false)
            );
    }

    public <U> DataFrame<U> transform(final RowFunction<V, U> transform) {
        final DataFrame<U> transformed = new DataFrame<>(columns.names());
        final Iterator<Object> it = index().iterator();
        for (final List<V> row : this) {
            for (final List<U> trans : transform.apply(row)) {
                transformed.append(it.hasNext() ? it.next() : transformed.length(), trans);
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
     * > df.append(Arrays.asList("one", "1", new Date()));
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

    public DataFrame<V> convert(final NumberDefault numDefault, final String naString) {
        Conversion.convert(this,numDefault,naString);
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
     * > df.append(Arrays.asList("one", 1, 1.0));
     * > df.append(Arrays.asList("two", 2, 2.0));
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
     * >         Arrays.asList("alpha", "bravo", null),
     * >         Arrays.asList(null, 2, 3)
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

    /**
     * Copy the values of contained in the data frame into a
     * flat array of length {@code #size()} * {@code #length()}.
     *
     * @return the array
     */
    public Object[] toArray() {
        return toArray(new Object[size() * length()]);
    }

    /**
     * Copy the values of contained in the data frame into the
     * specified array. If the length of the provided array is
     * less than length {@code #size()} * {@code #length()} a
     * new array will be created.
     *
     * @return the array
     */
    public <U> U[] toArray(final U[] array) {
        return new Views.FlatView<>(this).toArray(array);
    }

    @SuppressWarnings("unchecked")
    public <U> U[][] toArray(final U[][] array) {
        if (array.length >= size() && array.length > 0 && array[0].length >= length()) {
            for (int c = 0; c < size(); c++) {
                for (int r = 0; r < length(); r++) {
                    array[r][c] = (U)get(r, c);
                }
            }
        }
        return (U[][])toArray(array.getClass());
    }

    /**
     * Copy the values of contained in the data frame into a
     * array of the specified type.  If the type specified is
     * a two dimensional array, for example {@code double[][].class},
     * a row-wise copy will be made.
     *
     * @throws IllegalArgumentException if the values are not assignable to the specified component type
     * @return the array
     */
    public <U> U toArray(final Class<U> cls) {
        int dim = 0;
        Class<?> type = cls;
        while (type.getComponentType() != null) {
            type = type.getComponentType();
            dim++;
        }

        final int size = size();
        final int len = length();
        if (dim == 1) {
            @SuppressWarnings("unchecked")
            final U array = (U)Array.newInstance(type, size * len);
            for (int c = 0; c < size; c++) {
                for (int r = 0; r < len; r++) {
                    Array.set(array, c * len + r, data.get(c, r));
                }
            }
            return array;
        } else if (dim == 2) {
            @SuppressWarnings("unchecked")
            final U array = (U)Array.newInstance(type, new int[] { len, size });
            for (int r = 0; r < len; r++) {
                final Object aa = Array.get(array, r);
                for (int c = 0; c < size; c++) {
                    Array.set(aa, c, get(r, c));
                }
                Array.set(array, r, aa);
            }
            return array;
        }

        throw new IllegalArgumentException("class must be an array class");
    }

    /**
     *  Encodes the DataFrame as a model matrix, converting nominal values
     *  to dummy variables but does not add an intercept column.
     *
     *   More methods with additional parameters to control the conversion to
     *   the model matrix are available in the <code>Conversion</code> class.
     *
     * @param fillValue value to replace NA's with
     * @return a model matrix
     */
    public double[][] toModelMatrix(final double fillValue) {
        return Conversion.toModelMatrix(this, fillValue);
    }

    /**
     *  Encodes the DataFrame as a model matrix, converting nominal values
     *  to dummy variables but does not add an intercept column.
     *
     *   More methods with additional parameters to control the conversion to
     *   the model matrix are available in the <code>Conversion</code> class.
     *
     * @return a model matrix
     */
    public DataFrame<Number> toModelMatrixDataFrame() {
        return Conversion.toModelMatrixDataFrame(this);
    }

    /**
     * Group the data frame rows by the specified column names.
     *
     * @param cols the column names
     * @return the grouped data frame
     */
    @Timed
    public DataFrame<V> groupBy(final Object ... cols) {
        return groupBy(columns.indices(cols));
    }

    /**
     * Group the data frame rows by the specified columns.
     *
     * @param cols the column indices
     * @return the grouped data frame
     */
    @Timed
    public DataFrame<V> groupBy(final Integer ... cols) {
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
     * data frames. Observe that for this method to have any
     * effect a {@code groupBy} call must have been done before.
     *
     * @return a map of group names to data frames
     */
    public Map<Object, DataFrame<V>> explode() {
        final Map<Object, DataFrame<V>> exploded = new LinkedHashMap<>();
        for (final Map.Entry<Object, SparseBitSet> entry : groups) {
            final SparseBitSet selected = entry.getValue();
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
    public <U> DataFrame<V> aggregate(final Aggregate<V, U> function) {
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
     * >         Collections.emptyList(),
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
     * >         Collections.emptyList(),
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
     * Compute the percentile of the numeric columns for each group
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
    public DataFrame<V> percentile(final double quantile) {
        return groups.apply(this, new Aggregation.Percentile<V>(quantile));
    }

    /**
     * Compute the standard deviation of the numeric columns for each group
     * or the entire data frame if the data is not grouped.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>(
     * >         Collections.emptyList(),
     * >         Arrays.asList("name", "value"),
     * >         Arrays.asList(
     * >                 Arrays.<Object>asList("alpha", "alpha", "alpha", "bravo", "bravo", "bravo"),
     * >                 Arrays.<Object>asList(1, 2, 3, 4, 6, 8)
     * >             )
     * >     );
     * > df.groupBy("name")
     * >   .stddev()
     * >   .col("value");
     * [1.0, 2.0]} </pre>
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

    @Timed
    public DataFrame<Number> cov() {
        return Aggregation.cov(this);
    }

    @Timed
    public DataFrame<V> cumsum() {
        return groups.apply(this, new Transforms.CumulativeSum<V>());
    }

    @Timed
    public DataFrame<V> cumprod() {
        return groups.apply(this, new Transforms.CumulativeProduct<V>());
    }

    @Timed
    public DataFrame<V> cummin() {
        return groups.apply(this, new Transforms.CumulativeMin<V>());
    }

    @Timed
    public DataFrame<V> cummax() {
        return groups.apply(this, new Transforms.CumulativeMax<V>());
    }

    @Timed
    public DataFrame<V> describe() {
        return Aggregation.describe(
            groups.apply(this, new Aggregation.Describe<V>()));
    }

    public DataFrame<V> pivot(final Object row, final Object col, final Object ... values) {
        return pivot(Collections.singletonList(row), Collections.singletonList(col), Arrays.asList(values));
    }

    public DataFrame<V> pivot(final List<Object> rows, final List<Object> cols, final List<Object> values) {
        return pivot(columns.indices(rows), columns.indices(cols), columns.indices(values));
    }

    public DataFrame<V> pivot(final Integer row, final Integer col, final Integer ... values) {
        return pivot(new Integer[] { row }, new Integer[] { col }, values);
    }

    @Timed
    public DataFrame<V> pivot(final Integer[] rows, final Integer[] cols, final Integer[] values) {
        return Pivoting.pivot(this, rows, cols, values);
    }

    @Timed
    public <U> DataFrame<U> pivot(final KeyFunction<V> rows, final KeyFunction<V> cols, final Map<Integer, Aggregate<V,U>> values) {
        return Pivoting.pivot(this, rows, cols, values);
    }

    public DataFrame<V> sortBy(final Object ... cols) {
        final Map<Integer, SortDirection> sortCols = new LinkedHashMap<>();
        for (final Object col : cols) {
            final String str = col instanceof String ? String.class.cast(col) : "";
            final SortDirection dir = str.startsWith("-") ?
                    SortDirection.DESCENDING : SortDirection.ASCENDING;
            final int c = columns.get(str.startsWith("-") ? str.substring(1) : col);
            sortCols.put(c, dir);
        }
        return Sorting.sort(this, sortCols);
    }

    @Timed
    public DataFrame<V> sortBy(final Integer ... cols) {
        final Map<Integer, SortDirection> sortCols = new LinkedHashMap<>();
        for (final int c : cols) {
            final SortDirection dir = c < 0 ?
                    SortDirection.DESCENDING : SortDirection.ASCENDING;
            sortCols.put(Math.abs(c), dir);
        }
        return Sorting.sort(this, sortCols);
    }

    public DataFrame<V> sortBy(final Comparator<List<V>> comparator) {
        return Sorting.sort(this, comparator);
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
     * > df.append(Arrays.asList("one", 1));
     * > df.append(Arrays.asList("two", 2));
     * > df.numeric().columns();
     * [value] }</pre>
     *
     * @return a data frame containing only the numeric columns
     */
    public DataFrame<Number> numeric() {
        final SparseBitSet numeric = Inspection.numeric(this);
        final Set<Object> keep = Selection.select(columns, numeric).names();
        return retain(keep.toArray(new Object[keep.size()]))
                .cast(Number.class);
    }

    /**
     * Return a data frame containing only columns with non-numeric data.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<>("name", "value");
     * > df.append(Arrays.asList("one", 1));
     * > df.append(Arrays.asList("two", 2));
     * > df.nonnumeric().columns();
     * [name] }</pre>
     *
     * @return a data frame containing only the non-numeric columns
     */
    public DataFrame<V> nonnumeric() {
        final SparseBitSet nonnumeric = Inspection.nonnumeric(this);
        final Set<Object> keep = Selection.select(columns, nonnumeric).names();
        return retain(keep.toArray(new Object[keep.size()]));
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

    public ListIterator<Map<Object, V>> itermap() {
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
     * > df.append(Arrays.asList("one", "1"));
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
    public Map<Object, List<V>> map() {
        final Map<Object, List<V>> m = new LinkedHashMap<Object, List<V>>();

        final int len = length();
        final Iterator<Object> names = index.names().iterator();
        for (int r = 0; r < len; r++) {
            final Object name = names.hasNext() ? names.next() : r;
            m.put(name, row(r));
        }

        return m;
    }

    public Map<V, List<V>> map(final Object key, final Object value) {
        return map(columns.get(key), columns.get(value));
    }

    public Map<V, List<V>> map(final Integer key, final Integer value) {
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

    public DataFrame<V> unique(final Object ... cols) {
        return unique(columns.indices(cols));
    }

    public DataFrame<V> unique(final Integer ... cols) {
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

    public DataFrame<V> diff() {
        return diff(1);
    }

    public DataFrame<V> diff(final int period) {
        return Timeseries.diff(this, period);
    }

    public DataFrame<V> percentChange() {
        return percentChange(1);
    }

    public DataFrame<V> percentChange(final int period) {
        return Timeseries.percentChange(this, period);
    }

    public DataFrame<V> rollapply(final Function<List<V>, V> function) {
        return rollapply(function, 1);
    }

    public DataFrame<V> rollapply(final Function<List<V>, V> function, final int period) {
        return Timeseries.rollapply(this, function, period);
    }

    /**
     * Display the numeric columns of this data frame
     * as a line chart in a new swing frame.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<Object>(
     * >     Collections.emptyList(),
     * >     Arrays.asList("name", "value"),
     * >     Arrays.asList(
     * >         Arrays.asList("alpha", "bravo", "charlie"),
     * >         Arrays.asList(10, 20, 30)
     * >     )
     * > );
     * > df.plot();
     * } </pre>
     *
     */
    public final void plot() {
        plot(PlotType.LINE);
    }

    /**
     * Display the numeric columns of this data frame
     * as a chart in a new swing frame using the specified type.
     *
     * <pre> {@code
     * > DataFrame<Object> df = new DataFrame<Object>(
     * >     Collections.emptyList(),
     * >     Arrays.asList("name", "value"),
     * >     Arrays.asList(
     * >         Arrays.asList("alpha", "bravo", "charlie"),
     * >         Arrays.asList(10, 20, 30)
     * >     )
     * > );
     * > df.plot(PlotType.AREA);
     * } </pre>
     * @param type the type of plot to display
     */
    public final void plot(final PlotType type) {
        Display.plot(this, type);
    }

    /**
     * Draw the numeric columns of this data frame
     * as a chart in the specified {@link Container}.
     *
     * @param container the container to use for the chart
     */
    public final void draw(final Container container) {
        Display.draw(this, container, PlotType.LINE);
    }

    /**
     * Draw the numeric columns of this data frame  as a chart
     * in the specified {@link Container} using the specified type.
     *
     * @param container the container to use for the chart
     * @param type the type of plot to draw
     */
    public final void draw(final Container container, final PlotType type) {
        Display.draw(this, container, type);
    }

    public final void show() {
        Display.show(this);
    }

    public static final <V> DataFrame<String> compare(final DataFrame<V> df1, final DataFrame<V> df2) {
        return Comparison.compare(df1, df2);
    }

    /**
     * Read the specified csv file and
     * return the data as a data frame.
     *
     * @param file the csv file
     * @return a new data frame
     * @throws IOException if an error reading the file occurs
     */
    public static final DataFrame<Object> readCsv(final String file)
    throws IOException {
        return Serialization.readCsv(file);
    }

    /**
     * Read csv records from an input stream
     * and return the data as a data frame.
     *
     * @param input the input stream
     * @return a new data frame
     * @throws IOException if an error reading the stream occurs
     */
    public static final DataFrame<Object> readCsv(final InputStream input)
    throws IOException {
        return Serialization.readCsv(input);
    }

    public static final DataFrame<Object> readCsv(final String file, final String separator)
    throws IOException {
        return Serialization.readCsv(file, separator, NumberDefault.LONG_DEFAULT);
    }

    public static final DataFrame<Object> readCsv(final InputStream input, final String separator)
    throws IOException {
        return Serialization.readCsv(input, separator, NumberDefault.LONG_DEFAULT, null);
    }

    public static final DataFrame<Object> readCsv(final InputStream input, final String separator, final String naString)
    throws IOException {
        return Serialization.readCsv(input, separator, NumberDefault.LONG_DEFAULT, naString);
    }

    public static final DataFrame<Object> readCsv(final InputStream input, final String separator, final String naString, final boolean hasHeader)
    throws IOException {
        return Serialization.readCsv(input, separator, NumberDefault.LONG_DEFAULT, naString, hasHeader);
    }

    public static final DataFrame<Object> readCsv(final String file, final String separator, final String naString, final boolean hasHeader)
    throws IOException {
        return Serialization.readCsv(file, separator, NumberDefault.LONG_DEFAULT, naString, hasHeader);
    }

    public static final DataFrame<Object> readCsv(final String file, final String separator, final NumberDefault numberDefault, final String naString, final boolean hasHeader)
    throws IOException {
        return Serialization.readCsv(file, separator, numberDefault, naString, hasHeader);
    }

    public static final DataFrame<Object> readCsv(final String file, final String separator, final NumberDefault longDefault)
    throws IOException {
        return Serialization.readCsv(file, separator, longDefault);
    }

    public static final DataFrame<Object> readCsv(final String file, final String separator, final NumberDefault longDefault, final String naString)
    throws IOException {
        return Serialization.readCsv(file, separator, longDefault, naString);
    }

    public static final DataFrame<Object> readCsv(final InputStream input, final String separator, final NumberDefault longDefault)
    throws IOException {
        return Serialization.readCsv(input, separator, longDefault, null);
    }

    /**
     * Write the data from this data frame to
     * the specified file as comma separated values.
     *
     * @param file the file to write
     * @throws IOException if an error occurs writing the file
     */
    public final void writeCsv(final String file)
    throws IOException {
        Serialization.writeCsv(this, new FileOutputStream(file));
    }

    /**
     * Write the data from this data frame to
     * the provided output stream as comma separated values.
     *
     * @param output
     * @throws IOException
     */
    public final void writeCsv(final OutputStream output)
    throws IOException {
        Serialization.writeCsv(this, output);
    }

    /**
     * Read data from the specified excel
     * workbook into a new data frame.
     *
     * @param file the excel workbook
     * @return a new data frame
     * @throws IOException if an error occurs reading the workbook
     */
    public static final DataFrame<Object> readXls(final String file)
    throws IOException {
        return Serialization.readXls(file);
    }

    /**
     * Read data from the input stream as an
     * excel workbook into a new data frame.
     *
     * @param input the input stream
     * @return a new data frame
     * @throws IOException if an error occurs reading the input stream
     */
    public static final DataFrame<Object> readXls(final InputStream input)
    throws IOException {
        return Serialization.readXls(input);
    }

    /**
     * Write the data from the data frame
     * to the specified file as an excel workbook.
     *
     * @param file the file to write
     * @throws IOException if an error occurs writing the file
     */
    public final void writeXls(final String file)
    throws IOException {
        Serialization.writeXls(this, new FileOutputStream(file));
    }

    /**
     * Write the data from the data frame
     * to the provided output stream as an excel workbook.
     *
     * @param output the file to write
     * @throws IOException if an error occurs writing the file
     */
    public final void writeXls(final OutputStream output)
    throws IOException {
        Serialization.writeXls(this, output);
    }

    /**
     * Execute the SQL query and return the results as a new data frame.
     *
     * <pre> {@code
     * > Connection c = DriverManager.getConnection("jdbc:derby:memory:testdb;create=true");
     * > c.createStatement().executeUpdate("create table data (a varchar(8), b int)");
     * > c.createStatement().executeUpdate("insert into data values ('test', 1)");
     * > DataFrame.readSql(c, "select * from data").flatten();
     * [test, 1] }</pre>
     *
     * @param c the database connection
     * @param sql the SQL query
     * @return a new data frame
     * @throws SQLException if an error occurs execution the query
     */
    public static final DataFrame<Object> readSql(final Connection c, final String sql)
    throws SQLException {
        try (Statement stmt = c.createStatement()) {
            return readSql(stmt.executeQuery(sql));
        }
    }

    /**
     * Read data from the provided query results into a new data frame.
     *
     * @param rs the query results
     * @return a new data frame
     * @throws SQLException if an error occurs reading the results
     */
    public static final DataFrame<Object> readSql(final ResultSet rs)
    throws SQLException {
        return Serialization.readSql(rs);
    }

    /**
     * Write the data from the data frame to a database by
     * executing the specified SQL statement.
     *
     * @param c the database connection
     * @param sql the SQL statement
     * @throws SQLException if an error occurs executing the statement
     */
    public final void writeSql(final Connection c, final String sql)
    throws SQLException {
        writeSql(c.prepareStatement(sql));
    }

    /**
     * Write the data from the data frame to a database by
     * executing the provided prepared SQL statement.
     *
     * @param stmt a prepared insert statement
     * @throws SQLException if an error occurs executing the statement
     */
    public final void writeSql(final PreparedStatement stmt)
    throws SQLException {
        Serialization.writeSql(this, stmt);
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
     * @see DataFrame#apply(Function)
     * @see DataFrame#aggregate(Aggregate)
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
     * @see DataFrame#aggregate(Aggregate)
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

    /**
     * An enumeration of join types for joining data frames together.
     */
    public enum JoinType {
        INNER,
        OUTER,
        LEFT,
        RIGHT
    }

    /**
     * An enumeration of plot types for displaying data frames with charts.
     */
    public enum PlotType {
        SCATTER,
        SCATTER_WITH_TREND,
        LINE,
        LINE_AND_POINTS,
        AREA,
        BAR,
        GRID,
        GRID_WITH_TREND
    }

    /**
     * An enumeration of data frame axes.
     */
    public enum Axis {
        ROWS,
        COLUMNS
    }

    public static enum NumberDefault {
        LONG_DEFAULT,
        DOUBLE_DEFAULT
    }

    /**
     * Entry point to joinery as a command line tool.
     *
     * The available commands are:
     * <dl>
     *   <dt>show</dt><dd>display the specified data frame as a swing table</dd>
     *   <dt>plot</dt><dd>display the specified data frame as a chart</dd>
     *   <dt>compare</dt><dd>merge the specified data frames and output the result</dd>
     *   <dt>shell</dt><dd>launch an interactive javascript shell for exploring data</dd>
     * </dl>
     *
     * @param args file paths or urls of csv input data
     * @throws IOException if an error occurs reading input
     */
    public static final void main(final String[] args)
    throws IOException {
        final List<DataFrame<Object>> frames = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            frames.add(DataFrame.readCsv(args[i]));
        }

        if (args.length > 0 && "plot".equalsIgnoreCase(args[0])) {
            if (frames.size() == 1) {
                frames.get(0).plot();
                return;
            }
        }

        if (args.length > 0 && "show".equalsIgnoreCase(args[0])) {
            if (frames.size() == 1) {
                frames.get(0).show();
                return;
            }
        }

        if (args.length > 0 && "compare".equalsIgnoreCase(args[0])) {
            if (frames.size() == 2) {
                System.out.println(DataFrame.compare(frames.get(0), frames.get(1)));
                return;
            }
        }

        if (args.length > 0 && "shell".equalsIgnoreCase(args[0])) {
            Shell.repl(frames);
            return;
        }

        System.err.printf(
                "usage: %s [compare|plot|show|shell] [csv-file ...]\n",
                DataFrame.class.getCanonicalName()
            );
        System.exit(255);
    }


}
