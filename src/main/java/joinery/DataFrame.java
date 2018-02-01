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

import com.codahale.metrics.annotation.Timed;
import joinery.impl.Comparison;
import joinery.impl.Grouping;
import joinery.impl.Serialization;

import java.awt.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.List;

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
 * >     .groupBy(new KeyFunction<Object>() {
 * >         public Object apply(List<Object> row) {
 * >             return Date.class.cast(row.get(0)).getMonth();
 * >         }
 * >     })
 * >     .mean()
 * >     .sortBy("Close")
 * >     .tail(3)
 * >     .apply(new Function<Object, Number>() {
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
public interface DataFrame<V> extends Iterable<List<V>> {
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
    DataFrame<V> add(Object... columns);

    DataFrame<V> add(List<V> values);

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
    DataFrame<V> add(Object column, List<V> values);

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
    DataFrame<V> drop(Object... cols);

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
    DataFrame<V> drop(Integer... cols);

    DataFrame<V> dropna();

    DataFrame<V> dropna(Axis direction);

    /**
     * Returns a view of the of data frame with NA's replaced with {@code fill}.
     *
     * @param fill the value used to replace missing values
     * @return the new data frame
     */
    DataFrame<V> fillna(V fill);

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
    DataFrame<V> retain(Object... cols);

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
    DataFrame<V> retain(Integer... cols);

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
    DataFrame<V> reindex(Integer col, boolean drop);

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
    DataFrame<V> reindex(Integer[] cols, boolean drop);

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
    DataFrame<V> reindex(Integer... cols);

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
    DataFrame<V> reindex(Object col, boolean drop);

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
    DataFrame<V> reindex(Object[] cols, boolean drop);

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
    DataFrame<V> reindex(Object... cols);

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
    DataFrame<V> resetIndex();

    DataFrame<V> rename(Object old, Object name);

    DataFrame<V> rename(Map<Object, Object> names);

    DataFrame<V> append(Object name, V[] row);

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
    DataFrame<V> append(List<? extends V> row);

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
    DataFrame<V> append(Object name, List<? extends V> row);

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
    DataFrame<V> reshape(Integer rows, Integer cols);

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
    DataFrame<V> reshape(Collection<?> rows, Collection<?> cols);

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
    DataFrame<V> join(DataFrame<V> other);

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * using the row indices as the join key.
     *
     * @param other the other data frame
     * @param join the join type
     * @return the result of the join operation as a new data frame
     */
    DataFrame<V> join(DataFrame<V> other, JoinType join);

    /**
     * Return a new data frame created by performing a left outer join of this
     * data frame with the argument using the specified key function.
     *
     * @param other the other data frame
     * @param on the function to generate the join keys
     * @return the result of the join operation as a new data frame
     */
    DataFrame<V> join(DataFrame<V> other, KeyFunction<V> on);

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
    DataFrame<V> join(DataFrame<V> other, JoinType join, KeyFunction<V> on);

    /**
     * Return a new data frame created by performing a left outer join of
     * this data frame with the argument using the column values as the join key.
     *
     * @param other the other data frame
     * @param cols the indices of the columns to use as the join key
     * @return the result of the join operation as a new data frame
     */
    DataFrame<V> joinOn(DataFrame<V> other, Integer... cols);

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
    DataFrame<V> joinOn(DataFrame<V> other, JoinType join, Integer... cols);

    /**
     * Return a new data frame created by performing a left outer join of
     * this data frame with the argument using the column values as the join key.
     *
     * @param other the other data frame
     * @param cols the names of the columns to use as the join key
     * @return the result of the join operation as a new data frame
     */
    DataFrame<V> joinOn(DataFrame<V> other, Object... cols);

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
    DataFrame<V> joinOn(DataFrame<V> other, JoinType join, Object... cols);

    /**
     * Return a new data frame created by performing a left outer join of this
     * data frame with the argument using the common, non-numeric columns
     * from each data frame as the join key.
     *
     * @param other the other data frame
     * @return the result of the merge operation as a new data frame
     */
    DataFrame<V> merge(DataFrame<V> other);

    /**
     * Return a new data frame created by performing a join of this
     * data frame with the argument using the specified join type and
     * the common, non-numeric columns from each data frame as the join key.
     *
     * @param other the other data frame
     * @return the result of the merge operation as a new data frame
     */
    DataFrame<V> merge(DataFrame<V> other, JoinType join);

    /**
     * Update the data frame in place by overwriting the any values
     * with the non-null values provided by the data frame arguments.
     *
     * @param others the other data frames
     * @return this data frame with the overwritten values
     */
    //todo //@SafeVarargs
    DataFrame<V> update(DataFrame<? extends V>... others);

    /**
     * Update the data frame in place by overwriting any null values with
     * any non-null values provided by the data frame arguments.
     *
     * @param others the other data frames
     * @return this data frame with the overwritten values
     */
    //todo @SafeVarargs
    DataFrame<V> coalesce(DataFrame<? extends V>... others);

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
    int size();

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
     * @return the number of columns
     */
    int length();

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
    boolean isEmpty();

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
    Set<Object> index();

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
    Set<Object> columns();

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
    V get(Object row, Object col);

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
    V get(Integer row, Integer col);

    DataFrame<V> slice(Object rowStart, Object rowEnd);

    DataFrame<V> slice(Object rowStart, Object rowEnd, Object colStart, Object colEnd);

    DataFrame<V> slice(Integer rowStart, Integer rowEnd);

    DataFrame<V> slice(Integer rowStart, Integer rowEnd, Integer colStart, Integer colEnd);

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
    void set(Object row, Object col, V value);

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
    void set(Integer row, Integer col, V value);

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
    List<V> col(Object column);

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
    List<V> col(Integer column);

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
    List<V> row(Object row);

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
    List<V> row(Integer row);

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
    DataFrame<V> select(Predicate<V> predicate);

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
    DataFrame<V> head();

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
    DataFrame<V> head(int limit);

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
    DataFrame<V> tail();

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
    DataFrame<V> tail(int limit);

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
    List<V> flatten();

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
    DataFrame<V> transpose();

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
    <U> DataFrame<U> apply(Function<V, U> function);

    <U> DataFrame<U> transform(RowFunction<V, U> transform);

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
    DataFrame<V> convert();

    DataFrame<V> convert(NumberDefault numDefault, String naString);

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
    //todo @SafeVarargs
    DataFrame<V> convert(Class<? extends V>... columnTypes);

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
    DataFrame<Boolean> isnull();

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
    DataFrame<Boolean> notnull();

    /**
     * Copy the values of contained in the data frame into a
     * flat array of length {@code #size()} * {@code #length()}.
     *
     * @return the array
     */
    Object[] toArray();

    /**
     * Copy the values of contained in the data frame into the
     * specified array. If the length of the provided array is
     * less than length {@code #size()} * {@code #length()} a
     * new array will be created.
     *
     * @return the array
     */
    <U> U[] toArray(U[] array);

    @SuppressWarnings("unchecked")
    <U> U[][] toArray(U[][] array);

    /**
     * Copy the values of contained in the data frame into a
     * array of the specified type.  If the type specified is
     * a two dimensional array, for example {@code double[][].class},
     * a row-wise copy will be made.
     *
     * @throws IllegalArgumentException if the values are not assignable to the specified component type
     * @return the array
     */
    <U> U toArray(Class<U> cls);

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
    double[][] toModelMatrix(double fillValue);

    /**
     *  Encodes the DataFrame as a model matrix, converting nominal values
     *  to dummy variables but does not add an intercept column.
     *
     *   More methods with additional parameters to control the conversion to
     *   the model matrix are available in the <code>Conversion</code> class.
     *
     * @return a model matrix
     */
    DataFrame<Number> toModelMatrixDataFrame();

    /**
     * Group the data frame rows by the specified column names.
     *
     * @param cols the column names
     * @return the grouped data frame
     */
    @Timed
    DataFrame<V> groupBy(Object... cols);

    /**
     * Group the data frame rows by the specified columns.
     *
     * @param cols the column indices
     * @return the grouped data frame
     */
    @Timed
    DataFrame<V> groupBy(Integer... cols);

    /**
     * Group the data frame rows using the specified key function.
     *
     * @param function the function to reduce rows to grouping keys
     * @return the grouped data frame
     */
    @Timed
    DataFrame<V> groupBy(KeyFunction<V> function);

    Grouping groups();

    /**
     * Return a map of group names to data frame for grouped
     * data frames. Observe that for this method to have any
     * effect a {@code groupBy} call must have been done before.
     *
     * @return a map of group names to data frames
     */
    Map<Object, DataFrame<V>> explode();

    /**
     * Apply an aggregate function to each group or the entire
     * data frame if the data is not grouped.
     *
     * @param function the aggregate function
     * @return the new data frame
     */
    <U> DataFrame<V> aggregate(Aggregate<V, U> function);

    @Timed
    DataFrame<V> count();

    DataFrame<V> collapse();

    DataFrame<V> unique();

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
    DataFrame<V> sum();

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
    DataFrame<V> prod();

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
    DataFrame<V> mean();

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
    DataFrame<V> percentile(double quantile);

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
    DataFrame<V> stddev();

    @Timed
    DataFrame<V> var();

    @Timed
    DataFrame<V> skew();

    @Timed
    DataFrame<V> kurt();

    @Timed
    DataFrame<V> min();

    @Timed
    DataFrame<V> max();

    @Timed
    DataFrame<V> median();

    @Timed
    DataFrame<Number> cov();

    @Timed
    DataFrame<V> cumsum();

    @Timed
    DataFrame<V> cumprod();

    @Timed
    DataFrame<V> cummin();

    @Timed
    DataFrame<V> cummax();

    @Timed
    DataFrame<V> describe();

    DataFrame<V> pivot(Object row, Object col, Object... values);

    DataFrame<V> pivot(List<Object> rows, List<Object> cols, List<Object> values);

    DataFrame<V> pivot(Integer row, Integer col, Integer... values);

    @Timed
    DataFrame<V> pivot(Integer[] rows, Integer[] cols, Integer[] values);

    @Timed
    <U> DataFrame<U> pivot(KeyFunction<V> rows, KeyFunction<V> cols, Map<Integer, Aggregate<V, U>> values);

    DataFrame<V> sortBy(Object... cols);

    @Timed
    DataFrame<V> sortBy(Integer... cols);

    DataFrame<V> sortBy(Comparator<List<V>> comparator);

    /**
     * Return the types for each of the data frame columns.
     *
     * @return the list of column types
     */
    List<Class<?>> types();

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
    DataFrame<Number> numeric();

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
    DataFrame<V> nonnumeric();

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
    ListIterator<List<V>> iterator();

    ListIterator<List<V>> iterrows();

    ListIterator<List<V>> itercols();

    ListIterator<Map<Object, V>> itermap();

    ListIterator<V> itervalues();

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
    <T> DataFrame<T> cast(Class<T> cls);

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
    Map<Object, List<V>> map();

    Map<V, List<V>> map(Object key, Object value);

    Map<V, List<V>> map(Integer key, Integer value);

    DataFrame<V> unique(Object... cols);

    DataFrame<V> unique(Integer... cols);

    DataFrame<V> diff();

    DataFrame<V> diff(int period);

    DataFrame<V> percentChange();

    DataFrame<V> percentChange(int period);

    DataFrame<V> rollapply(Function<List<V>, V> function);

    DataFrame<V> rollapply(Function<List<V>, V> function, int period);

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
    void plot();

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
    void plot(PlotType type);

    /**
     * Draw the numeric columns of this data frame
     * as a chart in the specified {@link Container}.
     *
     * @param container the container to use for the chart
     */
    void draw(Container container);

    /**
     * Draw the numeric columns of this data frame  as a chart
     * in the specified {@link Container} using the specified type.
     *
     * @param container the container to use for the chart
     * @param type the type of plot to draw
     */
    void draw(Container container, PlotType type);

    void show();

    static  <V> DataFrame<String> compare(final DataFrame<V> df1, final DataFrame<V> df2) {
        return Comparison.compare(df1, df2);
    }

    static DataFrame<Object> readCsv(final String file)
            throws IOException {
        return Serialization.readCsv(file);
    }

    static DataFrame<Object> readCsv(final InputStream input)
            throws IOException {
        return Serialization.readCsv(input);
    }

    static DataFrame<Object> readCsv(final String file, final String separator)
            throws IOException {
        return Serialization.readCsv(file, separator, NumberDefault.LONG_DEFAULT);
    }

    static DataFrame<Object> readCsv(final InputStream input, final String separator)
            throws IOException {
        return Serialization.readCsv(input, separator, NumberDefault.LONG_DEFAULT, null);
    }

    static DataFrame<Object> readCsv(final InputStream input, final String separator, final String naString)
            throws IOException {
        return Serialization.readCsv(input, separator, NumberDefault.LONG_DEFAULT, naString);
    }

    static DataFrame<Object> readCsv(final InputStream input, final String separator, final String naString, final boolean hasHeader)
            throws IOException {
        return Serialization.readCsv(input, separator, NumberDefault.LONG_DEFAULT, naString, hasHeader);
    }

    static DataFrame<Object> readCsv(final String file, final String separator, final String naString, final boolean hasHeader)
            throws IOException {
        return Serialization.readCsv(file, separator, NumberDefault.LONG_DEFAULT, naString, hasHeader);
    }

    static DataFrame<Object> readCsv(final String file, final String separator, final NumberDefault numberDefault, final String naString, final boolean hasHeader)
            throws IOException {
        return Serialization.readCsv(file, separator, numberDefault, naString, hasHeader);
    }

    static DataFrame<Object> readCsv(final String file, final String separator, final NumberDefault longDefault)
            throws IOException {
        return Serialization.readCsv(file, separator, longDefault);
    }

    static DataFrame<Object> readCsv(final String file, final String separator, final NumberDefault longDefault, final String naString)
            throws IOException {
        return Serialization.readCsv(file, separator, longDefault, naString);
    }

    static DataFrame<Object> readCsv(final InputStream input, final String separator, final NumberDefault longDefault)
            throws IOException {
        return Serialization.readCsv(input, separator, longDefault, null);
    }

    void writeCsv(String file)
    throws IOException;

    void writeCsv(OutputStream output)
    throws IOException;

    static DataFrame<Object> readXls(final String file)
            throws IOException {
        return Serialization.readXls(file);
    }

    static DataFrame<Object> readXls(final InputStream input)
            throws IOException {
        return Serialization.readXls(input);
    }

    void writeXls(String file)
    throws IOException;

    void writeXls(OutputStream output)
    throws IOException;

    String toString(int limit);

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
}
