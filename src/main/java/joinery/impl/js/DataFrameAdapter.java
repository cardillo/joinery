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

package joinery.impl.js;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joinery.DataFrame;
import joinery.DataFrame.Aggregate;
import joinery.DataFrame.JoinType;
import joinery.DataFrame.KeyFunction;
import joinery.DataFrame.PlotType;
import joinery.DataFrame.Predicate;
import joinery.DataFrame.RowFunction;
import joinery.LocalDataFrame;
import joinery.impl.Grouping;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.NativeArray;
import org.mozilla.javascript.NativeJavaObject;
import org.mozilla.javascript.ScriptRuntime;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

/*
 * there are basically two options for assisting in method resolution
 * from javascript:
 *   1. wrap data frames in a subclass of NativeJavaObject and use get()
 *      to return the appropriate NativeJavaMethod object
 *      issues with this approach include:
 *      - needing to wrap both the object and the method definition
 *      - difficulty in determining the correct method from the argument types
 *   2. wrap data frames in a custom scriptable object with unambiguous
 *      methods that delegates to the underlying data frame
 *      issues with this approach include:
 *      - need to redefine every bit of data frame functionality in terms of javascript
 *      - need to keep up to data as new methods are added to make them available in js
 * after trying each, getting the correct method dynamically is not
 * worth the effort so for now the more verbose delegate class solution
 * wins out.  Java8s nashorn interpreter appears to do a better job
 * resolving methods (via dynalink) so hopefully this is short-lived
 */
public class DataFrameAdapter
extends ScriptableObject {

    private static final long serialVersionUID = 1L;
    private final DataFrame<Object> df;
    private static final DataFrame<Object> EMPTY_DF = new LocalDataFrame<>();

    public DataFrameAdapter() {
        this.df = EMPTY_DF;
    }

    public DataFrameAdapter(final DataFrame<Object> df) {
        this.df = df;
    }

    public DataFrameAdapter(final Scriptable scope, final DataFrame<Object> df) {
        this.df = df;
        setParentScope(scope.getParentScope());
        setPrototype(scope.getPrototype());
    }

    public static Scriptable jsConstructor(final Context ctx, final Object[] args, final Function ctor, final boolean newExpr) {
        if (args.length == 3 && args[0] instanceof NativeArray) {
            final List<List<Object>> data = new ArrayList<>();
            final NativeArray array = NativeArray.class.cast(args[2]);
            final Object[] ids = array.getIds();
            for (int i = 0; i < array.getLength(); i++) {
                data.add(asList(array.get((int)ids[i], null)));
            }
            return new DataFrameAdapter(
                    new LocalDataFrame<Object>(
                            asList(args[0]),
                            asList(args[1]),
                            data
                        )
                );
        } else if (args.length == 2 && args[0] instanceof NativeArray) {
            return new DataFrameAdapter(new LocalDataFrame<Object>(
                    asList(args[0]),
                    asList(args[1])
                ));
        } else if (args.length == 1 && args[0] instanceof NativeArray) {
            return new DataFrameAdapter(new LocalDataFrame<Object>(
                    asList(args[0])
                ));
        } else if (args.length > 0) {
            final String[] columns = new String[args.length];
            for (int i = 0; i < args.length; i++) {
                columns[i] = Context.toString(args[i]);
            }
            return new DataFrameAdapter(new LocalDataFrame<>(columns));
        }
        return new DataFrameAdapter(new LocalDataFrame<>());
    }

    private static DataFrameAdapter cast(final Scriptable object) {
        return DataFrameAdapter.class.cast(object);
    }

    public static Scriptable jsFunction_add(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        if (args.length == 2 && args[1] instanceof NativeArray) {
            return new DataFrameAdapter(object, cast(object).df.add(args[0], asList(args[1])));
        }
        if (args.length == 1 && args[0] instanceof NativeArray) {
            return new DataFrameAdapter(object, cast(object).df.add(asList(args[0])));
        }
        return new DataFrameAdapter(object, cast(object).df.add(args));
    }

    public static Scriptable jsFunction_drop(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        return new DataFrameAdapter(object, cast(object).df.drop(args));
    }

    public static Scriptable jsFunction_retain(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        return new DataFrameAdapter(object, cast(object).df.retain(args));
    }

    public static Scriptable jsFunction_reindex(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        if (args.length > 0 && args[0] instanceof NativeArray) {
            if (args.length > 1) {
                return new DataFrameAdapter(object, cast(object).df.reindex(
                    asList(args[0]).toArray(), Context.toBoolean(args[1])));
            }
            return new DataFrameAdapter(object, cast(object).df.reindex(asList(args[0]).toArray()));
        }
        return new DataFrameAdapter(object, cast(object).df.reindex(args));
    }

    public DataFrameAdapter jsFunction_resetIndex() {
        return new DataFrameAdapter(this, df.resetIndex());
    }

    public DataFrameAdapter jsFunction_rename(final Object old, final Object name) {
        return new DataFrameAdapter(this, df.rename(old, name));
    }

    public static Scriptable jsFunction_append(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        if (args.length == 2 && args[1] instanceof NativeArray) {
            return new DataFrameAdapter(object, cast(object).df.append(args[0], asList(args[1])));
        }
        return new DataFrameAdapter(object, cast(object).df.append(asList(args[0])));
    }

    public DataFrameAdapter jsFunction_reshape(final Integer rows, final Integer cols) {
        return new DataFrameAdapter(this, df.reshape(rows, cols));
    }

    public static Scriptable jsFunction_join(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final DataFrame<Object> other = DataFrameAdapter.class.cast(args[0]).df;
        final JoinType type = args.length > 1 && args[1] instanceof NativeJavaObject ?
                JoinType.class.cast(Context.jsToJava(args[1], JoinType.class)) : null;
        if (args.length > 1 && args[args.length - 1] instanceof Function) {
            @SuppressWarnings("unchecked")
            final KeyFunction<Object> f = (KeyFunction<Object>)Context.jsToJava(args[args.length - 1], KeyFunction.class);
            if (type != null) {
                return new DataFrameAdapter(object, cast(object).df.join(other, type, f));
            }
            return new DataFrameAdapter(object, cast(object).df.join(other, f));
        }
        if (type != null) {
            return new DataFrameAdapter(object, cast(object).df.join(other, type));
        }
        return new DataFrameAdapter(object, cast(object).df.join(other));
    }

    public static Scriptable jsFunction_joinOn(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final DataFrame<Object> other = DataFrameAdapter.class.cast(args[0]).df;
        final JoinType type = args.length > 1 && args[1] instanceof NativeJavaObject ?
                JoinType.class.cast(Context.jsToJava(args[1], JoinType.class)) : null;
        if (type != null) {
            return new DataFrameAdapter(object, cast(object).df.joinOn(other, type, Arrays.copyOfRange(args, 2, args.length)));
        }
        return new DataFrameAdapter(object, cast(object).df.joinOn(other, Arrays.copyOfRange(args, 1, args.length)));
    }

    public static Scriptable jsFunction_merge(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final DataFrame<Object> other = DataFrameAdapter.class.cast(args[0]).df;
        final JoinType type = args.length > 1 && args[1] instanceof NativeJavaObject ?
                JoinType.class.cast(Context.jsToJava(args[1], JoinType.class)) : null;
        if (type != null) {
            return new DataFrameAdapter(object, cast(object).df.merge(other, type));
        }
        return new DataFrameAdapter(object, cast(object).df.merge(other));
    }

    public static Scriptable jsFunction_update(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final DataFrame<?>[] others = new DataFrame[args.length];
        for (int i = 0; i < args.length; i++) {
            others[i] = DataFrameAdapter.class.cast(args[i]).df;
        }
        return new DataFrameAdapter(object, cast(object).df.update(others));
    }

    public static Scriptable jsFunction_coalesce(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final DataFrame<?>[] others = new DataFrame[args.length];
        for (int i = 0; i < args.length; i++) {
            others[i] = DataFrameAdapter.class.cast(args[i]).df;
        }
        return new DataFrameAdapter(object, cast(object).df.coalesce(others));
    }

    public int jsFunction_size() {
        return df.size();
    }

    public int jsFunction_length() {
        return df.length();
    }

    public boolean jsFunction_isEmpty() {
        return df.isEmpty();
    }

    public Set<Object> jsFunction_index() {
        return df.index();
    }

    public Set<Object> jsFunction_columns() {
        return df.columns();
    }

    public Object jsFunction_get(final Integer row, final Integer col) {
        return df.get(row, col);
    }

    public DataFrameAdapter jsFunction_slice(final Integer rowStart, final Integer rowEnd, final Integer colStart, final Integer colEnd) {
        return new DataFrameAdapter(this, df.slice(rowStart, rowEnd, colStart, colEnd));
    }

    public void jsFunction_set(final Integer row, final Integer col, final Scriptable value) {
        df.set(row, col, Context.jsToJava(value, Object.class));
    }

    public List<Object> jsFunction_col(final Integer column) {
        return df.col(column);
    }

    public List<Object> jsFunction_row(final Integer row) {
        return df.row(row);
    }

    public DataFrameAdapter jsFunction_select(final Function predicate) {
        @SuppressWarnings("unchecked")
        final Predicate<Object> p = (Predicate<Object>)Context.jsToJava(predicate, Predicate.class);
        return new DataFrameAdapter(this, df.select(p));
    }

    public static Scriptable jsFunction_head(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final Number limit = args.length == 1 ? Context.toNumber(args[0]) : null;
        return new DataFrameAdapter(object,
                limit != null ?
                    cast(object).df.head(limit.intValue()) :
                    cast(object).df.head()
            );
    }

    public static Scriptable jsFunction_tail(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final Number limit = args.length == 1 ? Context.toNumber(args[0]) : null;
        return new DataFrameAdapter(object,
                limit != null ?
                    cast(object).df.tail(limit.intValue()) :
                    cast(object).df.tail()
            );
    }

    public List<Object> jsFunction_flatten() {
        return df.flatten();
    }

    public DataFrameAdapter jsFunction_transpose() {
        return new DataFrameAdapter(this, df.transpose());
    }

    public DataFrameAdapter jsFunction_apply(final Function function) {
        @SuppressWarnings("unchecked")
        final DataFrame.Function<Object, Object> f = (DataFrame.Function<Object, Object>)Context.jsToJava(function, DataFrame.Function.class);
        return new DataFrameAdapter(this, df.apply(f));
    }

    public DataFrameAdapter jsFunction_transform(final Function function) {
        @SuppressWarnings("unchecked")
        final RowFunction<Object, Object> f = (RowFunction<Object, Object>)Context.jsToJava(function, DataFrame.RowFunction.class);
        return new DataFrameAdapter(this, df.transform(f));
    }

    public static Scriptable jsFunction_convert(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        if (args.length > 0) {
            final Class<?>[] types = new Class[args.length];
            for (int i = 0; i < args.length; i++) {
                types[i] = Class.class.cast(Context.jsToJava(args[i], Class.class));
            }
            return new DataFrameAdapter(object, cast(object).df.convert(types));
        }
        return new DataFrameAdapter(object, cast(object).df.convert());
    }

    public DataFrameAdapter jsFunction_isnull() {
        return new DataFrameAdapter(this, df.isnull().cast(Object.class));
    }

    public DataFrameAdapter jsFunction_notnull() {
        return new DataFrameAdapter(this, df.notnull().cast(Object.class));
    }

    public static Scriptable jsFunction_groupBy(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        if (args.length == 1 && args[0] instanceof Function) {
            @SuppressWarnings("unchecked")
            final KeyFunction<Object> f = (KeyFunction<Object>)Context.jsToJava(args[0], KeyFunction.class);
            return new DataFrameAdapter(object, cast(object).df.groupBy(f));
        }
        if (args.length == 1 && args[0] instanceof NativeArray) {
            return new DataFrameAdapter(object, cast(object).df.groupBy(asList(args[0]).toArray()));
        }
        return new DataFrameAdapter(object, cast(object).df.groupBy(args));
    }

    public Grouping jsFunction_groups() {
        return df.groups();
    }

    public Map<Object, DataFrame<Object>> jsFunction_explode() {
        return df.explode();
    }

    public DataFrameAdapter jsFunction_aggregate(final Function function) {
        @SuppressWarnings("unchecked")
        final Aggregate<Object, Object> f = (Aggregate<Object, Object>)Context.jsToJava(function, Aggregate.class);
        return new DataFrameAdapter(this, df.aggregate(f));
    }

    public DataFrameAdapter jsFunction_count() {
        return new DataFrameAdapter(this, df.count());
    }

    public DataFrameAdapter jsFunction_collapse() {
        return new DataFrameAdapter(this, df.collapse());
    }

    public DataFrameAdapter jsFunction_sum() {
        return new DataFrameAdapter(this, df.sum());
    }

    public DataFrameAdapter jsFunction_prod() {
        return new DataFrameAdapter(this, df.prod());
    }

    public DataFrameAdapter jsFunction_mean() {
        return new DataFrameAdapter(this, df.mean());
    }

    public DataFrameAdapter jsFunction_stddev() {
        return new DataFrameAdapter(this, df.stddev());
    }

    public DataFrameAdapter jsFunction_var() {
        return new DataFrameAdapter(this, df.var());
    }

    public DataFrameAdapter jsFunction_skew() {
        return new DataFrameAdapter(this, df.skew());
    }

    public DataFrameAdapter jsFunction_kurt() {
        return new DataFrameAdapter(this, df.kurt());
    }

    public DataFrameAdapter jsFunction_min() {
        return new DataFrameAdapter(this, df.min());
    }

    public DataFrameAdapter jsFunction_max() {
        return new DataFrameAdapter(this, df.max());
    }

    public DataFrameAdapter jsFunction_median() {
        return new DataFrameAdapter(this, df.median());
    }

    public DataFrameAdapter jsFunction_cumsum() {
        return new DataFrameAdapter(this, df.cumsum());
    }

    public DataFrameAdapter jsFunction_cumprod() {
        return new DataFrameAdapter(this, df.cumprod());
    }

    public DataFrameAdapter jsFunction_cummin() {
        return new DataFrameAdapter(this, df.cummin());
    }

    public DataFrameAdapter jsFunction_cummax() {
        return new DataFrameAdapter(this, df.cummax());
    }

    public DataFrameAdapter jsFunction_describe() {
        return new DataFrameAdapter(this, df.describe());
    }

    public static Scriptable jsFunction_pivot(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final Object row = Context.jsToJava(args[0], Object.class);
        final Object col = Context.jsToJava(args[0], Object.class);
        final Object[] values = new Object[args.length - 2];
        for (int i = 0; i < values.length; i++) {
            values[i] = Context.jsToJava(args[i + 2], Object.class);
        }
        return new DataFrameAdapter(object, cast(object).df.pivot(row, col, values));
    }

    public static Scriptable jsFunction_sortBy(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        return new DataFrameAdapter(object, cast(object).df.sortBy(args));
    }

    public List<Class<?>> jsFunction_types() {
        return df.types();
    }

    public DataFrameAdapter jsFunction_numeric() {
        return new DataFrameAdapter(this, df.numeric().cast(Object.class));
    }

    public DataFrameAdapter jsFunction_nonnumeric() {
        return new DataFrameAdapter(this, df.nonnumeric());
    }

    public Map<Object, List<Object>> jsFunction_map(final Object key, final Object value) {
        return df.map(key, value);
    }

    public static Scriptable jsFunction_unique(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        return new DataFrameAdapter(object, cast(object).df.unique(args));
    }

    public static Scriptable jsFunction_diff(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final Number period = args.length == 1 ? Context.toNumber(args[0]) : 1;
        return new DataFrameAdapter(object, cast(object).df.diff(period.intValue()));
    }

    public static Scriptable jsFunction_percentChange(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final Number period = args.length == 1 ? Context.toNumber(args[0]) : 1;
        return new DataFrameAdapter(object, cast(object).df.percentChange(period.intValue()));
    }

    public static Scriptable jsFunction_rollapply(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        @SuppressWarnings("unchecked")
        final DataFrame.Function<List<Object>, Object> f = (DataFrame.Function<List<Object>, Object>)Context.jsToJava(args[0], DataFrame.Function.class);
        final Number period = args.length == 2 ? Context.toNumber(args[1]) : 1;
        return new DataFrameAdapter(object, cast(object).df.rollapply(f, period.intValue()));
    }

    public static Object jsFunction_plot(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        if (args.length > 0) {
            final PlotType type = PlotType.class.cast(Context.jsToJava(args[0], PlotType.class));
            cast(object).df.plot(type);
            return Context.getUndefinedValue();
        }
        cast(object).df.plot();
        return Context.getUndefinedValue();
    }

    public void jsFunction_show() {
        df.show();
    }

    public static Scriptable jsStaticFunction_readCsv(final Context ctx, final Scriptable object, final Object[] args, final Function func)
    throws IOException {
        final String file = Context.toString(args[0]);
        final DataFrame<Object> df = DataFrame.readCsv(file);
        return new DataFrameAdapter(ctx.newObject(object, df.getClass().getSimpleName()), df);
    }

    public void jsFunction_writeCsv(final String file)
    throws IOException {
        df.writeCsv(file);
    }

    public static Scriptable jsStaticFunction_readXls(final Context ctx, final Scriptable object, final Object[] args, final Function func)
    throws IOException {
        final String file = Context.toString(args[0]);
        final DataFrame<Object> df = DataFrame.readXls(file);
        return new DataFrameAdapter(ctx.newObject(object, df.getClass().getSimpleName()), df);
    }

    public void jsFunction_writeXls(final String file)
    throws IOException {
        df.writeXls(file);
    }

    public static Scriptable jsFunction_toArray(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        return ctx.newArray(object, cast(object).df.toArray());
    }

    public static Object jsFunction_toString(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
        final Number limit = args.length == 1 ? Context.toNumber(args[0]) : null;
        return limit != null ?
            cast(object).df.toString(limit.intValue()) :
            cast(object).df.toString();
    }

    @Override
    public Object getDefaultValue(final Class<?> hint) {
        if (hint == ScriptRuntime.BooleanClass) {
            return df.isEmpty();
        }
        return df.toString();
    }

    @Override
    public String getClassName() {
        return df.getClass().getSimpleName();
    }

    @Override
    public Object[] getIds() {
        final List<String> ids = new ArrayList<>();
        for (final Method m : getClass().getMethods()) {
            final String name = m.getName();
            if (name.startsWith("js") && name.contains("_")) {
                ids.add(name.substring(name.indexOf('_') + 1));
            }
        }
        return ids.toArray();
    }

    @Override
    public Object[] getAllIds() {
        return getIds();
    }

    @Override
    public boolean equals(final Object o) {
        return df.equals(o);
    }

    @Override
    public int hashCode() {
        return df.hashCode();
    }

    private static List<Object> asList(final Object array) {
        return asList(NativeArray.class.cast(array));
    }

    private static List<Object> asList(final NativeArray array) {
        final List<Object> list = new ArrayList<>((int)array.getLength());
        for (final Object id : array.getIds()) {
            list.add(array.get((int)id, null));
        }
        return list;
    }
}
