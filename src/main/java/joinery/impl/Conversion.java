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

import java.text.DateFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import joinery.DataFrame;
import joinery.DataFrame.Function;
import joinery.DataFrame.NumberDefault;

public class Conversion {
    public static <V> void convert(final DataFrame<V> df) {
        convert(df, NumberDefault.LONG_DEFAULT, null);
    }

    public static <V> void convert(final DataFrame<V> df, final NumberDefault numDefault, final String naString) {
        final Map<Integer, Function<V, ?>> conversions = new HashMap<>();
        List<Function<V, ?>> converters;
        final int rows = df.length();
        final int cols = df.size();

        switch (numDefault) {
            case LONG_DEFAULT:
                converters = Arrays.<Function<V, ?>>asList(
                    new LongConversion<V>(),
                    new DoubleConversion<V>(),
                    new BooleanConversion<V>(),
                    new DateTimeConversion<V>());
                break;
            case DOUBLE_DEFAULT:
                converters = Arrays.<Function<V, ?>>asList(
                    new DoubleConversion<V>(),
                    new LongConversion<V>(),
                    new BooleanConversion<V>(),
                    new DateTimeConversion<V>());
                break;
            default:
                throw new IllegalArgumentException("Number default contains an Illegal value");
        }

        NAConversion<V> naConverter = new NAConversion<>(naString);
        // find conversions
        for (int c = 0; c < cols; c++) {
            for (final Function<V, ?> conv : converters) {
                boolean all = true;
                for (int r = 0; r < rows; r++) {
                    if (conv.apply(df.get(r, c)) == null && naConverter.apply(df.get(r, c)) != null) {
                        all = false;
                        break;
                    }
                }
                if (all) {
                    conversions.put(c, conv);
                    break;
                }
            }
        }

        // apply conversions
        convert(df, conversions, naString);
    }

    @SafeVarargs
    public static <V> void convert(final DataFrame<V> df, final Class<? extends V> ... columnTypes) {
        final Map<Integer, Function<V, ?>> conversions = new HashMap<>();
        for (int i = 0; i < columnTypes.length; i++) {
            final Class<? extends V> cls = columnTypes[i];
            if (cls != null) {
                Function<V, ?> conv = null;
                if (Date.class.isAssignableFrom(cls)) {
                    conv = new DateTimeConversion<V>();
                } else if (Boolean.class.isAssignableFrom(cls)) {
                    conv = new BooleanConversion<V>();
                } else if (Long.class.isAssignableFrom(cls)) {
                    conv = new LongConversion<V>();
                } else if (Number.class.isAssignableFrom(cls)) {
                    conv =  new DoubleConversion<V>();
                } else if (String.class.isAssignableFrom(cls)) {
                    conv = new StringConversion<V>();
                }
                conversions.put(i, conv);
            }
        }
        convert(df, conversions, null);
    }

    @SuppressWarnings("unchecked")
    public static <V> void convert(final DataFrame<V> df, final Map<Integer, Function<V, ?>> conversions, String naString) {
        final int rows = df.length();
        final int cols = df.size();
        for (int c = 0; c < cols; c++) {
            final Function<V, ?> conv = conversions.get(c);
            if (conv != null) {
                for (int r = 0; r < rows; r++) {
                    df.set(r, c, (V)conv.apply(df.get(r, c)));
                }
            } 
            else {
            	NAConversion<V> naConverter = new NAConversion<>(naString);
            	for (int r = 0; r < rows; r++) {
                    df.set(r, c, (V)naConverter.apply(df.get(r, c)));
                }
            }
        }
    }
    
    public static <V> double[][] toModelMatrix(final DataFrame<V> df, double fillValue) {
        return toModelMatrixDataFrame(df).fillna(fillValue).toArray(double[][].class);
    }

    public static <V> double[][] toModelMatrix(final DataFrame<V> df, double fillValue, boolean addIntercept) {
        return toModelMatrixDataFrame(df, null, addIntercept).fillna(fillValue).toArray(double[][].class);
    }

    public static <V> double[][] toModelMatrix(final DataFrame<V> df, double fillValue, DataFrame<Object> template) {
        return toModelMatrixDataFrame(df, template, false).fillna(fillValue).toArray(double[][].class);
    }

    public static <V> double[][] toModelMatrix(final DataFrame<V> df, double fillValue, DataFrame<Object> template, boolean addIntercept) {
        return toModelMatrixDataFrame(df, template, addIntercept).fillna(fillValue).toArray(double[][].class);
    }

    public static <V> DataFrame<Number> toModelMatrixDataFrame(final DataFrame<V> df) {
        return toModelMatrixDataFrame(df, null, false);
    }
    
    /**
     *  Encodes the DataFrame as a model matrix, converting nominal values 
     *  to dummy variables and optionally adds an intercept column
     *  
     * @param df Dataframe to be converted
     * @param template template DataFrame which has already been converted
     * @param addIntercept
     * @return a new DataFrame encoded as a model matrix
     */
    public static <V> DataFrame<Number> toModelMatrixDataFrame(final DataFrame<V> df, DataFrame<Object> template, boolean addIntercept) {
        DataFrame<Number> newDf = new DataFrame<>();

        if(addIntercept) {
            // Add an intercept column
            newDf.add("DFMMAddedIntercept");
            for (int i = 0; i < df.length(); i++) {
                newDf.append(Arrays.asList(1.0));
            }
        }

        final List<Object> columns = new ArrayList<>(df.columns());

        // Now convert Nominals (String columns) to dummy variables
        // Keep all others as is
        List<Class<?>> colTypes = df.types();
        for (int i = 0; i < df.size(); i++) {
            List<V> col = df.col(i);
            if(Number.class.isAssignableFrom(colTypes.get(i))) {
                List<Number> nums = new ArrayList<>();
                for (V num : col) {
                    nums.add((Number)num);
                }
                newDf.addCol(columns.get(i),nums);
            } else if (Date.class.isAssignableFrom(colTypes.get(i))) {
                List<Number> dates = new ArrayList<>();
                for (V date : col) {
                    dates.add(new Double(((Date)date).getTime()));
                }
                newDf.addCol(columns.get(i),dates);
            } else if (Boolean.class.isAssignableFrom(colTypes.get(i))) {
                List<Number> bools = new ArrayList<>();
                for (V tVal : col) {
                    bools.add((Boolean)tVal ? 1.0 : 0.0);
                }
                newDf.addCol(columns.get(i),bools);
            } else if (String.class.isAssignableFrom(colTypes.get(i))) {
                List<Object> extra = template != null ? template.col(i) : null;
                List<List<Number>> variable = variableToDummy(col, extra);
                int cnt = 0;
                for(List<Number> var : variable) {
                    String name = columns.get(i) + "-dummy" + cnt++;
                    newDf.addCol(name, var);
                }
            }
        }

        return newDf;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected static  <V> List<List<Number>> variableToDummy(List<V> col, List<Object> extra) {
        List<List<Number>> result = new ArrayList<List<Number>>();
        Set<V> factors = new TreeSet<>(col);
        if(extra!=null)
            factors.addAll(new TreeSet(extra));
        // Convert the variable to noFactors - 1
        Iterator<V> uniqueIter = factors.iterator();
        for (int u =0; u < factors.size()-1; u++) {
            V v = uniqueIter.next();
            List<Number> newDummy = new ArrayList<Number>();
            for (int i = 0; i < col.size(); i++) {
                if(col.get(i).equals(v)) {
                    newDummy.add(1.0);
                } else {
                    newDummy.add(0.0);
                }
            }
            result.add(newDummy);
        }
        return result;
    }


    public static <V> DataFrame<Boolean> isnull(final DataFrame<V> df) {
        return df.apply(new Function<V, Boolean>() {
                @Override
                public Boolean apply(final V value) {
                    return value == null;
                }
            });
    }

    public static <V> DataFrame<Boolean> notnull(final DataFrame<V> df) {
        return df.apply(new Function<V, Boolean>() {
                @Override
                public Boolean apply(final V value) {
                    return value != null;
                }
            });
    }

    private static class NAConversion<V>
    implements Function<V, V> {
    	final String naString; 
    	public NAConversion(String naString) {
    		this.naString = naString;
    	}
		@Override
		public V apply(V value) {
			return naString != null && String.valueOf(value).equals(naString) ? null : value;
		}	
    }
    
    private static final class StringConversion<V>
    implements Function<V, String> {
        @Override
        public String apply(final V value) {
            return String.valueOf(value);
        }
    }

    private static final class LongConversion<V>
    implements Function<V, Long> {
        @Override
        public Long apply(final V value) {
            try {
                return new Long(String.valueOf(value));
            } catch (final NumberFormatException ignored) { }
            return null;
        }
    }

    private static final class DoubleConversion<V>
    implements Function<V, Double> {
        @Override
        public Double apply(final V value) {
            try {
                return new Double(String.valueOf(value));
            } catch (final NumberFormatException ignored) { }
            return null;
        }
    }

    private static final class BooleanConversion<V>
    implements Function<V, Boolean> {
        @Override
        public Boolean apply(final V value) {
            final String str = String.valueOf(value);
            if (str.matches("t(r(u(e)?)?)?|y(e(s)?)?")) {
                return new Boolean(true);
            } else if (str.matches("f(a(l(s(e)?)?)?)?|n(o)?")) {
                return new Boolean(false);
            }
            return null;
        }
    }

    private static final class DateTimeConversion<V>
    implements Function<V, Date> {
        private final List<DateFormat> formats = Arrays.<DateFormat>asList(
                new SimpleDateFormat("y-M-d'T'HH:mm:ssXXX"),
                new SimpleDateFormat("y-M-d'T'HH:mm:ssZZZ"),
                new SimpleDateFormat("y-M-d"),
                new SimpleDateFormat("y-M-d hh:mm a"),
                new SimpleDateFormat("y-M-d HH:mm"),
                new SimpleDateFormat("y-M-d hh:mm:ss a"),
                new SimpleDateFormat("y-M-d HH:mm:ss"),
                new SimpleDateFormat("y/M/d hh:mm:ss a"),
                new SimpleDateFormat("y/M/d HH:mm:ss"),
                new SimpleDateFormat("y/M/d hh:mm a"),
                new SimpleDateFormat("y/M/d HH:mm"),
                new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSS a"),
                new SimpleDateFormat("dd-MMM-yy hh.mm.ss.SSSSSSSSS a"),
                new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy"),
                DateFormat.getDateTimeInstance(),
                new SimpleDateFormat("y/M/d"),
                new SimpleDateFormat("M/d/y hh:mm:ss a"),
                new SimpleDateFormat("M/d/y HH:mm:ss"),
                new SimpleDateFormat("M/d/y hh:mm a"),
                new SimpleDateFormat("M/d/y HH:mm"),
                new SimpleDateFormat("M/d/y"),
                DateFormat.getDateInstance()
            );

        @Override
        public Date apply(final V value) {
            final String source = String.valueOf(value);
            final ParsePosition pp = new ParsePosition(0);
            for (final DateFormat format : formats) {
                final Date dt = format.parse(source, pp);
                if (pp.getIndex() == source.length()) {
                    return dt;
                }
                pp.setIndex(0);
                pp.setErrorIndex(-1);
            }
            return null;
        }
    }
}
