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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import joinery.DataFrame;
import joinery.DataFrame.Function;
import joinery.DataFrame.NumberDefault;

public class Conversion {
    public static <V> void convert(final DataFrame<V> df) {
        convert(df, NumberDefault.LONG_DEFAULT);
    }

    public static <V> void convert(final DataFrame<V> df, final NumberDefault numDefault) {
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

        // find conversions
        for (int c = 0; c < cols; c++) {
            for (final Function<V, ?> conv : converters) {
                boolean all = true;
                for (int r = 0; r < rows; r++) {
                    if (conv.apply(df.get(r, c)) == null) {
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
        convert(df, conversions);
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
        convert(df, conversions);
    }

    @SuppressWarnings("unchecked")
    public static <V> void convert(final DataFrame<V> df, final Map<Integer, Function<V, ?>> conversions) {
        final int rows = df.length();
        final int cols = df.size();
        for (int c = 0; c < cols; c++) {
            final Function<V, ?> conv = conversions.get(c);
            if (conv != null) {
                for (int r = 0; r < rows; r++) {
                    df.set(r, c, (V)conv.apply(df.get(r, c)));
                }
            }
        }
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
                new SimpleDateFormat("y/M/d"),
                new SimpleDateFormat("M/d/y hh:mm:ss a"),
                new SimpleDateFormat("M/d/y HH:mm:ss"),
                new SimpleDateFormat("M/d/y hh:mm a"),
                new SimpleDateFormat("M/d/y HH:mm"),
                new SimpleDateFormat("M/d/y")
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
