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
import java.util.LinkedList;
import java.util.List;

import joinery.DataFrame;
import joinery.DataFrame.Function;

public class Timeseries {
    public static <V> DataFrame<V> rollapply(final DataFrame<V> df, final Function<List<V>, V> function, final int period) {
        // can't use apply because rolling window functions are likely path dependent
        final List<List<V>> data = new ArrayList<>(df.size());
        final WindowFunction<V> f = new WindowFunction<>(function, period);
        for (int c = 0; c < df.size(); c++) {
            final List<V> column = new ArrayList<>(df.length());
            for (int r = 0; r < df.length(); r++) {
                column.add(f.apply(df.get(r, c)));
            }
            data.add(column);
            f.reset();
        }
        return new DataFrame<>(df.index(), df.columns(), data);
    }

    private static class WindowFunction<V>
    implements Function<V, V> {
        private final Function<List<V>, V> function;
        private final int period;
        protected final LinkedList<V> window;

        private WindowFunction(final Function<List<V>, V> function, final int period) {
            this.function = function;
            this.period = period;
            this.window = new LinkedList<>();
        }

        @Override
        public V apply(final V value) {
            while (window.size() < period) {
                window.add(null);
            }
            window.add(value);
            final V result = function.apply(window);
            window.remove();
            return result;
        }

        public void reset() {
            window.clear();
        }
    }

    private static class DiscreteDifferenceFunction
    implements Function<List<Number>, Number> {
        @Override
        public Number apply(final List<Number> values) {
            if (values.contains(null)) {
                return null;
            }
            return values.get(values.size() - 1).doubleValue()
                 - values.get(0).doubleValue();
        }
    }

    private static class PercentChangeFunction
    implements Function<List<Number>, Number> {
        @Override
        public Number apply(final List<Number> values) {
            if (values.contains(null)) {
                return null;
            }
            final double x1 = values.get(0).doubleValue();
            final double x2 = values.get(values.size() - 1).doubleValue();
            return  (x2 - x1) / x1;
        }
    }

    public static <V> DataFrame<V> diff(final DataFrame<V> df, final int period) {
        final DataFrame<V> nonnumeric = df.nonnumeric();
        @SuppressWarnings("unchecked")
        final DataFrame<V> diff = (DataFrame<V>)df.numeric().apply(
                new WindowFunction<Number>(new DiscreteDifferenceFunction(), period));
        return nonnumeric.isEmpty() ? diff : nonnumeric.join(diff);
    }

    public static <V> DataFrame<V> percentChange(final DataFrame<V> df, final int period) {
        final DataFrame<V> nonnumeric = df.nonnumeric();
        @SuppressWarnings("unchecked")
        final DataFrame<V> diff = (DataFrame<V>)df.numeric().apply(
                new WindowFunction<Number>(new PercentChangeFunction(), period));
        return nonnumeric.isEmpty() ? diff : nonnumeric.join(diff);
    }
}
