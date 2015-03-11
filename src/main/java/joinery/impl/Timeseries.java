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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import joinery.DataFrame;
import joinery.DataFrame.RowFunction;

public class Timeseries {
    @SuppressWarnings("unchecked")
    public static <V> DataFrame<V> diff(final DataFrame<V> df, final int period) {
        final DataFrame<V> nonnumeric = df.nonnumeric();
        final DataFrame<V> diff = (DataFrame<V>)
                df.numeric().transform(new DiscreteDifferenceTransform(period));
        return nonnumeric.isEmpty() ? diff : nonnumeric.join(diff);
    }

    @SuppressWarnings("unchecked")
    public static <V> DataFrame<V> percentChange(final DataFrame<V> df, final int period) {
        final DataFrame<V> nonnumeric = df.nonnumeric();
        final DataFrame<V> diff = (DataFrame<V>)
                df.numeric() .transform(new PercentChangeTransform(period));
        return nonnumeric.isEmpty() ? diff : nonnumeric.join(diff);
    }

    private static abstract class WindowTransform<V>
    implements RowFunction<V, V> {
        protected final int period;
        protected final Queue<List<V>> window;

        protected WindowTransform(final int period) {
            if (period < 1) {
                throw new IllegalArgumentException("period must be a positive integer");
            }
            this.period = period;
            this.window = new LinkedList<>();
        }

        @Override
        public List<List<V>> apply(final List<V> values) {
            final List<V> row = new ArrayList<>(values);
            if (window.size() < period) {
                for (int i = 0; i < values.size(); i++) {
                    row.set(i, null);
                }
            } else {
                compute(window, row);
                window.remove();
            }
            window.add(values);
            return Collections.singletonList(row);
        }

        protected abstract void compute(Queue<List<V>> window, List<V> values);
    }

    private static class DiscreteDifferenceTransform
    extends WindowTransform<Number> {
        public DiscreteDifferenceTransform(final int period) {
            super(period);
        }

        @Override
        protected void compute(final Queue<List<Number>> window, final List<Number> values) {
            final List<Number> last = window.peek();
            for (int i = 0; i < values.size(); i++) {
                final Double diff = values.get(i).doubleValue()
                                    - last.get(i).doubleValue();
                values.set(i, diff);
            }
        }
    }

    private static class PercentChangeTransform
    extends WindowTransform<Number> {
        public PercentChangeTransform(final int period) {
            super(period);
        }

        @Override
        protected void compute(final Queue<List<Number>> window, final List<Number> values) {
            final List<Number> last = window.peek();
            for (int i = 0; i < values.size(); i++) {
                final double x1 = last.get(i).doubleValue();
                final double x2 = values.get(i).doubleValue();
                values.set(i, (x2 - x1) / x1);
            }
        }
    }
}
