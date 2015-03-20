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

import joinery.DataFrame.Function;

import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;

public class Transforms {
    public interface CumulativeFunction<I, O>
    extends Function<I, O> {
        void reset();
    }

    private static class AbstractCumulativeFunction<V>
    implements CumulativeFunction<Number, Number> {
        private final StorelessUnivariateStatistic stat;
        private final Number initialValue;

        protected AbstractCumulativeFunction(final StorelessUnivariateStatistic stat, final Number initialValue) {
            this.stat = stat;
            this.initialValue = initialValue;
            reset();
        }

        @Override
        public Number apply(final Number value) {
            stat.increment(value.doubleValue());
            return stat.getResult();
        }

        @Override
        public void reset() {
            stat.clear();
            stat.increment(initialValue.doubleValue());
        }
    }

    public static class CumulativeSum<V>
    extends AbstractCumulativeFunction<V> {
        public CumulativeSum() {
            super(new org.apache.commons.math3.stat.descriptive.summary.Sum(), 0);
        }
    }

    public static class CumulativeProduct<V>
    extends AbstractCumulativeFunction<V> {
        public CumulativeProduct() {
            super(new org.apache.commons.math3.stat.descriptive.summary.Product(), 1);
        }
    }

    public static class CumulativeMin<V>
    extends AbstractCumulativeFunction<V> {
        public CumulativeMin() {
            super(new org.apache.commons.math3.stat.descriptive.rank.Min(), Double.MAX_VALUE);
        }
    }

    public static class CumulativeMax<V>
    extends AbstractCumulativeFunction<V> {
        public CumulativeMax() {
            super(new org.apache.commons.math3.stat.descriptive.rank.Max(), Double.MIN_VALUE);
        }
    }
}
