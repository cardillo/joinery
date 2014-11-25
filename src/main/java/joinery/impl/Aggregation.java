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

import java.util.List;

import joinery.DataFrame.Aggregate;

import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.UnivariateStatistic;

public class Aggregation {
    public static class Count<V>
    implements Aggregate<V, V> {
        @Override
        @SuppressWarnings("unchecked")
        public V apply(final List<V> values) {
            return (V)new Integer(values.size());
        }
    }

    private static abstract class AbstractStorelessStatistic<V>
    implements Aggregate<V, Double> {
        protected final StorelessUnivariateStatistic stat;

        protected AbstractStorelessStatistic(final StorelessUnivariateStatistic stat) {
            this.stat = stat;
        }

        @Override
        public Double apply(final List<V> values) {
            stat.clear();
            for (final Object value : values) {
                stat.increment(Number.class.cast(value).doubleValue());
            }
            return stat.getResult();
        }
    }

    public static class Sum<V>
    extends AbstractStorelessStatistic<V> {
        public Sum() {
            super(new org.apache.commons.math3.stat.descriptive.summary.Sum());
        }
    }

    public static class Product<V>
    extends AbstractStorelessStatistic<V> {
        public Product() {
            super(new org.apache.commons.math3.stat.descriptive.summary.Product());
        }
    }

    public static class Mean<V>
    extends AbstractStorelessStatistic<V> {
        public Mean() {
            super(new org.apache.commons.math3.stat.descriptive.moment.Mean());
        }
    }

    public static class StdDev<V>
    extends AbstractStorelessStatistic<V> {
        public StdDev() {
            super(new org.apache.commons.math3.stat.descriptive.moment.StandardDeviation());
        }
    }

    public static class Variance<V>
    extends AbstractStorelessStatistic<V> {
        public Variance() {
            super(new org.apache.commons.math3.stat.descriptive.moment.Variance());
        }
    }

    public static class Skew<V>
    extends AbstractStorelessStatistic<V> {
        public Skew() {
            super(new org.apache.commons.math3.stat.descriptive.moment.Skewness());
        }
    }

    public static class Kurtosis<V>
    extends AbstractStorelessStatistic<V> {
        public Kurtosis() {
            super(new org.apache.commons.math3.stat.descriptive.moment.Kurtosis());
        }
    }

    public static class Min<V>
    extends AbstractStorelessStatistic<V> {
        public Min() {
            super(new org.apache.commons.math3.stat.descriptive.rank.Min());
        }
    }

    public static class Max<V>
    extends AbstractStorelessStatistic<V> {
        public Max() {
            super(new org.apache.commons.math3.stat.descriptive.rank.Max());
        }
    }

    private static abstract class AbstractStatistic<V>
    implements Aggregate<V, Double> {
        protected final UnivariateStatistic stat;

        protected AbstractStatistic(final UnivariateStatistic stat) {
            this.stat = stat;
        }

        @Override
        public Double apply(final List<V> values) {
            final double[] vals = new double[values.size()];
            for (int i = 0; i < vals.length; i++) {
                vals[i] = Number.class.cast(values.get(i)).doubleValue();
            }
            return stat.evaluate(vals);
        }
    }

    public static class Median<V>
    extends AbstractStatistic<V> {
        public Median() {
            super(new org.apache.commons.math3.stat.descriptive.rank.Median());
        }
    }
}
