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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import joinery.LocalDataFrame;
import org.apache.commons.math3.stat.correlation.StorelessCovariance;
import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.commons.math3.stat.descriptive.StorelessUnivariateStatistic;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.descriptive.UnivariateStatistic;

import joinery.DataFrame;
import joinery.DataFrame.Aggregate;

public class Aggregation {
    public static class Count<V>
    implements Aggregate<V, Number> {
        @Override
        public Number apply(final List<V> values) {
            return new Integer(values.size());
        }
    }

    public static class Unique<V>
    implements Aggregate<V, V> {
        @Override
        public V apply(final List<V> values) {
            final Set<V> unique = new HashSet<>(values);
            if (unique.size() > 1) {
                throw new IllegalArgumentException("values not unique: " + unique);
            }
            return values.get(0);
        }
    }

    public static class Collapse<V>
    implements Aggregate<V, String> {
        private final String delimiter;

        public Collapse() {
            this(",");
        }

        public Collapse(final String delimiter) {
            this.delimiter = delimiter;
        }

        @Override
        public String apply(final List<V> values) {
            final Set<V> seen = new HashSet<>();
            final StringBuilder sb = new StringBuilder();
            for (final V value : values) {
                if (!seen.contains(value)) {
                    if (sb.length() > 0) {
                        sb.append(delimiter);
                    }
                    sb.append(String.valueOf(value));
                    seen.add(value);
                }
            }
            return sb.toString();
        }
    }

    private static abstract class AbstractStorelessStatistic<V>
    implements Aggregate<V, Number> {
        protected final StorelessUnivariateStatistic stat;

        protected AbstractStorelessStatistic(final StorelessUnivariateStatistic stat) {
            this.stat = stat;
        }

        @Override
        public Number apply(final List<V> values) {
            stat.clear();
            for (Object value : values) {
                if (value != null) {
                    if (value instanceof Boolean) {
                        value = Boolean.class.cast(value) ? 1 : 0;
                    }
                    stat.increment(Number.class.cast(value).doubleValue());
                }
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
    implements Aggregate<V, Number> {
        protected final UnivariateStatistic stat;

        protected AbstractStatistic(final UnivariateStatistic stat) {
            this.stat = stat;
        }

        @Override
        public Number apply(final List<V> values) {
            int count = 0;
            final double[] vals = new double[values.size()];
            for (int i = 0; i < vals.length; i++) {
                final V val = values.get(i);
                if (val != null) {
                    vals[count++] = Number.class.cast(val).doubleValue();
                }
            }
            return stat.evaluate(vals, 0, count);
        }
    }

    public static class Median<V>
    extends AbstractStatistic<V> {
        public Median() {
            super(new org.apache.commons.math3.stat.descriptive.rank.Median());
        }
    }

    public static class Percentile<V>
    extends AbstractStatistic<V> {
        public Percentile(final double quantile) {
            super(new org.apache.commons.math3.stat.descriptive.rank.Percentile(quantile));
        }
    }

    public static class Describe<V>
    implements Aggregate<V, StatisticalSummary> {
        private final SummaryStatistics stat = new SummaryStatistics();

        @Override
        public StatisticalSummary apply(final List<V> values) {
            stat.clear();
            for (Object value : values) {
                if (value != null) {
                    if (value instanceof Boolean) {
                        value = Boolean.class.cast(value) ? 1 : 0;
                    }
                    stat.addValue(Number.class.cast(value).doubleValue());
                }
            }
            return stat.getSummary();
        }
    }

    private static final Object name(final DataFrame<?> df, final Object row, final Object stat) {
        // df index size > 1 only happens if the aggregate describes a grouped data frame
        return df.index().size() > 1 ? Arrays.asList(row, stat) : stat;
    }

    @SuppressWarnings("unchecked")
    public static <V> DataFrame<V> describe(final DataFrame<V> df) {
        final DataFrame<V> desc = new LocalDataFrame<>();
        for (final Object col : df.columns()) {
            for (final Object row : df.index()) {
                final V value = df.get(row, col);
                if (value instanceof StatisticalSummary) {
                    if (!desc.columns().contains(col)) {
                        desc.add(col);
                        if (desc.isEmpty()) {
                            for (final Object r : df.index()) {
                                for (final Object stat : Arrays.asList("count", "mean", "std", "var", "max", "min")) {
                                    final Object name = name(df, r, stat);
                                    desc.append(name, Collections.<V>emptyList());
                                }
                            }
                        }
                    }

                    final StatisticalSummary summary = StatisticalSummary.class.cast(value);
                    desc.set(name(df, row, "count"), col, (V)new Double(summary.getN()));
                    desc.set(name(df, row, "mean"),  col, (V)new Double(summary.getMean()));
                    desc.set(name(df, row, "std"),   col, (V)new Double(summary.getStandardDeviation()));
                    desc.set(name(df, row, "var"),   col, (V)new Double(summary.getVariance()));
                    desc.set(name(df, row, "max"),   col, (V)new Double(summary.getMax()));
                    desc.set(name(df, row, "min"),   col, (V)new Double(summary.getMin()));
                }
            }
        }
        return desc;
    }

    public static <V> DataFrame<Number> cov(final DataFrame<V> df) {
        DataFrame<Number> num = df.numeric();
        StorelessCovariance cov = new StorelessCovariance(num.size());

        // row-wise copy to double array and increment
        double[] data = new double[num.size()];
        for (List<Number> row : num) {
            for (int i = 0; i < row.size(); i++) {
                data[i] = row.get(i).doubleValue();
            }
            cov.increment(data);
        }

        // row-wise copy results into new data frame
        double[][] result = cov.getData();
        DataFrame<Number> r = new LocalDataFrame<>(num.columns());
        List<Number> row = new ArrayList<>(num.size());
        for (int i = 0; i < result.length; i++) {
            row.clear();
            for (int j = 0; j < result[i].length; j++) {
                row.add(result[i][j]);
            }
            r.append(row);
        }

        return r;
    }
}
