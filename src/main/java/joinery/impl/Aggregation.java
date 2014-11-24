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

public class Aggregation {
    public static class Count<V>
    implements Aggregate<V, V> {
        @Override
        @SuppressWarnings("unchecked")
        public V apply(final List<V> values) {
            return (V)new Integer(values.size());
        }
    }

    public static class Sum<V>
    implements Aggregate<V, V> {
        @Override
        @SuppressWarnings("unchecked")
        public V apply(final List<V> values) {
            Double sum = new Double(0);
            for (final V value : values) {
                sum += Number.class.cast(value).doubleValue();
            }
            return (V)sum;
        }
    }

    public static class Product<V>
    implements Aggregate<V, V> {
        @Override
        @SuppressWarnings("unchecked")
        public V apply(final List<V> values) {
            Double product = new Double(1);
            for (final V value : values) {
                product *= Number.class.cast(value).doubleValue();
            }
            return (V)product;
        }
    }
}
