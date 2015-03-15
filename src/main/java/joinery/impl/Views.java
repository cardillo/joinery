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

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joinery.DataFrame;
import joinery.DataFrame.Function;

public class Views {

    public static class ListView<V>
    extends AbstractList<List<V>> {
        private final DataFrame<V> df;
        private final boolean transpose;

        public ListView(final DataFrame<V> df, final boolean transpose) {
            this.df = df;
            this.transpose = transpose;
        }

        @Override
        public List<V> get(final int index) {
            return new SeriesListView<>(df, index, !transpose);
        }

        @Override
        public int size() {
            return transpose ? df.length() : df.size();
        }
    }

    public static class SeriesListView<V>
    extends AbstractList<V> {
        private final DataFrame<V> df;
        private final int index;
        private final boolean transpose;

        public SeriesListView(final DataFrame<V> df, final int index, final boolean transpose) {
            this.df = df;
            this.index = index;
            this.transpose = transpose;
        }

        @Override
        public V get(final int index) {
            return transpose ? df.get(index, this.index) : df.get(this.index, index);
        }

        @Override
        public int size() {
            return transpose ? df.length() : df.size();
        }
    }

    public static class MapView<V>
    extends AbstractList<Map<Object, V>> {
        private final DataFrame<V> df;
        private final boolean transpose;

        public MapView(final DataFrame<V> df, final boolean transpose) {
            this.df = df;
            this.transpose = transpose;
        }

        @Override
        public Map<Object, V> get(final int index) {
            return new SeriesMapView<>(df, index, !transpose);
        }

        @Override
        public int size() {
            return transpose ? df.length() : df.size();
        }
    }

    public static class SeriesMapView<V>
    extends AbstractMap<Object, V> {
        private final DataFrame<V> df;
        private final int index;
        private final boolean transpose;

        public SeriesMapView(final DataFrame<V> df, final int index, final boolean transpose) {
            this.df = df;
            this.index = index;
            this.transpose = transpose;
        }

        @Override
        public Set<Map.Entry<Object, V>> entrySet() {
            return new AbstractSet<Map.Entry<Object, V>>() {
                @Override
                public Iterator<Map.Entry<Object, V>> iterator() {
                    final Set<Object> names = transpose ? df.index() : df.columns();
                    final Iterator<Object> it = names.iterator();

                    return new Iterator<Map.Entry<Object, V>>() {
                        int value = 0;

                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Map.Entry<Object, V> next() {
                            final Object key = it.next();
                            final int value = this.value++;
                            return new Map.Entry<Object, V>() {
                                @Override
                                public Object getKey() {
                                    return key;
                                }

                                @Override
                                public V getValue() {
                                    return transpose ?
                                            df.get(value, index) :
                                            df.get(index, value);
                                }

                                @Override
                                public V setValue(final V value) {
                                    throw new UnsupportedOperationException();
                                }
                            };
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override
                public int size() {
                    return transpose ? df.length() : df.size();
                }
            };
        }
    }

    public static class TransformedView<V, U>
    extends AbstractList<List<U>> {
        protected final DataFrame<V> df;
        protected final Function<V, U> transform;
        protected final boolean transpose;

        public TransformedView(final DataFrame<V> df, final Function<V, U> transform, final boolean transpose) {
            this.df = df;
            this.transform = transform;
            this.transpose = transpose;
        }

        @Override
        public List<U> get(final int index) {
            return new TransformedSeriesView<>(df, transform, index, !transpose);
        }

        @Override
        public int size() {
            return transpose ? df.length() : df.size();
        }
    }

    public static class TransformedSeriesView<V, U>
    extends AbstractList<U> {
        protected final DataFrame<V> df;
        protected final int index;
        protected final boolean transpose;
        protected final Function<V, U> transform;

        public TransformedSeriesView(final DataFrame<V> df, final Function<V, U> transform, final int index, final boolean transpose) {
            this.df = df;
            this.transform = transform;
            this.index = index;
            this.transpose = transpose;
        }

        @Override
        public U get(final int index) {
            final V value = transpose ? df.get(index, this.index) : df.get(this.index, index);
            return transform.apply(value);
        }

        @Override
        public int size() {
            return transpose ? df.length() : df.size();
        }
    }

    public static class FlatView<V>
    extends AbstractList<V> {
        private final DataFrame<V> df;

        public FlatView(final DataFrame<V> df) {
            this.df = df;
        }

        @Override
        public V get(final int index) {
            return df.get(index % df.length(), index / df.length());
        }

        @Override
        public int size() {
            return df.size() * df.length();
        }
    }

    public static class FillNaFunction<V>
    implements Function<V, V> {
        private final V fill;

        public FillNaFunction(final V fill) {
            this.fill = fill;
        }

        @Override
        public V apply(final V value) {
            return value == null ? fill : value;
        }
    }
}
