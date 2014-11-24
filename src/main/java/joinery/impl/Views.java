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
import java.util.List;

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

    public static class TransformedView<V, U>
    extends AbstractList<List<U>> {
        private final DataFrame<V> df;
        private final Function<V, U> transform;
        private final boolean transpose;

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
        private final DataFrame<V> df;
        private final int index;
        private final boolean transpose;
        private final Function<V, U> transform;

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
            return df.get(index / df.size(), index % df.size());
        }

        @Override
        public int size() {
            return df.size() * df.length();
        }
    }
}
