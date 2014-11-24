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
}
