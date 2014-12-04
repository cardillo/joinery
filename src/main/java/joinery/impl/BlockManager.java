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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class BlockManager<V> {
    private final List<List<V>> blocks;

    public BlockManager() {
        this(Collections.<List<V>>emptyList());
    }

    public BlockManager(final Collection<? extends Collection<V>> data) {
        blocks = new LinkedList<>();
        for (final Collection<V> col : data) {
            add(new ArrayList<>(col));
        }
    }

    public void reshape(final int cols, final int rows) {
        for (int c = blocks.size(); c < cols; c++) {
            add(new ArrayList<V>(rows));
        }

        for (final List<V> block : blocks) {
            for (int r = block.size(); r < rows; r++) {
                block.add(null);
            }
        }
    }

    public V get(final int col, final int row) {
        return blocks.get(col).get(row);
    }

    public void set(final V value, final int col, final int row) {
        blocks.get(col).set(row, value);
    }

    public void add(final List<V> col) {
        final int len = length();
        for (int r = col.size(); r < len; r++) {
            col.add(null);
        }
        blocks.add(col);
    }

    public int size() {
        return blocks.size();
    }

    public int length() {
        return blocks.isEmpty() ? 0 : blocks.get(0).size();
    }
}

