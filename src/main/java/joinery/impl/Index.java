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

import java.util.*;

import joinery.DataFrame;
import joinery.DataFrame.RowFunction;


public class Index {
    private final Map<Object, Integer> index;

    public Index() {
        this(Collections.emptyList());
    }

    public Index(final Collection<?> names) {
        this(names, names.size());
    }

    public Index(final Collection<?> names, final int size) {
        index = new LinkedHashMap<>(names.size());
        final Iterator<?> it = names.iterator();
        for (int i = 0; i < size; i++) {
            final Object name = it.hasNext() ? it.next() : i;
            add(name, i);
        }
    }

    public Map<Integer, Object> getFields(){
        Map<Integer, Object> map = new HashMap<>();
        for(Object column: index.keySet()){
            map.put(index.get(column), column);
        }
        return map;
    }

    public void add(final Object name, final Integer value) {
        if (index.put(name, value) != null) {
            throw new IllegalArgumentException("duplicate name '" + name +  "' in index");
        }
    }

    public void extend(final Integer size) {
        for (int i = index.size(); i < size; i++) {
            add(i, i);
        }
    }

    public void set(final Object name, final Integer value) {
        index.put(name, value);
    }

    public Integer get(final Object name) {
        final Integer i = index.get(name);
        if (i == null) {
            throw new IllegalArgumentException("name '" + name + "' not in index");
        }
        return i;
    }

    public void rename(final Map<Object, Object> names) {
        final Map<Object, Integer> idx = new LinkedHashMap<>();
        for (final Map.Entry<Object, Integer> entry : index.entrySet()) {
            final Object col = entry.getKey();
            if (names.keySet().contains(col)) {
                idx.put(names.get(col), entry.getValue());
            } else {
                idx.put(col, entry.getValue());
            }
        }

        // clear and add all names back to preserve insertion order
        index.clear();
        index.putAll(idx);
    }

    public Set<Object> names() {
        return index.keySet();
    }

    public Integer[] indices(final Object[] names) {
        return indices(Arrays.asList(names));
    }

    public Integer[] indices(final List<Object> names) {
        final int size = names.size();
        final Integer[] indices = new Integer[size];
        for (int i = 0; i < size; i++) {
            indices[i] = get(names.get(i));
        }
        return indices;
    }

    public static <V> DataFrame<V> reindex(final DataFrame<V> df, final Integer ... cols) {
        return new DataFrame<V>(
                df.transform(
                    cols.length == 1 ?
                        new RowFunction<V, Object>() {
                            @Override
                            public List<List<Object>> apply(final List<V> values) {
                                return Collections.<List<Object>>singletonList(
                                    Collections.<Object>singletonList(
                                        values.get(cols[0])
                                    )
                                );
                            }
                        } :
                        new RowFunction<V, Object>() {
                            @Override
                            public List<List<Object>> apply(final List<V> values) {
                                final List<Object> key = new ArrayList<>(cols.length);
                                for (final int c : cols) {
                                    key.add(values.get(c));
                                }
                                return Collections.<List<Object>>singletonList(
                                    Collections.<Object>singletonList(
                                        Collections.unmodifiableList(key)
                                    )
                                );
                            }
                        }
                ).col(0),
                df.columns(),
                new Views.ListView<V>(df, false)
            );
    }

    public static <V> DataFrame<V> reset(final DataFrame<V> df) {
        final List<Object> index = new ArrayList<>(df.length());
        for (int i = 0; i < df.length(); i++) {
            index.add(i);
        }

        return new DataFrame<V>(
                index,
                df.columns(),
                new Views.ListView<V>(df, false)
            );
    }
}
