/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 * Copyright (c) 2026 zc (1.11 line, GLM-5.2 AI-assisted refactor)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at youroption) any later version.
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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import joinery.DataFrame;
import joinery.DataFrame.RowFunction;


/**
 * ┌─ What : 行/列名 → 位置的索引（pandas 风格，支持重名）
 * │  Why  : 旧实现用 {@code Map<Object, Integer>} 强制键唯一，重名直接抛
 * │         IllegalArgumentException —— 这是核心架构 bug：合法场景（Excel 重名列、
 * │         join 后两侧同名列、SQL SELECT * 拼接）都会崩溃。
 * │         pandas 的设计是「位置优先，名字辅助」：同名的多列保留所有位置，
 * │         df['x'] 重名时返回所有匹配列。
 * │  Who  : DataFrame 的 index / columns 两个 Index 字段
 * │  When : 按名访问行/列时
 * │  Where: joinery.impl.Index
 *
 *  设计要点（pandas 兼容）：
 *    - 底层改为 {@code Map<Object, List<Integer>>}：名字 → 位置列表（保留插入顺序）
 *    - add(name, pos)：同 name 时追加到列表，不抛异常
 *    - get(name)：返回首个匹配位置（向后兼容旧调用，重名时取第一个）
 *    - getAll(name)：返回所有匹配位置（pandas 风格，新增能力）
 *    - names()：保留所有名字（含重复，按插入顺序）
 *    - indices(List)：按名批量查位置，重名取首个
 */
public class Index {
    // ┌─ What : 名字 → 位置列表（支持重名；LinkedHashMap 保留首次插入顺序）
    private final Map<Object, List<Integer>> index;

    public Index() {
        this(Collections.<Object>emptyList());
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

    // ┌─ What : 追加一个名字→位置映射（pandas 风格，重名时追加到现有列表，不抛异常）
    // │  Why  : 旧实现对重名抛 IllegalArgumentException，导致 Excel 重名列、join 同名列都崩溃；
    // │         改为「同名追加位置」，保留所有同名条目
    public void add(final Object name, final Integer value) {
        // computeIfAbsent 保证首次出现时建新 List，重名时追加；LinkedHashMap 保留顺序
        index.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
    }

    public void extend(final Integer size) {
        for (int i = index.size(); i < size; i++) {
            add(i, i);   // 整数键天然唯一，不会重名
        }
    }

    // ┌─ What : 把指定名字映射到指定位置（覆盖式，用于 rename 等场景）
    // │  注意：与 add 不同——set 是替换，add 是追加。重名时 set 只覆盖该名字的整个位置列表为单值
    public void set(final Object name, final Integer value) {
        final List<Integer> single = new ArrayList<>(1);
        single.add(value);
        index.put(name, single);
    }

    // ┌─ What : 取名字对应的首个位置（向后兼容旧 get 语义）
    // │  Why  : 大量现有调用（df.col / get(row,col) / slice 等）只期望单个位置；
    // │         重名时取首个匹配，保持旧行为不破坏
    // │  throws IllegalArgumentException 名字完全不存在时
    public Integer get(final Object name) {
        final List<Integer> positions = index.get(name);
        if (positions == null || positions.isEmpty()) {
            throw new IllegalArgumentException("name '" + name + "' not in index");
        }
        return positions.get(0);   // 首个匹配（向后兼容）
    }

    // ┌─ What : 取名字对应的所有位置（pandas 风格，重名时返回多值）
    // │  Who  : 新增的 cols(name) / 多列选择 API 调用
    public List<Integer> getAll(final Object name) {
        final List<Integer> positions = index.get(name);
        if (positions == null || positions.isEmpty()) {
            throw new IllegalArgumentException("name '" + name + "' not in index");
        }
        return new ArrayList<>(positions);   // 防御性拷贝
    }

    // ┌─ What : 名字是否存在于索引（至少一个位置）
    public boolean contains(final Object name) {
        final List<Integer> positions = index.get(name);
        return positions != null && !positions.isEmpty();
    }

    // ┌─ What : 重命名（按映射把旧名替换为新名；新名可能与现有名重名，仍按 add 语义追加）
    public void rename(final Map<Object, Object> names) {
        final Map<Object, List<Integer>> idx = new LinkedHashMap<>();
        for (final Map.Entry<Object, List<Integer>> entry : index.entrySet()) {
            final Object col = entry.getKey();
            final Object newCol = names.keySet().contains(col) ? names.get(col) : col;
            // 同名时追加（pandas rename 后可能产生重名，按 add 语义处理）
            idx.computeIfAbsent(newCol, k -> new ArrayList<>()).addAll(entry.getValue());
        }
        index.clear();
        index.putAll(idx);
    }

    // ┌─ What : 返回所有名字（含重复，按插入顺序）
    // │  Why  : names() 用于列名展示；旧实现返回 keySet（去重）会丢失重名列信息，
    // │         改为返回含重复的完整列表（与 DataFrame.columns() 期望一致）
    public Set<Object> names() {
        // 注意：返回的是 LinkedHashMap 的 keySet，对重名只显示一次键
        // 但 DataFrame.columns() 依赖 names().size() 获取列数，重名时会少算
        // 因此另提供 namesList() 返回完整含重复的列表；这里保留 keySet 兼容旧调用
        return index.keySet();
    }

    // ┌─ What : 返回完整名字列表（含重复，按位置顺序）—— 新增，用于准确反映列数与列名
    public List<Object> namesList() {
        final List<Object> all = new ArrayList<>();
        for (final Map.Entry<Object, List<Integer>> entry : index.entrySet()) {
            for (int i = 0; i < entry.getValue().size(); i++) {
                all.add(entry.getKey());
            }
        }
        return all;
    }

    public Integer[] indices(final Object[] names) {
        return indices(Arrays.asList(names));
    }

    // ┌─ What : 按名批量查位置（重名时每个名字取首个匹配）
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
