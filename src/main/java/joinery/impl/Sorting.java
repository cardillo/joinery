/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 * Copyright (c) 2026 zc (1.11 line, GLM-5.2 AI-assisted refactor)
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import joinery.DataFrame;
import joinery.DataFrame.SortDirection;

public class Sorting {
    public static <V> DataFrame<V> sort(
            final DataFrame<V> df, final Map<Integer, SortDirection> cols) {
        final Comparator<List<V>> comparator = new Comparator<List<V>>() {
            @Override
            @SuppressWarnings("unchecked")
            public int compare(final List<V> r1, final List<V> r2) {
                int result = 0;
                for (final Map.Entry<Integer, SortDirection> col : cols.entrySet()) {
                    final int c = col.getKey();
                    final Comparable<V> v1 = Comparable.class.cast(r1.get(c));
                    final V v2 = r2.get(c);
                    result = v1.compareTo(v2);
                    result *= col.getValue() == SortDirection.DESCENDING ? -1 : 1;
                    if (result != 0) {
                        break;
                    }
                }
                return result;
            }
        };
        return sort(df, comparator);
    }

    public static <V> DataFrame<V> sort(
            final DataFrame<V> df, final Comparator<List<V>> comparator) {
        final DataFrame<V> sorted = new DataFrame<V>(df.columns());
        final Comparator<Integer> cmp = new Comparator<Integer>() {
            @Override
            public int compare(final Integer r1, final Integer r2) {
                return comparator.compare(df.row(r1),  df.row(r2));
            }
        };

        final Integer[] rows = new Integer[df.length()];
        for (int r = 0; r < df.length(); r++) {
            rows[r] = r;
        }
        Arrays.sort(rows, cmp);

        final List<Object> labels = new ArrayList<>(df.index());
        for (final Integer r : rows) {
            final Object label = r < labels.size() ? labels.get(r) : r;
            sorted.append(label, df.row(r));
        }

        // ┌─ What : 修复「sortBy 丢失 groupBy 状态」bug（2026-08）
        // │  Why  : 原实现 sortBy 返回全新 DataFrame，groups 字段被重置为空 Grouping，
        // │         导致 df.groupBy("x").sortBy("y").mean() 链式调用时 mean 退化成整列平均
        // │         （聚合结果静默错算，是严重的数据正确性事故）。
        // │         pandas 用户最自然的链式写法 `df.groupby('x').sort_values('y').mean()`
        // │         必须在 joinery 里也工作。sortBy 只应改顺序，绝不能丢分组状态。
        // │  Who  : 所有 sortBy 链式调用后还要聚合的场景
        // │  When : sortBy 返回时
        // │  Where: Sorting.sort
        // │  How  : 若原 df 已 groupBy（groups 包含分组列），在 sorted 上用相同列重建 Grouping。
        // │         因 sorted 的行是原 df 行的重排，新的分组行号变了，必须用列值重新 groupBy。
        if (!df.groups().columns().isEmpty()) {
            // 必须用 Integer[]（varargs Integer... 重载），int[] 会被当成 Object 当成单个 key
            final Integer[] groupCols = df.groups().columns().toArray(new Integer[0]);
            // 在已排序的 sorted 上按相同分组列重新分组（值相同即同组，与排序无关）
            return sorted.groupBy(groupCols);
        }

        return sorted;
    }
}
