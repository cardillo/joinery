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

package joinery.perf;

import java.util.ArrayList;
import java.util.List;

import joinery.DataFrame;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class DataFrameGroupByPerfTest {
    @After
    public void report()
    throws Exception {
        PerformanceTestUtils.displayMetricsIfAvailable();
    }

    @Test
    @Category(PerformanceTests.class)
    public void test() {
        final DataFrame<Object> df = PerformanceTestUtils.randomData(0.75);
        final List<String> columns = new ArrayList<>(df.columns());
        for (int i = 0; i < 10; i++) {
            // group by value or category
            // (name is pathological, results in 1 group per row)
            final int col = 1 + i % (columns.size() - 1);
            final String key = columns.get(col);
            System.out.printf("grouping %,d rows by %s\n", df.length(), key);
            final DataFrame<Object> grouped = df.groupBy(key);
            grouped.count();
            grouped.sum();
            grouped.min();
            grouped.max();
            grouped.median();
            grouped.skew();
            grouped.kurt();
            grouped.stddev();
            grouped.var();
        }
    }
}
