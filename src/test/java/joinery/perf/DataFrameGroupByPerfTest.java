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
        for (int i = 0; i < 10; i++) {
            System.out.printf("grouping %,d rows by category\n", df.length());
            final DataFrame<Object> grouped = df.groupBy("category");
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
