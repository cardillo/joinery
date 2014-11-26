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

public class DataFrameAppendPerfTest {
    @After
    public void report()
    throws Exception {
        PerformanceTestUtils.displayMetricsIfAvailable();
    }

    @Test
    @Category(PerformanceTests.class)
    public void test() {
        final DataFrame<Object> df = PerformanceTestUtils.randomData(0);
        while(PerformanceTestUtils.memoryUtilization() < 0.95) {
            df.append(PerformanceTestUtils.randomRow());
            if (df.length() % PerformanceTestUtils.MILLIONS == 0) {
                System.out.printf("added %dm rows (memory utilization %4.2f%%)\n",
                        df.length() / PerformanceTestUtils.MILLIONS, PerformanceTestUtils.memoryUtilization() * 100);
            }
        }
        System.out.printf("created %,d row data set (memory utilization %4.2f%%)\n",
                df.length(), PerformanceTestUtils.memoryUtilization() * 100);
    }
}
