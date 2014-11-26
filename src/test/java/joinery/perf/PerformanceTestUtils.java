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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import joinery.DataFrame;

public class PerformanceTestUtils {
    public static final int MILLIONS = 1_000_000;
    private static Runtime runtime = Runtime.getRuntime();
    private static Random random = new Random();
    private static String[] categories = new String[] {
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot"
        };

    private PerformanceTestUtils() { }

    public static DataFrame<Object> randomData(final int rows) {
        final DataFrame<Object> df = new DataFrame<Object>("name", "value", "category");
        for (int i = 0; i < rows; i++) {
            df.append(randomRow());
        }
        return df;
    }

    public static DataFrame<Object> randomData(final double utilization) {
        final DataFrame<Object> df = new DataFrame<Object>("name", "value", "category");
        for (int i = 0; i < MILLIONS || memoryUtilization() < utilization; i++) {
            df.append(randomRow());
        }
        return df;
    }

    public static List<Object> randomRow() {
        return Arrays.<Object>asList(
                UUID.randomUUID().toString(),
                random.nextInt(100),
                categories[random.nextInt(categories.length)]
            );
    }

    public static double memoryUtilization() {
        return 1.0 - runtime.freeMemory() / (double)runtime.maxMemory();
    }

    public static void displayMetricsIfAvailable()
    throws Exception {
        try {
            final Class<?> metrics = Class.forName("joinery.impl.Metrics");
            final Method method = metrics.getMethod("displayMetrics");
            method.invoke(metrics);
        } catch (final ClassNotFoundException ignored) { }
    }
}
