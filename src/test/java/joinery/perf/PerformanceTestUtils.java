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
