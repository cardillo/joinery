package joinery.perf;

import java.util.ArrayList;
import java.util.List;

import joinery.DataFrame;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class DataFrameSortByPerfTest {
    @After
    public void report()
    throws Exception {
        PerformanceTestUtils.displayMetricsIfAvailable();
    }

    @Test
    @Category(PerformanceTests.class)
    public void test() {
        final DataFrame<Object> df = PerformanceTestUtils.randomData(0.5);
        final List<String> columns = new ArrayList<>(df.columns());
        for (int i = 0; i < 10; i++) {
            final String key = String.format("%s%s", i % 2 == 0 ? "" : "-", columns.get(i / 2 % columns.size()));
            System.out.printf("sorting %,d rows by %s\n", df.length(), key);
            df.sortBy(key);
        }
    }
}
