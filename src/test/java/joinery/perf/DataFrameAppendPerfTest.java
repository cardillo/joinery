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
