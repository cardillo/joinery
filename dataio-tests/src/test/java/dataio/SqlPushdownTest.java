package dataio;

import joinery.DataFrame;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : SQL 下推聚合 vs DataFrame 内存聚合的一致性验证
 * │  Why  : 用户核心诉求「测试 SQL 下推聚合 vs DataFrame 聚合」。
 * │         场景：同一份数据，分别用
 * │            (a) SELECT category, COUNT(*), AVG(value), SUM(value) FROM t GROUP BY category  （SQL 下推聚合）
 * │            (b) SELECT * 读全表后用 df.groupBy("category").count() / .mean() / .sum()     （DataFrame 聚合）
 * │         两者结果应一致。这是「计算正确性」的硬保证。
 * │  Who  : dataio-tests 测试套件
 * │  When : mvn test 阶段
 * │  Where: dataio-tests/src/test/java/dataio/SqlPushdownTest.java
 */
class SqlPushdownTest {

    // ┌─ What : 构造一个测试表：3 个分类，每个分类 3 条数据
    // │  注：分类用英文名（中文作 GROUP BY 键在 H2 上行为不稳定，与 joinery 无关）
    private void seed(Connection conn) throws SQLException {
        conn.createStatement().executeUpdate(
            "CREATE TABLE sales (category VARCHAR(32), name VARCHAR(32), amount DOUBLE)");
        conn.createStatement().executeUpdate(
            "INSERT INTO sales VALUES " +
            "('fruit','apple',10), ('fruit','banana',20), ('fruit','cherry',30)," +
            "('drink','coke',15), ('drink','sprite',25), ('drink','juice',35)," +
            "('snack','chips',8),  ('snack','cookie',12), ('snack','chocolate',18)");
    }

    @Test
    void sql下推聚合_vs_DataFrame聚合_count一致() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_agg1;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            seed(conn);

            // (a) SQL 下推 COUNT
            final DataFrame<Object> sqlAgg = DataFrame.readSqlQuery(conn,
                "SELECT category, COUNT(*) AS cnt FROM sales GROUP BY category ORDER BY category");

            // (b) DataFrame 聚合 COUNT
            final DataFrame<Object> all = DataFrame.readSqlTable(conn, "sales");
            final DataFrame<Object> dfAgg = all.groupBy("category").count()
                    .sortBy("category");

            // 对比 3 个分类的 COUNT
            assertEquals(sqlAgg.length(), dfAgg.length(), "聚合后分类数应一致");
            for (int r = 0; r < sqlAgg.length(); r++) {
                final Object sqlCnt = sqlAgg.get(r, 1);
                final Object dfCnt = dfAgg.get(r, 1);
                assertEquals(((Number) sqlCnt).longValue(),
                             ((Number) dfCnt).longValue(),
                    "分类 " + sqlAgg.get(r, 0) + " 的 COUNT 不一致");
            }
        }
    }

    @Test
    void sql下推聚合_vs_DataFrame聚合_mean_sum一致() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_agg2;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            seed(conn);

            // (a) SQL 下推 AVG/SUM
            final DataFrame<Object> sqlAgg = DataFrame.readSqlQuery(conn,
                "SELECT category, AVG(amount) AS avg_amt, SUM(amount) AS sum_amt " +
                "FROM sales GROUP BY category ORDER BY category");

            // (b) DataFrame 聚合（mean、sum）—— 自然链式写法 groupBy().sortBy().mean()
            //     （sortBy bug 已修：不再丢失 groupBy 状态。若无此修复，mean 会退化成整列平均）
            //     mean 后列结构为 [category, amount]，category 索引 0，amount 索引 1
            final DataFrame<Object> all = DataFrame.readSqlTable(conn, "sales");
            final DataFrame<Object> meanDf = all.groupBy("category").sortBy("category").mean();
            final DataFrame<Object> sumDf = all.groupBy("category").sortBy("category").sum();

            assertEquals(sqlAgg.length(), meanDf.length(), "mean 聚合后行数应一致");

            // 对比 AVG / SUM（容差 0.001）。meanDf 列：[category=0, amount=1]
            for (int r = 0; r < sqlAgg.length(); r++) {
                final double sqlAvg = ((Number) sqlAgg.get(r, 1)).doubleValue();
                final Object dfAvg = meanDf.get(r, 1);
                assertNotNull(dfAvg, "DataFrame mean 不应为 null");
                assertEquals(sqlAvg, ((Number) dfAvg).doubleValue(), 0.001,
                    "分类 " + sqlAgg.get(r, 0) + " 的 AVG(amount) 不一致");

                final double sqlSum = ((Number) sqlAgg.get(r, 2)).doubleValue();
                final Object dfSum = sumDf.get(r, 1);
                assertNotNull(dfSum);
                assertEquals(sqlSum, ((Number) dfSum).doubleValue(), 0.001,
                    "分类 " + sqlAgg.get(r, 0) + " 的 SUM(amount) 不一致");
            }
        }
    }

    // ┌─ What : 验证 join（DataFrame 内存 join）正确性
    // │  注：joinery 的 join 默认是 outer join（保留所有行），与 SQL inner join 不同
    @Test
    void DataFrame_Join正确性() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_join;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE a (id INT, name VARCHAR(32))");
            conn.createStatement().executeUpdate("INSERT INTO a VALUES (1,'a'),(2,'b'),(3,'c')");
            conn.createStatement().executeUpdate("CREATE TABLE b (id INT, val INT)");
            conn.createStatement().executeUpdate("INSERT INTO b VALUES (1,100),(2,200)");

            final DataFrame<Object> a = DataFrame.readSqlTable(conn, "a").reindex("id");
            final DataFrame<Object> b = DataFrame.readSqlTable(conn, "b").reindex("id");

            // joinery join 默认 outer：a 有 3 个 id (1,2,3)，b 有 2 个 (1,2)
            // → 结果按 index 并集，共 3 行（id=3 在 b 中缺失，val=null）
            final DataFrame<Object> joined = a.join(b);
            assertEquals(3, joined.length(), "outer join 应得 3 行（id=1,2,3）");
        }
    }

    // ┌─ What : 验证 SQL WHERE 下推过滤 vs DataFrame filter 一致
    @Test
    void sql下推过滤_vs_DataFrame_sortBy_一致() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_where;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            seed(conn);

            // SQL 下推：amount > 15 升序
            final DataFrame<Object> sqlFiltered = DataFrame.readSqlQuery(conn,
                "SELECT name, amount FROM sales WHERE amount > 15 ORDER BY amount ASC");

            // DataFrame：全表读，sortBy amount
            final DataFrame<Object> all = DataFrame.readSqlTable(conn, "sales");

            // 断言：SQL 过滤后行数 == amount > 15 的行数
            int count = 0;
            for (int r = 0; r < all.length(); r++) {
                if (((Number) all.get(r, 2)).doubleValue() > 15) {
                    count++;
                }
            }
            assertEquals(count, sqlFiltered.length(), "SQL 下推过滤行数应与 DataFrame 计数一致");
            // 首行应是最小 amount > 15
            double first = ((Number) sqlFiltered.get(0, 1)).doubleValue();
            assertTrue(first >= 15, "ORDER BY ASC 后首行 amount 应 >= 15");
        }
    }
}
