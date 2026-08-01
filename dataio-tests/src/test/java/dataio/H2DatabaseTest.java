package dataio;

import joinery.DataFrame;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : 数据库导入导出测试（H2 + MODE 模拟 Oracle/MySQL/PostgreSQL 方言）
 * │  Why  : 用户要求支持 PG/Oracle/MySQL/SQLite/Doris/H2 等数据库，但官方 JDBC 驱动不在 Maven Central（Oracle）；
 * │         统一用 H2 嵌入式（兼容模式 MODE=Oracle/MySQL/PostgreSQL）验证 readSqlTable/writeSqlTable + 方言兼容性
 * │  Who  : dataio-tests 测试套件
 * │  When : mvn test 阶段
 * │  Where: dataio-tests/src/test/java/dataio/H2DatabaseTest.java
 *
 *  连接字符串格式（中文完美支持）：
 *    jdbc:h2:mem:test;MODE=MySQL;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true
 *    —— DATABASE_TO_UPPER=false 让中文列名保留原样，避免大小写/中文乱码问题
 */
class H2DatabaseTest {

    // ┌─ What : H2 默认模式 —— writeSqlTable 自动建表 + readSqlTable 全表读回
    @Test
    void writeSqlTable_自动建表_readSqlTable_全表读回() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_default;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            final DataFrame<Object> df = new DataFrame<>("id", "name", "amount");
            df.append(Arrays.asList(1L, "alpha", 1.5));
            df.append(Arrays.asList(2L, "bravo", 2.5));
            df.append(Arrays.asList(3L, "charlie", 3.5));
            df.convert();

            // 写入（自动建表，若存在则替换）
            df.writeSqlTable(conn, "t_data", joinery.sql.IfExists.REPLACE, 100);

            // 全表读回
            final DataFrame<Object> back = DataFrame.readSqlTable(conn, "t_data");
            assertEquals(3, back.length());
            assertEquals(2L, back.get(1, 0), "第 2 行 id=2");
            assertEquals("bravo", back.get(1, 1));
            assertEquals(2.5, ((Number) back.get(1, 2)).doubleValue(), 0.001);
        }
    }

    // ┌─ What : 中文数据 + 中文列名完整支持（DATABASE_TO_UPPER=false）
    @Test
    void writeReadSql_中文列名中文数据完整() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test_cn;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true;DB_CLOSE_DELAY=-1")) {
            final DataFrame<Object> df = new DataFrame<>("姓名", "城市", "金额");
            df.append(Arrays.asList("张三", "北京", 1234.56));
            df.append(Arrays.asList("李四", "上海", 7890.12));
            df.append(Arrays.asList("王五", "广州", 5678.90));
            df.convert();

            df.writeSqlTable(conn, "用户表", joinery.sql.IfExists.REPLACE, 100);

            final DataFrame<Object> back = DataFrame.readSqlTable(conn, "用户表");
            assertEquals(3, back.length());
            // 列名应保留中文
            java.util.List<Object> cols = new java.util.ArrayList<>(back.columns());
            assertTrue(cols.contains("姓名"), "列名应含中文「姓名」");
            assertTrue(cols.contains("城市"), "列名应含中文「城市」");
            // 数据应保留中文
            assertEquals("张三", back.get(0, cols.indexOf("姓名")));
            assertEquals("北京", back.get(0, cols.indexOf("城市")));
        }
    }

    // ┌─ What : MySQL 兼容模式（MODE=MySQL）—— 用 H2 模拟 MySQL 方言
    @Test
    void mysql兼容模式_读写正常() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test_mysql;MODE=MySQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            // 用 MySQL 方言建表
            conn.createStatement().executeUpdate(
                    "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(50), price DECIMAL(10,2))");
            conn.createStatement().executeUpdate(
                    "INSERT INTO products VALUES (1, '苹果', 5.50), (2, '香蕉', 3.80), (3, '橙子', 8.20)");

            final DataFrame<Object> df = DataFrame.readSqlTable(conn, "products");
            assertEquals(3, df.length());
            assertEquals("苹果", df.get(0, 1));
        }
    }

    // ┌─ What : PostgreSQL 兼容模式（MODE=PostgreSQL）—— 用 H2 模拟 PG 方言
    @Test
    void postgres兼容模式_读写正常() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test_pg;MODE=PostgreSQL;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate(
                    "CREATE TABLE employees (id INTEGER, name VARCHAR(50), salary DOUBLE PRECISION)");
            conn.createStatement().executeUpdate(
                    "INSERT INTO employees VALUES (1, '张三', 15000.0), (2, '李四', 18000.0)");

            final DataFrame<Object> df = DataFrame.readSqlTable(conn, "employees");
            assertEquals(2, df.length());
            assertEquals("张三", df.get(0, 1));
            assertEquals(18000.0, ((Number) df.get(1, 2)).doubleValue(), 0.001);
        }
    }

    // ┌─ What : Oracle 兼容模式（MODE=Oracle）—— 用 H2 模拟 Oracle 方言
    @Test
    void oracle兼容模式_读写正常() throws SQLException {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test_ora;MODE=Oracle;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate(
                    "CREATE TABLE orders (id NUMBER, customer VARCHAR(50), total NUMBER)");
            conn.createStatement().executeUpdate(
                    "INSERT INTO orders VALUES (1001, '客户A', 500), (1002, '客户B', 1200)");

            final DataFrame<Object> df = DataFrame.readSqlTable(conn, "orders");
            assertEquals(2, df.length());
            assertEquals("客户A", df.get(0, 1));
        }
    }

    // ┌─ What : IfExists.FAIL 策略 —— 表已存在应抛异常
    @Test
    void writeSqlTable_表已存在且FAIL策略_应抛异常() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_fail;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            final DataFrame<Object> df = new DataFrame<>("id");
            df.append(Arrays.asList(1L));
            // 先建一次
            df.writeSqlTable(conn, "t_dup", joinery.sql.IfExists.REPLACE, 10);
            // 再用 FAIL 应抛
            assertThrows(SQLException.class, () ->
                df.writeSqlTable(conn, "t_dup", joinery.sql.IfExists.FAIL, 10));
        }
    }

    // ┌─ What : IfExists.APPEND 策略 —— 数据追加
    @Test
    void writeSqlTable_APPEND策略_数据追加() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test_append;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            final DataFrame<Object> df = new DataFrame<>("id", "v");
            df.append(Arrays.asList(1L, "a"));
            df.convert();

            df.writeSqlTable(conn, "t_app", joinery.sql.IfExists.REPLACE, 10);

            // 第二次用 APPEND
            final DataFrame<Object> df2 = new DataFrame<>("id", "v");
            df2.append(Arrays.asList(2L, "b"));
            df2.convert();
            df2.writeSqlTable(conn, "t_app", joinery.sql.IfExists.APPEND, 10);

            final DataFrame<Object> back = DataFrame.readSqlTable(conn, "t_app");
            assertEquals(2, back.length(), "两次写入应共 2 行（追加）");
        }
    }
}
