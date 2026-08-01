package dataio;

import joinery.DataFrame;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : SQL 旧 API 全场景测试（readSql(ResultSet) / writeSql(PreparedStatement)）
 * │  Why  : 旧 API 仍保留并向后兼容，需验证其真实行为，与新 Engine API 对比
 * │  Who  : dataio-tests
 * │  When : mvn test 阶段
 *
 *  关键真实行为（来自源码阅读）：
 *    - readSql(ResultSet) 用 rs.getString(c) —— 所有列（含 INTEGER/DOUBLE/DATE）都转为 String
 *      这是 joinery 1.x 的既有行为，不是 bug；调用方需自行 convert()
 *    - readSql(Connection, sql) 内部走 Statement.executeQuery → readSql(ResultSet)
 *    - writeSql(PreparedStatement) 用 stmt.setObject 批量写，类型由 JDBC 驱动推断
 *    - writeSql(Connection, sql) 内部走 prepareStatement → writeSql
 */
class SqlLegacyApiTest {

    // ┌─ What : readSql(ResultSet) —— 所有列转 String（这是真实行为，非 bug）
    @Test
    void readSql_ResultSet_所有列转String() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:legacy1;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE t (id INT, name VARCHAR(32), price DOUBLE)");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES (1, 'alice', 9.99)");
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
                final DataFrame<Object> df = DataFrame.readSql(rs);
                // 真实行为：readSql 用 rs.getString，所以整数列也是 String
                assertEquals(String.class, df.get(0, 0).getClass(),
                    "readSql(ResultSet) 把所有列转 String（joinery 1.x 既有行为）");
                assertEquals("1", df.get(0, 0));
                assertEquals("alice", df.get(0, 1));
                assertEquals("9.99", df.get(0, 2));
            }
        }
    }

    // ┌─ What : readSql(Connection, sql) —— 与 readSql(ResultSet) 行为一致
    @Test
    void readSql_Connection_sql_与ResultSet行为一致() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:legacy2;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE t (id INT, val DOUBLE)");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES (42, 3.14)");
            final DataFrame<Object> df = DataFrame.readSql(conn, "SELECT * FROM t");
            assertEquals(1, df.length());
            assertEquals("42", df.get(0, 0), "整数列也是 String");
            assertEquals("3.14", df.get(0, 1));
        }
    }

    // ┌─ What : writeSql(PreparedStatement) 批量写 + 读回
    @Test
    void writeSql_PreparedStatement_批量写() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:legacy3;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE t (id INT, name VARCHAR(32))");
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                final DataFrame<Object> df = new DataFrame<>("id", "name");
                df.append(Arrays.asList(1, "alice"));
                df.append(Arrays.asList(2, "bob"));
                df.append(Arrays.asList(3, "carol"));
                df.writeSql(stmt);
            }
            // 读回验证（3 行）
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM t")) {
                rs.next();
                assertEquals(3, rs.getInt(1), "应批量写入 3 行");
            }
        }
    }

    // ┌─ What : writeSql(Connection, sql) —— 走 prepareStatement 内部委托
    @Test
    void writeSql_Connection_sql_委托() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:legacy4;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE t (id INT, name VARCHAR(32))");
            final DataFrame<Object> df = new DataFrame<>("id", "name");
            df.append(Arrays.asList(1, "alice"));
            df.writeSql(conn, "INSERT INTO t VALUES (?, ?)");
            final DataFrame<Object> back = DataFrame.readSql(conn, "SELECT * FROM t");
            assertEquals(1, back.length());
        }
    }

    // ┌─ What : 写入 NULL 值再读回
    @Test
    void writeSql_NULL值写入读回() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:legacy5;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE t (id INT, name VARCHAR(32))");
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                final DataFrame<Object> df = new DataFrame<>("id", "name");
                df.append(Arrays.asList(1, null));   // name 列写 null
                df.append(Arrays.asList(2, "bob"));
                df.writeSql(stmt);
            }
            // 用 readSqlQuery（强类型）读回，验证 NULL 是否正确
            final DataFrame<Object> back = DataFrame.readSqlQuery(conn, "SELECT * FROM t ORDER BY id");
            assertNull(back.get(0, 1), "第 1 行 name 应为 null");
            assertEquals("bob", back.get(1, 1));
        }
    }

    // ┌─ What : 空表读回（0 行）
    @Test
    void readSql_空表_返回0行DataFrame() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:legacy6;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE empty_t (id INT, name VARCHAR(32))");
            // 不插入任何数据
            final DataFrame<Object> df = DataFrame.readSql(conn, "SELECT * FROM empty_t");
            assertEquals(0, df.length(), "空表应返回 0 行");
            assertEquals(2, df.size(), "但列数应保留（表头 id, name）");
        }
    }

    // ┌─ What : ResultSet 已关闭后 readSql 应抛异常（finally 关闭）
    @Test
    void readSql_ResultSet自动关闭_finally语义() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:legacy7;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE t (id INT)");
            conn.createStatement().executeUpdate("INSERT INTO t VALUES (1)");
            final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery("SELECT * FROM t");
            DataFrame.readSql(rs);
            // readSql 内部 finally 关闭了 rs，再访问应抛异常
            assertThrows(SQLException.class, rs::next, "readSql 后 rs 应已关闭");
        }
    }

    // ┌─ What : 中文数据写入与读回（验证字符集）
    @Test
    void writeSql_readSql_中文数据完整() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:legacy8;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            conn.createStatement().executeUpdate("CREATE TABLE t (id INT, 姓名 VARCHAR(32))");
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                final DataFrame<Object> df = new DataFrame<>("id", "姓名");
                df.append(Arrays.asList(1, "张三"));
                df.append(Arrays.asList(2, "李四"));
                df.writeSql(stmt);
            }
            final DataFrame<Object> back = DataFrame.readSql(conn, "SELECT * FROM t ORDER BY id");
            assertEquals("张三", back.get(0, 1), "中文姓名应完整保留");
            assertEquals("李四", back.get(1, 1));
        }
    }
}
