package dataio;

import joinery.DataFrame;
import joinery.sql.Engine;
import joinery.sql.IfExists;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : Engine/Dialect 全场景契约测试
 * │  Why  : 验证方言的 DDL 生成、标识符转义、类型映射、Engine 的读写链路与边界
 * │  Who  : dataio-tests
 */
class DialectContractTest {

    // ─────────────── Dialect.matches / driverClassName ───────────────

    @Test
    void dialect_matches_按URL前缀和端口识别() {
        // 用 Engine.fromUrl().dialect() 拿方言实例
        assertTrue(DataFrame.sqlEngine("jdbc:h2:mem:t").dialect().matches("jdbc:h2:mem:x"));
        assertTrue(DataFrame.sqlEngine("jdbc:mysql://h:3306/d").dialect().matches("jdbc:mysql://h:3306/d"));
        assertTrue(DataFrame.sqlEngine("jdbc:postgresql://h/d").dialect().matches("jdbc:postgresql://h/d"));
        assertTrue(DataFrame.sqlEngine("jdbc:sqlite:/tmp/x.db").dialect().matches("jdbc:sqlite:/tmp/y.db"));
        // Doris 按端口 9030 识别
        assertTrue(DataFrame.sqlEngine("jdbc:mysql://h:9030/d").dialect() instanceof joinery.sql.dialects.DorisDialect);
    }

    @Test
    void dialect_driverClassName_各驱动类名正确() {
        assertEquals("org.h2.Driver", DataFrame.sqlEngine("jdbc:h2:mem:t").dialect().driverClassName());
        assertEquals("com.mysql.cj.jdbc.Driver",
            DataFrame.sqlEngine("jdbc:mysql://h:3306/d").dialect().driverClassName());
        assertEquals("org.postgresql.Driver",
            DataFrame.sqlEngine("jdbc:postgresql://h/d").dialect().driverClassName());
        assertEquals("org.sqlite.JDBC",
            DataFrame.sqlEngine("jdbc:sqlite:/tmp/x.db").dialect().driverClassName());
        assertEquals("oracle.jdbc.OracleDriver",
            new joinery.sql.dialects.OracleDialect().driverClassName());
    }

    // ─────────────── quoteIdentifier ───────────────

    @Test
    void quoteIdentifier_各方言转义符正确() {
        // MySQL 系（含 Doris）用反引号
        assertEquals("`user`", DataFrame.sqlEngine("jdbc:mysql://h:3306/d").dialect().quoteIdentifier("user"));
        assertEquals("`user`", DataFrame.sqlEngine("jdbc:mysql://h:9030/d").dialect().quoteIdentifier("user"));
        // H2/PG/Oracle/SQLite 用双引号
        assertEquals("\"user\"", DataFrame.sqlEngine("jdbc:h2:mem:t").dialect().quoteIdentifier("user"));
        assertEquals("\"user\"", DataFrame.sqlEngine("jdbc:postgresql://h/d").dialect().quoteIdentifier("user"));
        assertEquals("\"user\"", DataFrame.sqlEngine("jdbc:sqlite:/tmp/x.db").dialect().quoteIdentifier("user"));
        assertEquals("\"user\"", new joinery.sql.dialects.OracleDialect().quoteIdentifier("user"));
    }

    // ─────────────── sqlType 类型映射 ───────────────

    @Test
    void sqlType_各方言按Java类型翻译() {
        // PG：String → TEXT，Long → BIGINT
        final joinery.sql.Dialect pg = DataFrame.sqlEngine("jdbc:postgresql://h/d").dialect();
        assertEquals("TEXT", pg.sqlType(String.class));
        assertEquals("BIGINT", pg.sqlType(Long.class));
        assertEquals("DOUBLE", pg.sqlType(Double.class));

        // Oracle：String → VARCHAR2，Long → NUMBER(19)，Boolean → NUMBER(1)
        final joinery.sql.Dialect ora = new joinery.sql.dialects.OracleDialect();
        assertEquals("VARCHAR2(255)", ora.sqlType(String.class));
        assertEquals("NUMBER(19)", ora.sqlType(Long.class));
        assertEquals("NUMBER(1)", ora.sqlType(Boolean.class));
        assertEquals("TIMESTAMP", ora.sqlType(Date.class));

        // SQLite：String → TEXT，Long → INTEGER，Double → REAL
        final joinery.sql.Dialect sqlite = DataFrame.sqlEngine("jdbc:sqlite:/tmp/x.db").dialect();
        assertEquals("TEXT", sqlite.sqlType(String.class));
        assertEquals("INTEGER", sqlite.sqlType(Long.class));
        assertEquals("REAL", sqlite.sqlType(Double.class));

        // MySQL：Boolean → TINYINT(1)
        final joinery.sql.Dialect mysql = DataFrame.sqlEngine("jdbc:mysql://h:3306/d").dialect();
        assertEquals("TINYINT(1)", mysql.sqlType(Boolean.class));
    }

    @Test
    void sqlType_null类型_默认String兜底() {
        // null Java 类型应默认 VARCHAR（保证中文/文本可用）
        final joinery.sql.Dialect pg = DataFrame.sqlEngine("jdbc:postgresql://h/d").dialect();
        assertEquals("TEXT", pg.sqlType(null));
    }

    // ─────────────── supportsIfNotExists ───────────────

    @Test
    void supportsIfNotExists_Oracle为false其余true() {
        // Oracle 老版本不支持 CREATE TABLE IF NOT EXISTS（21c+ 才支持）
        assertFalse(new joinery.sql.dialects.OracleDialect().supportsIfNotExists());
        assertTrue(DataFrame.sqlEngine("jdbc:h2:mem:t").dialect().supportsIfNotExists());
        assertTrue(DataFrame.sqlEngine("jdbc:mysql://h:3306/d").dialect().supportsIfNotExists());
        assertTrue(DataFrame.sqlEngine("jdbc:postgresql://h/d").dialect().supportsIfNotExists());
        assertTrue(DataFrame.sqlEngine("jdbc:sqlite:/tmp/x.db").dialect().supportsIfNotExists());
    }

    // ─────────────── buildCreateTable DDL 生成 ───────────────

    @Test
    void buildCreateTable_PG生成的DDL含IF_NOT_EXISTS和TEXT类型() {
        final joinery.sql.Dialect pg = DataFrame.sqlEngine("jdbc:postgresql://h/d").dialect();
        final java.util.List<joinery.sql.Dialect.ColumnDef> cols = Arrays.asList(
            new joinery.sql.Dialect.ColumnDef("name", String.class),
            new joinery.sql.Dialect.ColumnDef("age", Long.class)
        );
        final String ddl = pg.buildCreateTable("\"users\"", true, cols);
        assertTrue(ddl.contains("CREATE TABLE IF NOT EXISTS"), "PG 支持 IF NOT EXISTS");
        assertTrue(ddl.contains("\"users\""));
        assertTrue(ddl.contains("\"name\" TEXT"));
        assertTrue(ddl.contains("\"age\" BIGINT"));
    }

    @Test
    void buildCreateTable_Oracle生成的DDL不含IF_NOT_EXISTS() {
        final joinery.sql.Dialect ora = new joinery.sql.dialects.OracleDialect();
        final java.util.List<joinery.sql.Dialect.ColumnDef> cols = Arrays.asList(
            new joinery.sql.Dialect.ColumnDef("id", Long.class)
        );
        final String ddl = ora.buildCreateTable("\"t\"", true, cols);
        // Oracle supportsIfNotExists=false，即便传 true 也不应加 IF NOT EXISTS
        assertFalse(ddl.contains("IF NOT EXISTS"));
        assertTrue(ddl.contains("\"id\" NUMBER(19)"));
    }

    @Test
    void buildCreateTable_MySQL用反引号() {
        final joinery.sql.Dialect mysql = DataFrame.sqlEngine("jdbc:mysql://h:3306/d").dialect();
        final java.util.List<joinery.sql.Dialect.ColumnDef> cols = Arrays.asList(
            new joinery.sql.Dialect.ColumnDef("name", String.class),
            new joinery.sql.Dialect.ColumnDef("flag", Boolean.class)
        );
        final String ddl = mysql.buildCreateTable("`t`", false, cols);
        assertTrue(ddl.contains("`name` VARCHAR(255)"));
        assertTrue(ddl.contains("`flag` TINYINT(1)"));
    }

    // ─────────────── Engine 真实读写边界 ───────────────

    @Test
    void engine_writeTable_批写大批量_1000行(@TempDir Path tmp) throws Exception {
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:batch;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
        final DataFrame<Object> df = new DataFrame<>("id", "val");
        for (int i = 0; i < 1000; i++) {
            df.append(Arrays.asList((long) i, "v" + i));
        }
        df.convert();
        df.writeSqlTable(engine, "batch_t", IfExists.REPLACE, 100);   // batch=100，10 批
        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "batch_t");
        assertEquals(1000, back.length());
        assertEquals(999L, back.get(999, 0));
    }

    @Test
    void engine_writeTable_NULL值读写(@TempDir Path tmp) throws Exception {
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:null;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
        final DataFrame<Object> df = new DataFrame<>("id", "name");
        df.append(Arrays.asList(1L, null));
        df.append(Arrays.asList(2L, "x"));
        df.convert();
        df.writeSqlTable(engine, "null_t", IfExists.REPLACE, 100);
        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "null_t");
        assertNull(back.get(0, 1), "第 1 行 name 应为 null");
        assertEquals("x", back.get(1, 1));
    }

    @Test
    void engine_writeTable_空DataFrame_不报错() throws Exception {
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:empty;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
        final DataFrame<Object> df = new DataFrame<>("id");   // 0 行
        // 只建表不插数据
        df.writeSqlTable(engine, "empty_t", IfExists.REPLACE, 100);
        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "empty_t");
        assertEquals(0, back.length());
    }

    @Test
    void engine_Connection_API_与_Engine_API_行为一致(@TempDir Path tmp) throws Exception {
        // 两种 API 写同样的数据，读回应一致
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:cmp;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1")) {
            final DataFrame<Object> df = new DataFrame<>("id", "name");
            df.append(Arrays.asList(1L, "alice"));
            df.convert();

            // Connection API（旧）
            df.writeSqlTable(conn, "old_t", IfExists.REPLACE, 100);
            final DataFrame<Object> byConn = DataFrame.readSqlTable(conn, "old_t");

            // Engine API（新）
            final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:cmp2;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
            df.writeSqlTable(engine, "new_t", IfExists.REPLACE);
            final DataFrame<Object> byEngine = DataFrame.readSqlTable(engine, "new_t");

            assertEquals(byConn.length(), byEngine.length());
            assertEquals(byConn.get(0, 0), byEngine.get(0, 0));
            assertEquals(byConn.get(0, 1), byEngine.get(0, 1));
        }
    }

    @Test
    void engine_readSqlQuery_保留SQL列类型() throws Exception {
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:typed;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
        try (Connection conn = engine.connection()) {
            conn.createStatement().executeUpdate("CREATE TABLE typed (id INTEGER, price DOUBLE, name VARCHAR(32))");
            conn.createStatement().executeUpdate("INSERT INTO typed VALUES (1, 9.99, 'apple')");
        }
        final DataFrame<Object> df = engine.readQuery("SELECT * FROM typed");
        // readQuery 强类型：INTEGER→Long，DOUBLE→Double，VARCHAR→String
        assertEquals(Long.class, df.get(0, 0).getClass());
        assertEquals(Double.class, df.get(0, 1).getClass());
        assertEquals(String.class, df.get(0, 2).getClass());
    }

    @Test
    void engine_jdbcUrl_dialect_访问器() {
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:x");
        assertEquals("jdbc:h2:mem:x", engine.jdbcUrl());
        assertNotNull(engine.dialect());
    }

    @Test
    void engine_fromUrl_空URL_抛IllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> Engine.fromUrl(null));
        assertThrows(IllegalArgumentException.class, () -> Engine.fromUrl(""));
    }

    @Test
    void engine_fromUrl_驱动缺失_抛IllegalStateException带提示() {
        // Oracle ojdbc 不在 classpath
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> Engine.fromUrl("jdbc:oracle:thin:@localhost:1521:orcl"));
        assertTrue(ex.getMessage().contains("oracle.jdbc.OracleDriver"));
        assertTrue(ex.getMessage().contains("classpath"), "异常消息应提示加驱动到 classpath");
    }
}
