package dataio;

import joinery.DataFrame;
import joinery.sql.Engine;
import joinery.sql.IfExists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ┌─ What : 六方言（H2/MySQL/PostgreSQL/Oracle/SQLite/Doris）接入与读写验证
 * │  Why  : 验证 Engine.fromUrl 按 URL 自动选对方言 + 真实驱动可读写
 * │  Who  : dataio-tests 测试套件
 * │  When : mvn test 阶段
 * │  Where: dataio-tests/src/test/java/dataio/DialectTest.java
 *
 *  方言识别：按 URL 前缀（jdbc:h2:/jdbc:mysql:/jdbc:postgresql:/jdbc:oracle:/jdbc:sqlite:）
 *           + 端口识别（MySQL 3306 vs Doris 9030）
 *  测试策略：
 *    - H2：嵌入式直连真实读写
 *    - SQLite：真实文件直连读写
 *    - MySQL/PostgreSQL：H2 MODE 模拟（无需真实服务器）+ 真实驱动加载验证
 *    - Oracle：H2 MODE=Oracle 模拟（ojdbc 不在 Maven Central）
 *    - Doris：方言识别测试（按端口 9030 选 DorisDialect）
 */
class DialectTest {

    // ┌─ What : 六方言的 URL 识别（按前缀 + 端口）
    // │  注意：Oracle ojdbc 不在 Maven Central，识别测试用 assertThrows 验证 URL 被识别为 OracleDialect
    // │        （加载驱动会抛 IllegalStateException，但消息里含驱动类名，证明方言识别正确）
    @Test
    void fromUrl_六方言识别正确() {
        assertEquals(joinery.sql.dialects.H2Dialect.class,
            DataFrame.sqlEngine("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1").dialect().getClass());
        assertEquals(joinery.sql.dialects.MySQLDialect.class,
            DataFrame.sqlEngine("jdbc:mysql://localhost:3306/db").dialect().getClass());
        assertEquals(joinery.sql.dialects.PostgreSQLDialect.class,
            DataFrame.sqlEngine("jdbc:postgresql://localhost:5432/db").dialect().getClass());
        assertEquals(joinery.sql.dialects.SQLiteDialect.class,
            DataFrame.sqlEngine("jdbc:sqlite:/tmp/test.db").dialect().getClass());
        // Doris：端口 9030 必须识别为 DorisDialect 而非 MySQLDialect
        assertEquals(joinery.sql.dialects.DorisDialect.class,
            DataFrame.sqlEngine("jdbc:mysql://doris-host:9030/db").dialect().getClass(),
            "端口 9030 应识别为 Doris（而非通用 MySQL）");

        // Oracle：ojdbc 不在 classpath，fromUrl 会抛 IllegalStateException，
        // 但消息含 oracle.jdbc.OracleDriver —— 证明 URL 被识别为 Oracle 方言
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> DataFrame.sqlEngine("jdbc:oracle:thin:@localhost:1521:orcl"));
        assertTrue(ex.getMessage().contains("oracle.jdbc.OracleDriver"),
            "Oracle URL 应识别并尝试加载 ojdbc 驱动，消息含驱动类名");
    }

    @Test
    void fromUrl_不支持的URL_应抛带提示的异常() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> DataFrame.sqlEngine("jdbc:unknown://localhost/db"));
        assertTrue(ex.getMessage().contains("不支持的 JDBC URL"),
            "异常消息应提示不支持的方言");
    }

    // ┌─ What : H2 嵌入式真实读写（默认方言）
    @Test
    void h2默认模式_Engine读写往返() throws Exception {
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:dialect_h2;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true;DB_CLOSE_DELAY=-1");
        final DataFrame<Object> df = new DataFrame<>("id", "name", "value");
        df.append(Arrays.asList(1L, "alpha", 1.5));
        df.append(Arrays.asList(2L, "beta", 2.5));
        df.convert();

        df.writeSqlTable(engine, "t_h2", IfExists.REPLACE, 10);
        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "t_h2");

        assertEquals(2, back.length());
        assertEquals(2L, back.get(1, 0));
        assertEquals("beta", back.get(1, 1));
    }

    // ┌─ What : MySQL 兼容模式（MODE=MySQL）—— H2 模拟 + 真实驱动加载
    // │  Why  : 不依赖真实 MySQL 服务器；同时验证 MySQL Connector/J 驱动可被加载
    @Test
    void mysql兼容模式_Engine读写() throws Exception {
        // 真实 MySQL URL（只用于方言识别 + 驱动加载验证，不实际连接）
        assertDoesNotThrow(() -> DataFrame.sqlEngine("jdbc:mysql://localhost:3306/test"));

        // 实际读写走 H2 MODE=MySQL
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:dialect_mysql;MODE=MySQL;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true;DB_CLOSE_DELAY=-1");
        final DataFrame<Object> df = new DataFrame<>("id", "name");
        df.append(Arrays.asList(1L, "苹果"));
        df.append(Arrays.asList(2L, "香蕉"));
        df.convert();
        df.writeSqlTable(engine, "t_mysql", IfExists.REPLACE, 10);

        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "t_mysql");
        assertEquals(2, back.length());
        assertEquals("苹果", back.get(0, 1), "中文应完整保留");
    }

    // ┌─ What : PostgreSQL 兼容模式（MODE=PostgreSQL）+ 真实驱动
    @Test
    void postgres兼容模式_Engine读写() throws Exception {
        assertDoesNotThrow(() -> DataFrame.sqlEngine("jdbc:postgresql://localhost:5432/test"));
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:dialect_pg;MODE=PostgreSQL;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true;DB_CLOSE_DELAY=-1");
        final DataFrame<Object> df = new DataFrame<>("id", "value");
        df.append(Arrays.asList(1L, 100.0));
        df.append(Arrays.asList(2L, 200.0));
        df.convert();
        df.writeSqlTable(engine, "t_pg", IfExists.REPLACE, 10);

        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "t_pg");
        assertEquals(2, back.length());
        assertEquals(200.0, ((Number) back.get(1, 1)).doubleValue(), 0.001);
    }

    // ┌─ What : Oracle 兼容模式（MODE=Oracle）—— ojdbc 不在 Maven Central，仅用 H2 模拟方言切换
    @Test
    void oracle兼容模式_Engine读写() throws Exception {
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:dialect_ora;MODE=Oracle;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true;DB_CLOSE_DELAY=-1");
        final DataFrame<Object> df = new DataFrame<>("id", "name");
        df.append(Arrays.asList(1001L, "客户A"));
        df.append(Arrays.asList(1002L, "客户B"));
        df.convert();
        df.writeSqlTable(engine, "t_ora", IfExists.REPLACE, 10);

        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "t_ora");
        assertEquals(2, back.length());
        assertEquals("客户A", back.get(0, 1));
    }

    // ┌─ What : SQLite 真实文件直连（不需要服务器，验证 SQLiteDialect）
    @Test
    void sqlite真实文件_Engine读写往返(@TempDir Path tmp) throws Exception {
        final String url = "jdbc:sqlite:" + tmp.resolve("test.db").toString();
        final Engine engine = DataFrame.sqlEngine(url);

        final DataFrame<Object> df = new DataFrame<>("id", "name", "amount");
        df.append(Arrays.asList(1L, "张三", 100.0));
        df.append(Arrays.asList(2L, "李四", 200.0));
        df.convert();
        df.writeSqlTable(engine, "users", IfExists.REPLACE, 10);

        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "users");
        assertEquals(2, back.length());
        assertEquals("张三", back.get(0, 1), "中文应完整保留");
    }

    // ┌─ What : Doris 方言识别（端口 9030）+ 兼容模式读写
    // │  Why  : Doris 兼容 MySQL 协议，复用 MySQLDialect；本测试验证端口识别 + MODE=MySQL 模拟读写
    @Test
    void doris方言识别_9030端口与读写模拟() throws Exception {
        // 端口 9030 识别为 Doris（驱动复用 MySQL Connector/J）
        assertEquals(joinery.sql.dialects.DorisDialect.class,
            DataFrame.sqlEngine("jdbc:mysql://doris-fe:9030/test").dialect().getClass());

        // 实际读写走 H2 MODE=MySQL 模拟（不依赖真实 Doris 集群）
        final Engine engine = DataFrame.sqlEngine("jdbc:h2:mem:dialect_doris;MODE=MySQL;DATABASE_TO_UPPER=false;CASE_INSENSITIVE_IDENTIFIERS=true;DB_CLOSE_DELAY=-1");
        final DataFrame<Object> df = new DataFrame<>("category", "amount");
        df.append(Arrays.asList("电子产品", 1500.0));
        df.append(Arrays.asList("图书", 300.0));
        df.convert();
        df.writeSqlTable(engine, "doris_sales", IfExists.REPLACE, 10);

        final DataFrame<Object> back = DataFrame.readSqlTable(engine, "doris_sales");
        assertEquals(2, back.length());
        assertEquals("电子产品", back.get(0, 0));
    }
}
