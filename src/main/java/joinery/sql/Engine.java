/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
 * Copyright (c) 2026 zc (1.11 line, GLM-5.2 AI-assisted refactor)
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

package joinery.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joinery.DataFrame;
import joinery.sql.dialects.DorisDialect;
import joinery.sql.dialects.H2Dialect;
import joinery.sql.dialects.MySQLDialect;
import joinery.sql.dialects.OracleDialect;
import joinery.sql.dialects.PostgreSQLDialect;
import joinery.sql.dialects.SQLiteDialect;

/**
 * ┌─ What : 数据库连接门面（对标 Python SQLAlchemy 的 create_engine() 返回的 Engine）
 * │  Why  : 把「连接管理 + 方言差异 + 读/写 API」收口到一个对象，
 * │         调用方只面向 Engine，不再手写 SQL/驱动类名/方言转义
 * │  Who  : 由 DataFrame.sqlEngine(jdbcUrl) 工厂调用 Engine.fromUrl(...) 创建
 * │  When : 用户需要数据库导入导出时
 * │  Where: joinery.sql.Engine
 *
 *  设计模式：
 *    - Factory（fromUrl 按 URL 自动选 Dialect）
 *    - Facade（readTable/writeTable 屏蔽底层 SQL/类型映射/批量细节）
 *    - Strategy（Dialect 是可替换策略）
 *
 *  示例：
 *    Engine engine = Engine.fromUrl("jdbc:postgresql://host:5432/db");
 *    DataFrame.readSqlTable(engine, "users");
 *    df.writeSqlTable(engine, "users", IfExists.REPLACE);
 */
public class Engine {

    // ┌─ What : 首批注册的方言列表（顺序敏感：Doris 必须先于 MySQL，否则端口 9030 被通用 MySQL 抢匹配）
    private static final List<Dialect> REGISTERED = Arrays.asList(
            new SQLiteDialect(),
            new OracleDialect(),
            new PostgreSQLDialect(),
            new H2Dialect(),
            new DorisDialect(),     // 必须先于 MySQLDialect（端口 9030 优先识别为 Doris）
            new MySQLDialect()
        );

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final Dialect dialect;

    private Engine(final String jdbcUrl, final String user, final String password, final Dialect dialect) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.dialect = dialect;
    }

    // ┌─ What : 工厂方法（按 JDBC URL 自动选方言）—— 对标 SQLAlchemy create_engine(url)
    // │  How  : 伪代码：
    //            1. 遍历 REGISTERED 方言，找第一个 matches(url) 的
    //            2. 找不到 → 抛 IllegalArgumentException，提示支持的方言列表
    //            3. Class.forName 加载驱动类（要求驱动在 classpath；缺失给清晰错误）
    public static Engine fromUrl(final String jdbcUrl) {
        return fromUrl(jdbcUrl, null, null);
    }

    public static Engine fromUrl(final String jdbcUrl, final String user, final String password) {
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException("jdbcUrl 不能为空");
        }
        // 选方言
        Dialect picked = null;
        for (final Dialect d : REGISTERED) {
            if (d.matches(jdbcUrl)) {
                picked = d;
                break;
            }
        }
        if (picked == null) {
            throw new IllegalArgumentException(
                "不支持的 JDBC URL: " + jdbcUrl + "，当前支持的方言: H2/MySQL/PostgreSQL/Oracle/SQLite/Doris");
        }
        // 加载驱动类（缺失则给清晰提示）
        try {
            Class.forName(picked.driverClassName());
        }
        catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "JDBC 驱动未找到: " + picked.driverClassName()
                + "。请把对应驱动 jar 加入 classpath（joinery 主工程不含具体驱动）", e);
        }
        return new Engine(jdbcUrl, user, password, picked);
    }

    /** 暴露此 Engine 使用的方言（测试/调试用） */
    public Dialect dialect() {
        return dialect;
    }

    /** 暴露 JDBC URL */
    public String jdbcUrl() {
        return jdbcUrl;
    }

    // ┌─ What : 建立新连接（每次调用新建一个，调用方负责关闭）
    // │  Why  : 简单稳妥；连接池由使用方在更高层引入（如 HikariCP），本类不耦合池实现
    public Connection connection() throws SQLException {
        if (user != null) {
            return DriverManager.getConnection(jdbcUrl, user, password);
        }
        return DriverManager.getConnection(jdbcUrl);
    }

    // ───────────────────────── 读 API（对标 pandas.read_sql） ─────────────────────────

    /**
     * 按表名直读全表（对标 pandas.read_sql_table）。
     * 按 ResultSetMetaData 列类型保留原生 Java 类型（INTEGER→Long、DOUBLE→Double、TIMESTAMP→Date...），
     * 中文字符串 VARCHAR 走 getString，UTF-8 原生支持。
     */
    public DataFrame<Object> readTable(final String tableName) throws SQLException {
        return readQuery("SELECT * FROM " + dialect.quoteIdentifier(tableName));
    }

    /**
     * 执行 SQL 查询并按列原类型读取（强类型版，对标 pandas.read_sql_query）。
     */
    public DataFrame<Object> readQuery(final String sql) throws SQLException {
        try (Connection conn = connection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return readResultSet(rs);
        }
    }

    // ┌─ What : 把 ResultSet 强类型读为 DataFrame（保留 SQL 列类型，不全转字符串）
    // │  How  : 见 Database.readSqlQuery 同名方法的 5W1H（已迁过来）
    public static DataFrame<Object> readResultSet(final ResultSet rs) throws SQLException {
        final ResultSetMetaData md = rs.getMetaData();
        final int n = md.getColumnCount();
        final List<String> columns = new ArrayList<>(n);
        final int[] types = new int[n];
        for (int i = 0; i < n; i++) {
            columns.add(md.getColumnLabel(i + 1));
            types[i] = md.getColumnType(i + 1);
        }
        final DataFrame<Object> df = new DataFrame<>(columns);
        while (rs.next()) {
            final List<Object> row = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                row.add(getTypedValue(rs, i + 1, types[i]));
            }
            df.append(row);
        }
        return df;
    }

    private static Object getTypedValue(final ResultSet rs, final int idx, final int jdbcType)
    throws SQLException {
        switch (jdbcType) {
            case Types.INTEGER: case Types.BIGINT: case Types.SMALLINT: case Types.TINYINT: {
                final long v = rs.getLong(idx);
                return rs.wasNull() ? null : v;
            }
            case Types.DOUBLE: case Types.FLOAT: case Types.REAL: {
                final double v = rs.getDouble(idx);
                return rs.wasNull() ? null : v;
            }
            case Types.DECIMAL: case Types.NUMERIC: {
                final java.math.BigDecimal bd = rs.getBigDecimal(idx);
                return bd == null ? null : bd.doubleValue();
            }
            case Types.DATE:
                return rs.getDate(idx);
            case Types.TIMESTAMP: case Types.TIME_WITH_TIMEZONE: case Types.TIMESTAMP_WITH_TIMEZONE:
                return rs.getTimestamp(idx);
            case Types.BOOLEAN: case Types.BIT: {
                final boolean v = rs.getBoolean(idx);
                return rs.wasNull() ? null : v;
            }
            case Types.BLOB: case Types.BINARY: case Types.VARBINARY: case Types.LONGVARBINARY:
                return rs.getBytes(idx);
            default:
                // VARCHAR/CHAR/NVARCHAR/CLOB/LONGVARCHAR —— 中文字符串走这里
                return rs.getString(idx);
        }
    }

    // ───────────────────────── 写 API（对标 pandas.to_sql） ─────────────────────────

    /**
     * 把 DataFrame 写入数据库表（对标 pandas.to_sql）。
     * 自动按列类型建表/覆盖/追加，并批量插入。
     */
    public <V> void writeTable(final DataFrame<V> df, final String tableName, final IfExists ifExists, final int batchSize)
    throws SQLException {
        try (Connection conn = connection()) {
            writeTableWithConnection(df, conn, tableName, ifExists, batchSize);
        }
    }

    /** 便捷重载：默认 batchSize=1000 */
    public <V> void writeTable(final DataFrame<V> df, final String tableName, final IfExists ifExists)
    throws SQLException {
        writeTable(df, tableName, ifExists, 1000);
    }

    // ┌─ What : 用已有连接写表（内部实现 + 兼容旧 Connection 风格 API）
    // │  How  : 伪代码：
    //            1. 探测表是否存在
    //            2. 按 ifExists：FAIL 抛异常；REPLACE 先 DROP；APPEND 跳过建表
    //            3. 不存在或 REPLACE → buildCreateTable + executeUpdate
    //            4. 批量 INSERT（每批 batchSize 行 + commit）
    public <V> void writeTableWithConnection(final DataFrame<V> df, final Connection conn,
            final String tableName, final IfExists ifExists, final int batchSize)
    throws SQLException {
        final String quotedTable = dialect.quoteIdentifier(tableName);
        final boolean exists = tableExists(conn, tableName);

        if (exists) {
            switch (ifExists) {
                case FAIL:
                    throw new SQLException("表已存在: " + tableName + "（用 IfExists.REPLACE 覆盖 / APPEND 追加）");
                case REPLACE:
                    try (Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate("DROP TABLE " + quotedTable);
                    }
                    createTable(df, conn, quotedTable);
                    break;
                case APPEND:
                    break;   // 不动表结构
            }
        }
        else {
            createTable(df, conn, quotedTable);
        }
        batchInsert(df, conn, quotedTable, batchSize);
    }

    private boolean tableExists(final Connection conn, final String tableName) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getTables(
                conn.getCatalog(), conn.getSchema(), tableName, null)) {
            return rs.next();
        }
        catch (SQLException ignored) {
            // 某些驱动对 schema/catalog 处理不一致，回退 SELECT 探测
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1 FROM " + dialect.quoteIdentifier(tableName) + " WHERE 1=0");
                return true;
            }
            catch (SQLException e) {
                return false;
            }
        }
    }

    // ┌─ What : 用方言 DDL 自动建表
    // │  Why  : 空 DataFrame（0 行）时 df.types() 返回空 list，无法推断列类型；
    // │         用 String.class 兜底（保证建表成功，写入数据后再由 JDBC 推断）。
    // │         types 大小小于 columns 时同样兜底（防御性，理论上不应发生）
    private <V> void createTable(final DataFrame<V> df, final Connection conn, final String quotedTable)
    throws SQLException {
        final List<Object> columns = new ArrayList<>(df.columns());
        final List<Class<?>> types = df.types();
        final List<Dialect.ColumnDef> colDefs = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            // 类型缺失时（空 df 或 types 不全）用 String 兜底，保证建表成功
            final Class<?> type = i < types.size() ? types.get(i) : String.class;
            colDefs.add(new Dialect.ColumnDef(String.valueOf(columns.get(i)), type));
        }
        final String ddl = dialect.buildCreateTable(quotedTable, true, colDefs);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(ddl);
        }
    }

    // ┌─ What : 批量 INSERT（JDBC batch + PreparedStatement，对标 pandas to_sql method='multi'）
    private <V> void batchInsert(final DataFrame<V> df, final Connection conn,
            final String quotedTable, final int batchSize)
    throws SQLException {
        final int n = df.size();
        if (n == 0) {
            return;
        }
        final StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(quotedTable).append(" VALUES (");
        for (int i = 0; i < n; i++) {
            sb.append(i > 0 ? ",?" : "?");
        }
        sb.append(")");

        final boolean originalAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            try (PreparedStatement stmt = conn.prepareStatement(sb.toString())) {
                int count = 0;
                for (int r = 0; r < df.length(); r++) {
                    for (int c = 0; c < n; c++) {
                        stmt.setObject(c + 1, df.get(r, c));
                    }
                    stmt.addBatch();
                    count++;
                    if (count % batchSize == 0) {
                        stmt.executeBatch();
                        conn.commit();
                    }
                }
                if (count % batchSize != 0) {
                    stmt.executeBatch();
                    conn.commit();
                }
            }
        }
        finally {
            conn.setAutoCommit(originalAutoCommit);
        }
    }
}
