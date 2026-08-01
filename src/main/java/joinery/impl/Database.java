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

package joinery.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import joinery.DataFrame;
import joinery.sql.Dialect;
import joinery.sql.Engine;
import joinery.sql.IfExists;

/**
 * ┌─ What : 旧版 Connection 风格 SQL API（保留以向后兼容）
 * │  Why  : 原 DataFrame.readSqlTable(Connection,...)/writeSqlTable(Connection,...) 入口仍在使用，
 * │         本类作为薄包装，内部全部委托新 joinery.sql.Engine（用通用 H2Dialect 兜底，因 Connection
 * │         不带 URL 信息，无法自动识别方言；如需方言感知请用 DataFrame.sqlEngine(url)）
 * │  Who  : 由 DataFrame 的 Connection 重载方法调用
 * │  When : 用户使用旧 Connection API 时
 * │  Where: 本类
 *
 *  注：DataTypeMapper / IfExists 已迁至 joinery.sql 包（DataTypeMapper 拆入 AbstractDialect，
 *       IfExists 改名 IfExists 保留同名枚举常量 FAIL/REPLACE/APPEND）。
 */
public class Database {

    // ┌─ What : 通用方言（Connection API 不带 URL，无法自动识别，统一用 ANSI 默认）
    private static final Dialect GENERIC = new joinery.sql.dialects.H2Dialect();

    /**
     * 按表名直读全表为 DataFrame（对标 pandas.read_sql_table）。
     * 强类型读取：按 ResultSetMetaData 列类型保留原生 Java 类型（INTEGER→Long、DOUBLE→Double、
     * TIMESTAMP→Date），中文字符串 VARCHAR 走 getString，UTF-8 原生支持。
     */
    public static DataFrame<Object> readSqlTable(final Connection conn, final String tableName)
    throws SQLException {
        return readSqlQuery(conn, "SELECT * FROM " + tableName);
    }

    /**
     * 执行 SQL 查询并按列原类型读取（强类型版 readSql）。
     */
    public static DataFrame<Object> readSqlQuery(final Connection conn, final String sql)
    throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                return Engine.readResultSet(rs);
            }
        }
    }

    /**
     * 把 DataFrame 写入数据库表（对标 pandas.to_sql）。
     * 自动按列类型建表/覆盖/追加，并批量插入（用通用 ANSI 方言建表）。
     */
    public static <V> void writeSqlTable(final DataFrame<V> df, final Connection conn,
            final String tableName, final IfExists ifExists, final int batchSize)
    throws SQLException {
        // 用一个临时 Engine 包装连接走统一的 writeTableWithConnection 流程
        // Engine 构造器是包级可见，用 fromUrl 拿一个再调 writeTableWithConnection
        // 但 Engine.connection() 会建新连接——此处我们要用调用方的现成连接
        // 所以直接走 Engine.writeTableWithConnection 的等价私有逻辑：内联委托
        writeTableWithDialect(df, conn, tableName, ifExists, batchSize, GENERIC);
    }

    // ┌─ What : 用指定方言把 DataFrame 写入连接（Connection API + 方言感知）
    // │  Why  : 给那些已持有 Connection 但想用具体方言的调用方（如 Oracle 连接想用 OracleDialect 建表）
    static <V> void writeTableWithDialect(final DataFrame<V> df, final Connection conn,
            final String tableName, final IfExists ifExists, final int batchSize, final Dialect dialect)
    throws SQLException {
        final String quotedTable = dialect.quoteIdentifier(tableName);
        final boolean exists = tableExists(conn, tableName);

        if (exists) {
            switch (ifExists) {
                case FAIL:
                    throw new SQLException("表已存在: " + tableName + "（若需覆盖请用 IfExists.REPLACE）");
                case REPLACE:
                    try (Statement stmt = conn.createStatement()) {
                        stmt.executeUpdate("DROP TABLE " + quotedTable);
                    }
                    createTable(df, conn, quotedTable, dialect);
                    break;
                case APPEND:
                    break;
            }
        }
        else {
            createTable(df, conn, quotedTable, dialect);
        }
        batchInsert(df, conn, quotedTable, batchSize);
    }

    private static boolean tableExists(final Connection conn, final String tableName)
    throws SQLException {
        try (ResultSet rs = conn.getMetaData().getTables(
                conn.getCatalog(), conn.getSchema(), tableName, null)) {
            return rs.next();
        }
        catch (SQLException ignored) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery("SELECT 1 FROM " + tableName + " WHERE 1=0");
                return true;
            }
            catch (SQLException e) {
                return false;
            }
        }
    }

    private static <V> void createTable(final DataFrame<V> df, final Connection conn,
            final String quotedTable, final Dialect dialect)
    throws SQLException {
        final List<Object> columns = new ArrayList<>(df.columns());
        final List<Class<?>> types = df.types();
        final List<Dialect.ColumnDef> colDefs = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            // 空 df 时 types 为空，用 String 兜底（与 Engine.createTable 一致）
            final Class<?> type = i < types.size() ? types.get(i) : String.class;
            colDefs.add(new Dialect.ColumnDef(String.valueOf(columns.get(i)), type));
        }
        final String ddl = dialect.buildCreateTable(quotedTable, true, colDefs);
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(ddl);
        }
    }

    private static <V> void batchInsert(final DataFrame<V> df, final Connection conn,
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
            try (java.sql.PreparedStatement stmt = conn.prepareStatement(sb.toString())) {
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
