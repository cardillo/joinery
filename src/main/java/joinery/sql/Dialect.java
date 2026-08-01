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

import java.util.Date;
import java.util.List;

/**
 * ┌─ What : SQL 方言策略接口（对标 Python SQLAlchemy 的 Dialect 抽象）
 * │  Why  : 不同数据库（PG/Oracle/MySQL/SQLite/Doris/H2）在以下方面有差异：
 * │           - JDBC 驱动类名
 * │           - Java 类型 → SQL 类型映射（如 Oracle 用 NUMBER，PG 用 INTEGER/NUMERIC）
 * │           - 标识符转义（MySQL `name`、PG "name"、SQL Server [name]）
 * │           - DDL 语法（IF EXISTS / IF NOT EXISTS、自增主键写法）
 * │         通过 Dialect 接口把差异抽象掉，让 Engine 只面向统一接口（开闭原则）。
 * │  Who  : Engine.fromUrl(jdbcUrl) 按 URL 选具体 Dialect 实现
 * │  When : 连接建立、读写表时
 * │  Where: joinery.sql.Dialect（接口），具体实现在 joinery.sql.dialects.*
 *
 *  设计模式：Strategy（方言是可替换的策略）+ Factory（Engine.fromUrl 按 URL 选用哪个策略）
 */
public interface Dialect {

    /**
     * 判断此方言是否匹配给定的 JDBC URL（按 URL 前缀/端口识别）。
     * Engine.fromUrl 用此方法在已注册方言里挑出唯一匹配。
     */
    boolean matches(String jdbcUrl);

    /**
     * 此方言对应的 JDBC 驱动类全名。
     * 调用方需保证该类在 classpath（joinery 主工程不引入具体驱动，由使用方按需提供）。
     */
    String driverClassName();

    /**
     * 把 Java 类型映射为此方言下的 SQL 列类型（用于自动建表 DDL）。
     * 例：OracleDialect.mapType(Long.class) → "NUMBER(19)"；PostgreSQLDialect → "BIGINT"。
     */
    String sqlType(Class<?> javaType);

    /**
     * 标识符转义（表名、列名）。
     * 例：MySQLDialect.quoteIdentifier("user") → "`user"`；
     *     PostgreSQLDialect.quoteIdentifier("user") → "\"user\""。
     */
    String quoteIdentifier(String name);

    /**
     * 构造建表 DDL（CREATE TABLE ...）。
     *
     * @param table 表名（已转义）
     * @param ifNotExists 是否加 IF NOT EXISTS 子句（方言不支持时返回 false 详见 {@link #supportsIfNotExists}）
     * @param columns 列定义（列名 + Java 类型），按顺序生成列定义子句
     * @return 完整的 CREATE TABLE SQL
     */
    String buildCreateTable(String table, boolean ifNotExists, List<ColumnDef> columns);

    /**
     * 此方言是否支持 CREATE TABLE IF NOT EXISTS 语法（Oracle 老版本不支持）。
     */
    boolean supportsIfNotExists();

    /** 列定义：列名 + Java 类型（DDL 生成时按方言翻译为 SQL 类型）。 */
    final class ColumnDef {
        public final String name;
        public final Class<?> type;
        public ColumnDef(final String name, final Class<?> type) {
            this.name = name;
            this.type = type;
        }
    }

    // ───────────────────────── 默认实现工具（被各具体方言复用） ─────────────────────────

    /**
     * 默认 Java→JDBC 类型映射（基于 java.sql.Types，方言可在 sqlType 里覆盖）。
     * 参见 SQLAlchemy 的 types.py 反向映射设计。
     */
    int defaultJdbcType(Class<?> cls);

    /** 把 java.sql.Types 常量转 ANSI SQL 类型名（VARCHAR/INTEGER/...）。 */
    String ansiSqlName(int jdbcType);

    /** 默认工具：SQL 类型名（被各 Dialect 的 sqlType 调用）。 */
    default String defaultSqlType(final Class<?> cls) {
        return ansiSqlName(defaultJdbcType(cls));
    }

    /** 工具：null 安全的 Java 类型判定（null 视为 String）。 */
    default Class<?> safeType(final Class<?> cls) {
        return cls == null ? String.class : cls;
    }

    /** 工具：Date 既是 java.util.Date 也含 java.sql.Date/Timestamp（方言映射时统一处理）。 */
    default boolean isDateType(final Class<?> cls) {
        return cls != null && Date.class.isAssignableFrom(cls);
    }
}
