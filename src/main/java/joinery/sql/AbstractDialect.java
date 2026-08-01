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

import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ┌─ What : 方言的默认实现基类（模板方法模式）
 * │  Why  : 各方言共享 Java→JDBC 类型映射、ANSI SQL 类型名、buildCreateTable 拼接逻辑；
 * │         具体方言只需覆盖差异部分（quoteIdentifier / sqlType / supportsIfNotExists 等）
 * │  Who  : H2Dialect/MySQLDialect/.../DorisDialect 继承本类
 * │  When : Engine 调用方言方法时
 * │  Where: joinery.sql.AbstractDialect
 */
public abstract class AbstractDialect implements Dialect {

    // ┌─ What : Java 类型 → JDBC Types 常量映射（方言无关的默认表）
    // │  Why  : 各方言的 sqlType() 默认走这张表，特殊覆盖在自己的 sqlType 里做
    private static final Map<Class<?>, Integer> JAVA_TO_JDBC = new HashMap<>();
    static {
        JAVA_TO_JDBC.put(String.class, Types.VARCHAR);
        JAVA_TO_JDBC.put(Boolean.class, Types.BOOLEAN);
        JAVA_TO_JDBC.put(Boolean.TYPE, Types.BOOLEAN);
        JAVA_TO_JDBC.put(Byte.class, Types.TINYINT);
        JAVA_TO_JDBC.put(Byte.TYPE, Types.TINYINT);
        JAVA_TO_JDBC.put(Short.class, Types.SMALLINT);
        JAVA_TO_JDBC.put(Short.TYPE, Types.SMALLINT);
        JAVA_TO_JDBC.put(Integer.class, Types.INTEGER);
        JAVA_TO_JDBC.put(Integer.TYPE, Types.INTEGER);
        JAVA_TO_JDBC.put(Long.class, Types.BIGINT);
        JAVA_TO_JDBC.put(Long.TYPE, Types.BIGINT);
        JAVA_TO_JDBC.put(Float.class, Types.FLOAT);
        JAVA_TO_JDBC.put(Float.TYPE, Types.FLOAT);
        JAVA_TO_JDBC.put(Double.class, Types.DOUBLE);
        JAVA_TO_JDBC.put(Double.TYPE, Types.DOUBLE);
        JAVA_TO_JDBC.put(Date.class, Types.TIMESTAMP);
        JAVA_TO_JDBC.put(java.sql.Date.class, Types.DATE);
        JAVA_TO_JDBC.put(java.sql.Timestamp.class, Types.TIMESTAMP);
        JAVA_TO_JDBC.put(byte[].class, Types.VARBINARY);
    }

    @Override
    public int defaultJdbcType(final Class<?> cls) {
        if (cls == null) {
            return Types.VARCHAR;
        }
        final Integer t = JAVA_TO_JDBC.get(cls);
        return t == null ? Types.VARCHAR : t;
    }

    @Override
    public String ansiSqlName(final int jdbcType) {
        switch (jdbcType) {
            case Types.VARCHAR:      return "VARCHAR(255)";
            case Types.NVARCHAR:     return "NVARCHAR(255)";
            case Types.BOOLEAN:      return "BOOLEAN";
            case Types.TINYINT:      return "TINYINT";
            case Types.SMALLINT:     return "SMALLINT";
            case Types.INTEGER:      return "INTEGER";
            case Types.BIGINT:       return "BIGINT";
            case Types.FLOAT:        return "FLOAT";
            case Types.DOUBLE:       return "DOUBLE";
            case Types.REAL:         return "REAL";
            case Types.DECIMAL:      return "DECIMAL(20,6)";
            case Types.NUMERIC:      return "NUMERIC(20,6)";
            case Types.DATE:         return "DATE";
            case Types.TIMESTAMP:    return "TIMESTAMP";
            case Types.VARBINARY:    return "VARBINARY(255)";
            case Types.CLOB:         return "CLOB";
            case Types.BLOB:         return "BLOB";
            default:                 return "VARCHAR(255)";
        }
    }

    // ┌─ What : 默认建表 DDL 拼接（CREATE [TABLE IF NOT EXISTS] name (col type, ...)）
    // │  How  : 伪代码：
    //            1. CREATE TABLE
    //            2. if ifNotExists && supportsIfNotExists() → 拼 "IF NOT EXISTS"
    //            3. 拼表名（已转义）
    //            4. 拼列定义：(col_name sql_type, ...) —— 列名走 quoteIdentifier，类型走 sqlType
    @Override
    public String buildCreateTable(final String table, final boolean ifNotExists, final List<ColumnDef> columns) {
        final StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (ifNotExists && supportsIfNotExists()) {
            sb.append("IF NOT EXISTS ");
        }
        sb.append(table).append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            final ColumnDef col = columns.get(i);
            sb.append(quoteIdentifier(col.name)).append(" ").append(sqlType(safeType(col.type)));
        }
        sb.append(")");
        return sb.toString();
    }
}
