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

package joinery.sql.dialects;

import joinery.sql.AbstractDialect;

/**
 * ┌─ What : Oracle 方言
 * │  Why  : Oracle 类型与 ANSI 差异大（NUMBER 代替所有数值、VARCHAR2 代替 VARCHAR、DATE 含时间）；
 * │         标识符默认大写；老版本不支持 IF NOT EXISTS
 * │  Who  : Engine.fromUrl("jdbc:oracle:thin:@...") 自动选用
 */
public class OracleDialect extends AbstractDialect {

    @Override
    public boolean matches(final String jdbcUrl) {
        return jdbcUrl != null && jdbcUrl.startsWith("jdbc:oracle:");
    }

    @Override
    public String driverClassName() {
        return "oracle.jdbc.OracleDriver";
    }

    @Override
    public String sqlType(final Class<?> javaType) {
        // Oracle 用 NUMBER(p) 表达整数、NUMBER(p,s) 表达小数；VARCHAR2 替代 VARCHAR
        final Class<?> cls = safeType(javaType);
        if (String.class.equals(cls)) {
            return "VARCHAR2(255)";
        }
        if (Boolean.class.equals(cls) || Boolean.TYPE.equals(cls)) {
            return "NUMBER(1)";   // Oracle 无 BOOLEAN，用 NUMBER(1) 模拟
        }
        if (Byte.class.equals(cls) || Byte.TYPE.equals(cls)
                || Short.class.equals(cls) || Short.TYPE.equals(cls)) {
            return "NUMBER(5)";
        }
        if (Integer.class.equals(cls) || Integer.TYPE.equals(cls)) {
            return "NUMBER(10)";
        }
        if (Long.class.equals(cls) || Long.TYPE.equals(cls)) {
            return "NUMBER(19)";
        }
        if (Float.class.equals(cls) || Float.TYPE.equals(cls)
                || Double.class.equals(cls) || Double.TYPE.equals(cls)) {
            return "NUMBER(19,4)";
        }
        if (isDateType(cls)) {
            return "TIMESTAMP";   // Oracle 的 DATE 含时间，但 TIMESTAMP 更通用
        }
        return defaultSqlType(cls);
    }

    @Override
    public String quoteIdentifier(final String name) {
        // Oracle 标识符默认全大写；带双引号则保留大小写（为支持中文列名，统一带双引号）
        return "\"" + name + "\"";
    }

    @Override
    public boolean supportsIfNotExists() {
        return false;   // Oracle 老版本不支持 CREATE TABLE IF NOT EXISTS（21c+ 才支持）
    }
}
