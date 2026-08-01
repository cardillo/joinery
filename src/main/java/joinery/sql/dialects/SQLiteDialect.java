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
 * ┌─ What : SQLite 方言
 * │  Why  : SQLite 是轻量嵌入式库，类型亲和（affinity）与标准 SQL 不同；
 * │         只分 INTEGER/REAL/TEXT/BLOB/NUMERIC 五大类，无 VARCHAR(n) 的 n 强制
 * │  Who  : Engine.fromUrl("jdbc:sqlite:...") 自动选用
 * │  When : 读写在真实 SQLite 文件 / :memory: 时
 */
public class SQLiteDialect extends AbstractDialect {

    @Override
    public boolean matches(final String jdbcUrl) {
        return jdbcUrl != null && jdbcUrl.startsWith("jdbc:sqlite:");
    }

    @Override
    public String driverClassName() {
        return "org.sqlite.JDBC";
    }

    @Override
    public String sqlType(final Class<?> javaType) {
        // SQLite 用亲和类型（INTEGER/REAL/TEXT/BLOB），其他细节交给运行时动态推断
        final Class<?> cls = safeType(javaType);
        if (String.class.equals(cls)) {
            return "TEXT";
        }
        if (Boolean.class.equals(cls) || Boolean.TYPE.equals(cls)
                || Byte.class.equals(cls) || Byte.TYPE.equals(cls)
                || Short.class.equals(cls) || Short.TYPE.equals(cls)
                || Integer.class.equals(cls) || Integer.TYPE.equals(cls)
                || Long.class.equals(cls) || Long.TYPE.equals(cls)) {
            return "INTEGER";
        }
        if (Float.class.equals(cls) || Float.TYPE.equals(cls)
                || Double.class.equals(cls) || Double.TYPE.equals(cls)) {
            return "REAL";
        }
        if (isDateType(cls)) {
            return "TEXT";   // SQLite 无原生日期类型，存 ISO-8601 字符串
        }
        if (byte[].class.equals(cls)) {
            return "BLOB";
        }
        return "TEXT";
    }

    @Override
    public String quoteIdentifier(final String name) {
        return "\"" + name + "\"";   // SQLite 用双引号
    }

    @Override
    public boolean supportsIfNotExists() {
        return true;
    }
}
