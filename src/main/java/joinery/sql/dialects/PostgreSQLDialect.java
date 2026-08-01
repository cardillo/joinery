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
 * ┌─ What : PostgreSQL 方言
 * │  Who  : Engine.fromUrl("jdbc:postgresql://...") 自动选用
 */
public class PostgreSQLDialect extends AbstractDialect {

    @Override
    public boolean matches(final String jdbcUrl) {
        return jdbcUrl != null
                && (jdbcUrl.startsWith("jdbc:postgresql:")
                    || jdbcUrl.startsWith("jdbc:postgres:"));
    }

    @Override
    public String driverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String sqlType(final Class<?> javaType) {
        // PG 用 TEXT 替代 VARCHAR(255) 更地道；数值沿用 ANSI
        final Class<?> cls = safeType(javaType);
        if (String.class.equals(cls)) {
            return "TEXT";
        }
        return defaultSqlType(cls);
    }

    @Override
    public String quoteIdentifier(final String name) {
        return "\"" + name + "\"";   // PG 用双引号
    }

    @Override
    public boolean supportsIfNotExists() {
        return true;
    }
}
