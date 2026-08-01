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
 * ┌─ What : H2 数据库方言（嵌入式/服务端，常用于测试与开发，支持 MODE 模拟其他方言）
 * │  Why  : H2 是 joinery dataio-tests 的主力库，且可通过 MODE=Oracle/MySQL/PostgreSQL 模拟其他方言
 * │  Who  : Engine.fromUrl("jdbc:h2:...") 自动选用
 * │  When : 连接 H2 时
 */
public class H2Dialect extends AbstractDialect {

    @Override
    public boolean matches(final String jdbcUrl) {
        return jdbcUrl != null && jdbcUrl.startsWith("jdbc:h2:");
    }

    @Override
    public String driverClassName() {
        return "org.h2.Driver";
    }

    @Override
    public String sqlType(final Class<?> javaType) {
        return defaultSqlType(javaType);   // H2 兼容 ANSI 类型
    }

    @Override
    public String quoteIdentifier(final String name) {
        return "\"" + name + "\"";   // H2/ANSI 用双引号
    }

    @Override
    public boolean supportsIfNotExists() {
        return true;
    }
}
