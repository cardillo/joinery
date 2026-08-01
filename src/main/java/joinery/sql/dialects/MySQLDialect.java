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
 * ┌─ What : MySQL 方言（同时是 Doris 的基类 —— Doris 兼容 MySQL 协议）
 * │  Why  : MySQL 是最常用的开源 OLTP 库；标识符用反引号转义、类型名略有差异
 * │  Who  : Engine.fromUrl("jdbc:mysql://...") 自动选用；DorisDialect 继承本类
 */
public class MySQLDialect extends AbstractDialect {

    @Override
    public boolean matches(final String jdbcUrl) {
        if (jdbcUrl == null) {
            return false;
        }
        // jdbc:mysql://host:3306/... 或 jdbc:mysql://host:9030/...（Doris）
        // DorisDialect.matches 会优先匹配端口 9030，本类匹配其他 3306 / 默认
        if (!jdbcUrl.startsWith("jdbc:mysql:")) {
            return false;
        }
        // 若是 Doris（端口 9030 或显式标记），让 DorisDialect 处理
        final int port = extractPort(jdbcUrl);
        return port != 9030;
    }

    /** 子类（DorisDialect）可覆盖以改变端口识别 */
    protected int extractPort(final String jdbcUrl) {
        // 形如 jdbc:mysql://host:9030/db
        try {
            final int colon = jdbcUrl.indexOf("://") + 3;
            final int portColon = jdbcUrl.indexOf(':', colon);
            if (portColon < 0) {
                return -1;
            }
            int end = portColon + 1;
            while (end < jdbcUrl.length() && Character.isDigit(jdbcUrl.charAt(end))) {
                end++;
            }
            return Integer.parseInt(jdbcUrl.substring(portColon + 1, end));
        }
        catch (Exception e) {
            return -1;
        }
    }

    @Override
    public String driverClassName() {
        return "com.mysql.cj.jdbc.Driver";   // MySQL Connector/J 8.x+
    }

    @Override
    public String sqlType(final Class<?> javaType) {
        // MySQL 没有 BOOLEAN 字面量（映射到 TINYINT(1)）；其余沿用 ANSI 默认
        final Class<?> cls = safeType(javaType);
        if (Boolean.class.equals(cls) || Boolean.TYPE.equals(cls)) {
            return "TINYINT(1)";
        }
        return defaultSqlType(cls);
    }

    @Override
    public String quoteIdentifier(final String name) {
        return "`" + name + "`";   // MySQL 用反引号
    }

    @Override
    public boolean supportsIfNotExists() {
        return true;
    }
}
