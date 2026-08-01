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

/**
 * ┌─ What : Apache Doris 方言
 * │  Why  : Doris 完全兼容 MySQL 协议（官方推荐用 MySQL Connector/J 接入，默认端口 9030）；
 * │         因此继承 MySQLDialect，仅覆盖端口识别与少量差异
 * │  Who  : Engine.fromUrl("jdbc:mysql://host:9030/...") 自动选用
 * │  When : 连接 Doris 时
 * │  How  : 复用 MySQLDialect 的 driverClassName / sqlType / quoteIdentifier；
 * │         仅 matches() 改为识别端口 9030（Doris FE 默认查询端口）
 */
public class DorisDialect extends MySQLDialect {

    @Override
    public boolean matches(final String jdbcUrl) {
        if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:mysql:")) {
            return false;
        }
        // 端口 9030（Doris FE 默认）→ Doris；也可被 ?dialect=doris 参数显式标记
        final int port = extractPort(jdbcUrl);
        return port == 9030 || jdbcUrl.toLowerCase().contains("dialect=doris");
    }

    // 其余完全复用 MySQLDialect（Doris 协议层与 MySQL 一致）
}
