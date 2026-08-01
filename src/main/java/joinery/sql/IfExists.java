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

/**
 * ┌─ What : writeSqlTable 的 if_exists 策略枚举（对标 Python pandas.DataFrame.to_sql 的 if_exists）
 * │  Why  : 不同场景对「目标表已存在」的处理诉求不同，提供 3 种可控行为
 * │  Who  : Engine.writeTable / DataFrame.writeSqlTable 调用
 * │  When : 写数据库表前
 */
public enum IfExists {
    /** 表已存在则抛 SQLException（pandas 默认行为） */
    FAIL,
    /** 表已存在则先 DROP TABLE 再 CREATE，覆盖原数据与结构 */
    REPLACE,
    /** 表已存在则只追加数据，不动表结构 */
    APPEND
}
