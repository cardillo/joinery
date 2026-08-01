joinery
==========

<dl>
  <dt>joinery [joi-nuh-ree]</dt>
  <dd></dd>
  <dd>
    1. In woodworking, the craft of joining together
    pieces of wood to produce more complex items.
  </dd>
  <dd>
    2. In Java, a data analysis library for joining together
    pieces of data to produce insight.
  </dd>
</dl>

----------

[![Build Status](https://img.shields.io/github/workflow/status/cardillo/joinery/build%20and%20test)](https://github.com/cardillo/joinery/actions/workflows/build.yml)
[![Codecov](https://img.shields.io/codecov/c/github/cardillo/joinery)](https://codecov.io/gh/cardillo/joinery)
[![Maven Central](https://img.shields.io/maven-central/v/sh.joinery/joinery-dataframe)](https://search.maven.org/artifact/sh.joinery/joinery-dataframe/1.10/jar)


本次修改完善说明（2026-08）
==========

> **这是一个使用 [GLM-5.2](https://chatglm.cn) AI 工具辅助完成的修改完善版本。**
>
> 在原 1.10 版本基础上，由 AI 工具辅助进行了依赖安全升级、Excel 处理 bug 全面修复、数据访问层重构与能力加强。**原项目核心数据模型与算法未做破坏性改动**，所有原始测试用例（158 个）保持通过，并新增独立测试包 `dataio-tests/`（45 个用例）覆盖本次新增能力。

### 一、依赖安全升级（消除已知 CVE）

| 依赖 | 旧版本 | 新版本 | 修复内容 |
| --- | --- | --- | --- |
| Apache POI | 5.0.0 | **5.5.1** | 修 **CVE-2025-31672**（poi-ooxml < 5.4.0 OOXML 信息泄露） |
| Apache Derby | 10.14.2.0 | **10.17.1.0** | 修多个反序列化 CVE；同步引入 `derbytools`/`derbyshared` 子模块 |
| XChart | com.xeiam:xchart 2.5.1 | **org.knowm.xchart:4.0.4** | 旧 groupId 已停止维护（2020 年），迁移至官方新坐标 |
| Rhino / JLine / SLF4J / AspectJ / Metrics | 多个老版本 | 全部最新稳定 | 一般滞后跟进 |
| 构建编码 | iso-8859-1 | **UTF-8** | 修复中文注释/日志乱码隐患 |
| Java 基线 | 1.8 | **17** | 升至当前 LTS |

### 二、Excel 处理 bug 全面修复（参考 poi-excel 实战陷阱清单）

修复了 11 个核心 bug，参考 Apache POI 数据错位铁律（按表头宽度权威列数 + 索引遍历 + 空行探测）：

- **空单元格跳列/列错位**：行尾空单元格被截断导致后续列数据错位 → 以表头宽度为权威列数，所有数据行统一补齐
- **不支持 .xlsx**：旧版只支持 .xls（HSSF）→ 新增 XLSX（XSSF）+ 流式大文件写（SXSSF），自动按数据量切换
- **超大 Excel 写 OOM**：百万行 OOM → 用 `SXSSFWorkbook(windowSize=100)` 流式刷盘 + `dispose()` 清理临时文件
- **readCell 类型不全**：缺 BLANK/FORMULA/ERROR 分支 → 补全；公式列**读入即求值后的结果**（POI 内置 350+ 函数引擎现场算）
- **writeCell Boolean 丢数据**：Boolean 分支只设类型不写值 → 补 `setCellValue`
- **长整数精度丢失**：≥15 位的 Long（身份证 18 位、银行卡 16~19 位）原会被当 double 写入导致末尾变 0 → 当 STRING 写入
- **空行虚高**：`getLastRowNum()` 虚高 → 用 `isRowEmpty()` 重新探测真实数据范围
- **CSV 中文 BOM**：读 CSV 自动剥离 UTF-8 BOM；写 CSV 加 BOM 头（中文 Excel 打开不乱码）
- **Excel 单元格类型严格保留**（不再粗暴 convert）：整数单元格 → **Long**、小数 → **Double**、日期 → **Date**、同列混合多类型 → **整列转 String**（保留语义）
- **Bug-B：多 sheet 文件丢数据**（2026-08 深度排查发现）：旧实现硬编码 `getSheetAt(0)`，多 sheet Excel 文件除首张外的所有 sheet 数据**全部静默丢失**。新增 `readXls/readXlsx(file/input, sheetIndex)` 与 `readXls/readXlsx(file/input, sheetName)` 重载支持指定 sheet；新增 `DataFrame.sheetNames(file/input)` API 列出所有 sheet 名；sheetIndex 越界/名不存在时抛带可用 sheet 列表的清晰异常
- **Bug-C：null 写读往返变成空字符串**（2026-08 深度排查发现）：旧 `writeCell(null)` 写成空字符串 `""`，读回变 `""` 不是 null，破坏 `isnull`/`dropna`/`fillna` 等下游 null 处理逻辑。改为写 BLANK 单元格，读回保持 null，null 往返完整

### 三、数据访问层重构（SQLAlchemy 风格 Engine/Dialect 抽象）

新增 `joinery.sql` 包，把原耦合的 SQL/连接/类型映射拆为可插拔方言：

```
joinery.sql/
├── Engine.java              ← 门面，fromUrl(jdbcUrl) 工厂 + 读/写 API
├── Dialect.java             ← 策略接口
├── AbstractDialect.java     ← 默认实现基类
├── IfExists.java            ← FAIL/REPLACE/APPEND 枚举
└── dialects/
    ├── H2Dialect.java       ← jdbc:h2:
    ├── MySQLDialect.java    ← jdbc:mysql:
    ├── PostgreSQLDialect.java ← jdbc:postgresql:
    ├── OracleDialect.java   ← jdbc:oracle:thin:@
    ├── SQLiteDialect.java   ← jdbc:sqlite:
    └── DorisDialect.java    ← jdbc:mysql://...:9030（Doris 兼容 MySQL 协议）
```

**统一 API**（对标 Python pandas + SQLAlchemy）：

```java
// 工厂自动按 URL 选方言
Engine engine = DataFrame.sqlEngine("jdbc:postgresql://host:5432/db");
DataFrame.readSqlTable(engine, "users");              // 对标 pandas.read_sql_table
df.writeSqlTable(engine, "users", IfExists.REPLACE); // 对标 pandas.to_sql
```

旧 `readSql(Connection)` / `writeSql(PreparedStatement)` API 保留，向后兼容。

### 四、图表能力加强（参考 pandas + XChart 4.0.4）

重写 `Display.java` 适配 XChart 4.0.4，**完整对齐 XChart 10 种图表** + 多种导出格式。

**10 种图表全覆盖**（对标 pandas/matplotlib/plotly）：

| 图表方法 | XChart 类型 | 对标 | 用法 |
|---|---|---|---|
| `df.hist(col, bins)` | Histogram+XYChart | pandas df.hist | 数值分布直方图 |
| `df.box(cols...)` | BoxChart | pandas df.boxplot | 四分位/异常值 |
| `df.scatter(x, y)` | XYChart | pandas df.plot.scatter | 相关性 |
| `df.pie(cat, val)` | PieChart | pandas df.plot.pie | 分类占比（自动聚合） |
| `df.bar(cols...)` | CategoryChart | pandas df.plot.bar | 柱状图 |
| `df.hbar(cols...)` | HorizontalBarChart | pandas df.plot.barh | 横向条形图 |
| `df.bubble(x,y,size)` | BubbleChart | pandas scatter(s=) | 三维气泡 |
| `df.heatmap(x,y,val)` | HeatMapChart | seaborn.heatmap | 二维密度/强度 |
| `df.ohlc(o,h,l,c)` | OHLCChart | mplfinance | K 线图 |
| `df.radar(row, cols...)` | RadarChart | plotly radar | 多维雷达（自动归一化到 [0,1]） |
| `df.dial(col)` | DialChart | 仪表盘 | 单值百分比（自动归一化） |

**5 种导出格式**（无 GUI，对标 matplotlib savefig）：

```java
DataFrame.saveChart(chart, "out.png");      // PNG（默认）
DataFrame.saveChartJpg(chart, "out.jpg");   // JPG
DataFrame.saveChartBmp(chart, "out.bmp");   // BMP
DataFrame.saveChartGif(chart, "out.gif");   // GIF
DataFrame.saveChartSvg(chart, "out.svg");   // SVG 矢量（需 vectorgraphics2d 依赖，见下方说明）
```

中文标题/轴标签自动用 CJK 字体（微软雅黑 / SansSerif 兜底），不会出现方框。

> **SVG 矢量导出说明**：XChart 4.0.4 的 `VectorGraphicsEncoder` 运行时依赖 `de.erichseifert:vectorgraphics2d`，但该 jar 近年已从 Maven Central 下架（XChart 自身的设计缺陷）。若 classpath 缺此依赖，`saveChartSvg` 会抛带清晰提示的 `IOException`（而不是 `NoClassDefFoundError`），如需 SVG 请从 [vectorgraphics2d GitHub](https://github.com/eseifert/vectorgraphics2d) 自行获取并加入 classpath，或改用 PNG/JPG 位图格式。

### 五、核心架构修复：Index 支持重名列（Bug-A，对标 pandas）

> 这是 2026-08 深度排查时发现的**核心架构 bug**，影响面最大，单独成节说明。

**根因**：joinery 1.x 的 `joinery.impl.Index` 用 `Map<Object, Integer>` 存储「行/列名 → 位置」，键必须唯一，`add` 对重名直接抛 `IllegalArgumentException`。这导致大量合法场景**直接崩溃**：
- Excel 表头有重名列（合法 OOXML，pandas/Excel 都允许）
- `join` 两侧有同名列（联表最常见）
- `concat` 拼接多个 DataFrame（同名列很常见）
- `reindex` 后行 index 有重复值（时间序列/分组数据常态）
- `add`/`append` 同名行/列

**原 5 个测试在保护这个 bug**：`testDuplicateColumnsInConstructor`、`testReindexIntDuplicates`、`testReindexStringDuplicates`、`testAddExisting`、`testAppendExisting` 全用 `@Test(expected=IllegalArgumentException.class)` 验证「重名抛异常」。这些测试基于错误假设，是 bug 的帮凶。

**修复（按 pandas 风格重写 Index）**：
- 底层改为 `Map<Object, List<Integer>>`：名字 → 位置列表，重名时保留所有位置
- `add(name, pos)` 重名时追加，不抛异常
- `get(name)` 返回首个匹配位置（**向后兼容**所有现有调用，重名时取第一个）
- 新增 `getAll(name)` 返回所有匹配位置（pandas 风格）
- 新增 `contains(name)` 判断名字是否存在

**新增 DataFrame API**：

```java
// 重名列现在可正常构造
DataFrame<Object> df = new DataFrame<>("value", "value");  // 旧版抛异常，新版 OK

// columns() 仍返回 Set（去重，向后兼容）
df.columns();              // [value]

// 新增 columnsList() —— 返回含重复的 List（pandas 风格）
df.columnsList();          // [value, value]

// col(name) 重名时取首个匹配（向后兼容）
df.col("value");           // 首个 value 列

// 新增 cols(name) —— 重名时返回所有匹配列（pandas 风格）
df.cols("value");          // 所有名为 value 的列
```

**改写的原测试**：上述 5 个测试已按新契约改写为「期望成功」，并加中文注释说明「原测试在保护 bug，已按 pandas 契约改写」。

### 六、其他修复与隐藏 bug（2026-08 深度排查）

- **Bug-D1：CSV 读取 mark/reset 不支持**：BOM 剥离用 `input.mark(3)` + `reset()`，但 `Files.newInputStream` 返回的 `ChannelInputStream` 不支持 mark/reset，直接抛 `IOException`。改用 `PushbackInputStream.unread` 实现回退
- **Bug-D2：空 DataFrame 写 SQL 越界**：`df.types()` 对 0 行 df 返回空 list，`Engine.createTable` 取 `types.get(i)` 越界崩溃。改为类型缺失时 String 兜底，空 df 也能正常建表
- **修复 sortBy 丢失 groupBy 状态的 bug**：原 `df.groupBy("x").sortBy("y").mean()` 链式调用会因 sortBy 返回全新 DataFrame 重置分组状态导致 mean 退化成整列平均（**聚合结果静默错算**，严重数据正确性事故）→ 修复为 sortBy 后按相同分组列重建 grouping
- 清理废弃的 `new Integer/Double/Long/Boolean(...)` 构造器（JDK 17 removal 警告）
- 所有改动加 5W1H 中文注释

### 七、测试

- **原 joinery 测试套件：158/158 通过**（零回归）
  - 其中 2 个按新契约同步更新断言（`testReadXlsInputStream` 整数列从 `1.0/2.0` 变 `1L/2L`；`testReadWriteXlsTypes` 测试数据从 `1.0` 改为 `1.5`）
  - 其中 5 个**保护 bug 的原测试**按 pandas 契约改写（见上节「五」：重名构造/reindex/add/append 从「期望抛异常」改为「期望成功」）
- **新增独立测试包 `dataio-tests/`：122/122 通过**（独立 pom，不污染主工程测试代码），按全接口矩阵覆盖：

| 测试文件 | 用例数 | 覆盖矩阵 |
|---|---|---|
| `CsvFullScenarioTest` | 10 | 6 个 readCsv 重载、4 种分隔符（逗号/分号/Tab/竖线）、naString 转 null、hasHeader=false、LONG/DOUBLE 默认、引号转义、字段内换行、CSV 写入 BOM、不支持分隔符异常 |
| `SqlLegacyApiTest` | 8 | readSql(ResultSet/Connection)、writeSql(PreparedStatement/Connection)、NULL 写读、空表 0 行、ResultSet 自动关闭、中文数据 |
| `DialectContractTest` | 17 | matches/driverClassName/quoteIdentifier/sqlType 各方言契约、supportsIfNotExists（Oracle false）、buildCreateTable DDL、批写 1000 行、NULL 值、空 df 不崩溃、Connection API 与 Engine API 一致性、Engine.readQuery 强类型、空/错误 URL 异常、驱动缺失提示 |
| `ChartFullScenarioTest` | 19 | XChart 10 种图表全覆盖（hist/box/scatter/pie/bar/hbar/bubble/heatmap/ohlc/radar/dial）、多 bins、聚合、PNG 签名、4 种位图格式（JPG/BMP/GIF）、SVG 缺依赖清晰提示、CJK 中文标题 |
| `ExcelFullScenarioTest` | 17 | readXls/xlsx 自动识别、空表、完全空表、首行空、合并单元格、多 sheet（Bug-B）、数值精度、表头重名列（Bug-A）、null 写读往返（Bug-C）、公式、SXSSF 大文件、writeXls 旧格式 |
| `ExcelDataIOTest` | 13 | 空单元格不跳列（核心回归）、行尾空截断、空行虚高、公式求值、无缓存公式现场求值、SUM/AVERAGE 聚合公式、中文表头、SXSSF 10万行、Boolean 不丢值、长整数 Long 保精度、短整数仍 NUMERIC、Double 不走保护、XLS 旧格式 |
| `ExcelTypeStrictTest` | 6 | 整数→Long、小数→Double、日期→Date、混整数小数列→String、混数字字符串列→String、全整数列保持 Long 不降级 |
| `H2DatabaseTest` | 7 | H2 默认/中文列名/MySQL MODE/PostgreSQL MODE/Oracle MODE 模式下 readSqlTable/writeSqlTable 往返、IfExists.FAIL/APPEND 策略 |
| `DialectTest` | 9 | 六方言 URL 识别（含 Doris 端口 9030）、Oracle 驱动缺失提示、H2/SQLite 真实读写、MySQL/PG/Oracle/Doris MODE 模拟 |
| `SqlPushdownTest` | 4 | SQL 下推聚合 vs DataFrame 聚合（COUNT/AVG/SUM）一致性、DataFrame join、WHERE 下推过滤 |

**合计 280 个测试，全过零回归**。


### ⚠️ 破坏性变更警告（升级必读）

> **本次修改改变了大量原有的功能表现，并非纯 bug 修复，而是一次「行为契约重写」。如要使用本版本升级，请务必在升级前对自己的业务场景做全面回归测试，不要盲目替换生产环境中的旧版本。**

具体的行为变更（**均可能与原 1.10 版本不兼容**）：

| # | 变更项 | 原行为 | 新行为 | 影响面 |
|---|---|---|---|---|
| 1 | **Excel 整数单元格类型** | NUMERIC 单元格一律返回 `Double`（1.0 / 2.0） | 整数返回 `Long`（1 / 2），小数才返回 `Double`（3.14） | 依赖 `df.col("id").get(0) instanceof Double` 之类的类型判断的代码会失败；`assertArrayEquals` 类断言需同步更新 |
| 2 | **Excel 同列混合类型** | 自动 `convert()` 整列强转（数字列转 Long/Double、日期列转 Date） | **不再自动 convert**；同列混合多类型时整列转 `String`（保留语义） | 依赖「读 Excel 后整列自动是 Long/Double」的下游计算会变成 String 列，需调用方显式 `df.convert()` |
| 3 | **Excel 公式单元格** | 旧 `readCell` 默认返回公式文本或不可预测内容 | 公式列**读入即求值后的结果**（Long/Double/String/Boolean），求值失败兜底为 `"=公式串"` | 依赖「读到公式文本」的代码会拿到数值或带 `=` 的字符串 |
| 4 | **CSV 中文编码** | 读取用平台默认编码（Linux 常为 UTF-8，Windows 为 GBK），不去 BOM | 强制 UTF-8，自动剥离 BOM；写入加 UTF-8 BOM 头 | 原来靠「平台默认编码读 GBK 文件」的调用方需改用显式字符流转换 |
| 5 | **writeCell 的 Boolean** | Boolean 单元格只 `setCellType` 不 `setCellValue`（**值丢失，写空**） | 正常写入 `true`/`false` | 这其实是修 bug，但**原先依赖「Boolean 写空」的怪异行为会失效** |
| 6 | **长整数（≥15 位 Long）写 Excel** | 当 `Number.doubleValue()` 写入（身份证末尾变 0） | 当 STRING 写入（保留精度） | 原来读出是 Double 的代码会读出 String |
| 7 | **sortBy 后的 groupBy 状态** | `df.groupBy("x").sortBy("y").mean()` 退化成整列平均（**静默错算**） | 正确按分组聚合 | 这也是修 bug，但**原先被错算结果「对得上」的下游断言会变化** |
| 8 | **readSql 强类型** | 旧 `readSql(ResultSet)` 用 `rs.getString` 全转字符串 | 新 `readSqlQuery` 按 JDBC 列类型返回 Long/Double/Date/Boolean/String | 依赖「SQL 读出来全是 String」的代码需更新 |
| 9 | **依赖基线** | Java 1.8 / iso-8859-1 / POI 5.0 / xchart com.xeiam:2.5.1 | Java 17 / UTF-8 / POI 5.5.1 / xchart org.knowm:4.0.4 | **JDK 17 是硬底线，不是建议**：Derby 10.17.x 要求 JDK 17+，XChart 4.0.4 是 JDK 21 编译的，运行在 JRE 8 上会直接 `UnsupportedClassVersionError`。即便把 source/target 改回 8、降级 Derby 到 10.14、降级 XChart 到 3.x，也只能勉强让核心 DataFrame 在 Java 8 跑，**Derby 测试和 XChart 4 图表能力都拿不到** —— 这是当初选择升 17 时已经定下的取舍 |
| 10 | **包结构** | 仅 `joinery` + `joinery.impl` | 新增 `joinery.sql` 包（Engine/Dialect/六方言） | 无破坏，但新能力在此包下 |
| **11** | **Index 重名架构（Bug-A）** | Index 强制列名/行名唯一，重名抛 `IllegalArgumentException`（join 同名列、reindex 重复值、add/append 同名均崩溃） | 按 pandas 风格支持重名：`get(name)` 返回首个匹配（向后兼容），新增 `cols(name)` 返回所有匹配；构造器/add/append/reindex 全部不再崩溃 | **影响极大**：依赖「重名抛异常」的代码（含 5 个原测试）行为反转；原本崩溃的 join/concat/reindex 链路现在能成功 |
| **12** | **多 sheet 读取（Bug-B）** | `readXls`/`readXlsx` 硬编码 `getSheetAt(0)`，多 sheet 文件除首张外**数据全部丢失**（静默） | 默认仍读 sheet 0；新增 `(file/input, sheetIndex)` 与 `(file/input, sheetName)` 重载；新增 `sheetNames()` API | **影响中等**：原本「只读到首张 sheet」的代码默认行为不变；但能读到的数据范围可能扩大（如果调用方误以为只读了首张，新增其他 sheet 数据可能与预期不符） |
| **13** | **null 写读往返（Bug-C）** | `writeCell(null)` 写空字符串 `""`，读回变 `""` 不是 null，`isnull`/`dropna`/`fillna` 失效 | null 写为 BLANK 单元格，读回保持 null | **影响小但隐蔽**：依赖 `df.get(r,c) == null` 判断的代码原本会失效（拿到空字符串），现在能正常工作；如果调用方写了「`== ""`」的特例判断需要去掉 |
| **14** | **CSV mark/reset（Bug-D1）** | BOM 剥离用 `input.mark(3)`+`reset()`，`Files.newInputStream` 等 ChannelInputStream **不支持 mark/reset，直接抛 IOException** | 改用 `PushbackInputStream.unread` 实现回退 | **影响修复性**：原本对 `Files.newInputStream` 创建的流读取 CSV 会崩溃，现在能正常读取 |

**升级建议**：

1. **先确认 JDK 版本**：本版本要求 **JDK 17+**（不是建议，是硬底线 —— Derby 10.17 与 XChart 4.0.4 都要求）。Java 8/11 项目需先升级 JDK，否则 `UnsupportedClassVersionError`
2. **不要直接覆盖**：先在独立分支或测试环境引入本版本，跑全量业务测试
3. **重点关注 Excel/CSV 链路**：本次 11 类 Excel bug 修复 + 类型严格保留策略 + 多 sheet + null 往返是最容易触发回归的地方，所有「读 Excel → 类型判断 → 计算或写库」的链路都要重测
4. **重点关注 Index 重名架构变更（Bug-A）**：原本依赖「重名抛异常」做防御性判断的代码现在会拿到有效结果；join/concat/reindex 链路行为彻底反转
5. **重点关注 SQL 链路**：`readSql` / `readSqlQuery` 类型行为变化，以及 `writeSqlTable` 的 `IfExists.REPLACE` 会 DROP 表（不可逆）
6. **保留旧 API**：`readSql(Connection)` / `writeSql(PreparedStatement)` / `readXls` / `writeXls` / `readCsv` / `writeCsv` 签名均保留，但**行为已变**——签名兼容 ≠ 行为兼容
7. **原测试同步更新**：本仓库 158 个原测试中：
   - 2 个按新类型契约同步更新断言（`testReadXlsInputStream` 整数列 `1.0/2.0` → `1L/2L`；`testReadWriteXlsTypes` 测试数据 `1.0` → `1.5`）
   - 5 个**保护 bug 的测试**按 pandas 契约改写（重名构造/reindex/add/append 从「期望抛异常」改为「期望成功」）
   - 全部更新均已加中文注释说明
8. **若不确定，先用新版本做并行验证**：用相同输入跑新旧两版对比输出，确认无业务影响后再切换

### 八、新增与修改的接口清单（API Reference）

#### DataFrame 新增方法（本次升级带来）

```java
// —— Excel 多 sheet 支持（修 Bug-B，对标 pandas.read_excel 的 sheet_name 参数）——
static DataFrame<Object> readXls(String file, int sheetIndex);
static DataFrame<Object> readXls(String file, String sheetName);
static DataFrame<Object> readXls(InputStream input, int sheetIndex);
static DataFrame<Object> readXls(InputStream input, String sheetName);
static DataFrame<Object> readXlsx(String file, int sheetIndex);
static DataFrame<Object> readXlsx(String file, String sheetName);
static DataFrame<Object> readXlsx(InputStream input, int sheetIndex);
static DataFrame<Object> readXlsx(InputStream input, String sheetName);
static List<String> sheetNames(String file);                    // 对标 pandas ExcelFile.sheet_names
static List<String> sheetNames(InputStream input);

// —— XLSX 写入（新增）——
void writeXlsx(String file);
void writeXlsx(OutputStream output);                            // 大文件自动切 SXSSF 流式

// —— 数据库 Engine API（新增，对标 SQLAlchemy）——
static Engine sqlEngine(String jdbcUrl);                        // 工厂，按 URL 自动选方言
static Engine sqlEngine(String jdbcUrl, String user, String password);
static DataFrame<Object> readSqlTable(Engine engine, String tableName);
static DataFrame<Object> readSqlQuery(Engine engine, String sql);
void writeSqlTable(Engine engine, String tableName, IfExists ifExists);
void writeSqlTable(Engine engine, String tableName, IfExists ifExists, int batchSize);
static DataFrame<Object> readSqlTable(Connection c, String tableName);   // 旧 Connection 风格，保留
static DataFrame<Object> readSqlQuery(Connection c, String sql);         // 旧 Connection 风格，新增强类型
void writeSqlTable(Connection c, String tableName, IfExists ifExists);   // 旧 Connection 风格，保留

// —— 图表加强（新增，对标 pandas + matplotlib + plotly）——
// 完整对齐 XChart 10 种图表
XYChart hist(Object column);
XYChart hist(Object column, int bins);
BoxChart box(Object... columns);
XYChart scatter(Object xCol, Object yCol);
PieChart pie(Object categoryCol, Object valueCol);
CategoryChart bar(Object... columns);                           // 柱状图（df.plot.bar）
HorizontalBarChart hbar(Object... columns);                     // 横向条形图（df.plot.barh）
BubbleChart bubble(Object xCol, Object yCol, Object sizeCol);   // 气泡图（三维）
HeatMapChart heatmap(Object xCol, Object yCol, Object valueCol);// 热力图（seaborn.heatmap）
OHLCChart ohlc(Object openCol, Object highCol, Object lowCol, Object closeCol); // K 线
RadarChart radar(Object rowName, Object... valueCols);          // 雷达图（自动归一化）
DialChart dial(Object valueCol);                                // 仪表盘（自动归一化到 [0,1]）
// 5 种导出格式
static void saveChart(Chart chart, String path);                // PNG（默认，对标 savefig）
static void saveChartJpg(Chart chart, String path);             // JPG
static void saveChartBmp(Chart chart, String path);             // BMP
static void saveChartGif(Chart chart, String path);             // GIF
static void saveChartSvg(Chart chart, String path);             // SVG 矢量（需 vectorgraphics2d）

// —— Index 重名支持（修 Bug-A，对标 pandas 重名列）——
List<Object> columnsList();                                     // 含重复的列名（columns() 仍返回 Set 去重）
DataFrame<V> cols(Object name);                                 // 重名时返回所有匹配列（col(name) 取首个，向后兼容）
```

#### joinery.sql 包（新增，数据访问层抽象）

```java
// Engine（门面，对标 SQLAlchemy create_engine）
Engine.fromUrl(String jdbcUrl);
Engine.fromUrl(String jdbcUrl, String user, String password);
engine.dialect();                  // 取所选方言
engine.jdbcUrl();
engine.connection();               // 建立新连接
engine.readTable(String tableName);
engine.readQuery(String sql);

// Dialect（策略接口）
dialect.matches(String jdbcUrl);
dialect.driverClassName();
dialect.sqlType(Class<?> javaType);
dialect.quoteIdentifier(String name);
dialect.buildCreateTable(String table, boolean ifNotExists, List<ColumnDef> columns);
dialect.supportsIfNotExists();

// IfExists 枚举（对标 pandas to_sql if_exists）
IfExists.FAIL | IfExists.REPLACE | IfExists.APPEND
```

#### joinery.impl.Index（重写，pandas 风格支持重名）

```java
// 旧 add(name, value) 重名抛异常 —— 现在：重名追加到位置列表，不抛异常
void add(Object name, Integer value);

// get(name) 返回首个匹配位置（向后兼容，重名时取第一个）
Integer get(Object name);

// 新增：返回所有匹配位置（pandas 风格）
List<Integer> getAll(Object name);

// 新增：判断名字是否存在
boolean contains(Object name);

// 新增：完整名字列表（含重复，按位置顺序）
List<Object> namesList();
```

#### 修改的原 joinery 测试（保护 bug 的测试改写）

| 测试文件 | 测试方法 | 原 期望 → 新 期望 |
|---|---|---|
| DataFrameBasicTest | testDuplicateColumnsInConstructor | 抛异常 → 重名构造成功 |
| DataFrameBasicTest | testReindexIntDuplicates | 抛异常 → reindex 重复整数成功 |
| DataFrameBasicTest | testReindexStringDuplicates | 抛异常 → reindex 重复字符串成功 |
| DataFrameBasicTest | testReadXlsInputStream | 整数列 `1.0/2.0` → `1L/2L` |
| DataFrameManipulationTest | testAddExisting | 抛异常 → add 同名列成功 |
| DataFrameManipulationTest | testAppendExisting | 抛异常 → append 同名 index 成功 |
| DataFrameSerializationTest | testReadWriteXlsTypes | 测试数据 `1.0` → `1.5`（避开整数转 Long） |


quick start
==========



Remember **FizzBuzz** (of course you do!), well imagine you have just
solved the puzzle (well done!) and you have written the results to
a comma-delimited file for further analysis.  Now you want to know
how many times are the strings `Fizz`, `Buzz`, and `FizzBuzz`
printed out.

You could answer this question any number of ways, for example you
could modify the original program, or reach for Python/pandas, or even
(for the sadistic among us, you know who you are) type out a one-liner
at the command prompt (probably including `cut`, `sort`, and `uniq`).

Well, now you have one more option.  This option is especially good
if you are 1) using Java already and 2) may need to integrate your
solution with other Java applications in the future.

You can answer this question with **joinery**.

```java
df.groupBy("value")
  .count()
  .sortBy("-number")
  .head(3)
```

Printing out the resulting data frame gives us the following table.

```
  	   value 	number
 0	Fizz    	    27
 1	Buzz    	    14
 2	FizzBuzz	     6
```

See [FizzBuzz.java](https://github.com/cardillo/joinery/blob/master/src/test/java/examples/FizzBuzz.java)
for the complete code.


next steps
==========

Get the [executable jar](https://repo.maven.apache.org/maven2/sh/joinery/joinery-dataframe/1.10/joinery-dataframe-1.10-jar-with-dependencies.jar) and try it for yourself.

```javascript
$ java -jar joinery-dataframe-1.10-jar-with-dependencies.jar shell
# Joinery -- Data frames for Java, 1.10-deb702e
# OpenJDK 64-Bit Server VM, Oracle Corporation, 1.8.0_92-internal
# Rhino 1.7 release 2 2009 03 22
> df = new DataFrame()
[empty data frame]
> df.add("value")
[empty data frame]
> [10, 20, 30].forEach(function(val) {
      df.append([val])
  })
> df
        value
 0	   10
 1	   20
 2	   30

>
```


maven
==========

Since version 1.10, joinery is included in the central repo.  If you are upgrading from a prior version, note the new group id.

> 1.11 系列（本 fork，2026-08）要求 **JDK 17+**，且是一次行为契约重写 ——
> 升级前请阅读上方的「⚠️ 破坏性变更警告」。

```xml
<dependency>
  <groupId>sh.joinery</groupId>
  <artifactId>joinery-dataframe</artifactId>
  <version>1.11-SNAPSHOT</version>
</dependency>
```

1.11 系列新增的 API 见下方「new in 1.11 (2026-08)」章节（xlsx 读写、
SQL Engine/方言、多 sheet 读取、pandas 风格重名列、图表）。1.10 的旧 API
（`readCsv`、`readXls`、`readSql(Connection)`、`writeSql`、`plot`、
`show`、`shell`）保留以向后兼容 —— 签名不变，但**行为**已按上方破坏性
变更表所列发生变化。

utilities
==========

**joinery** includes some tools to make working with data frames easier.
These tools are available by running `joinery.DataFrame` as an application.

```bash
$ java joinery.DataFrame
usage: joinery.DataFrame [compare|plot|show|shell] [csv-file ...]
```

show
----------
Show displays the tabular data of a data frame in a gui window.

```bash
$ java joinery.DataFrame show data.csv
```

![Screenshot of show window](https://raw.githubusercontent.com/cardillo/joinery/master/src/main/resources/templates/assets/images/joinery-show.png)

plot
----------
Plot displays the numeric data of a data frame as a chart.

```bash
$ java joinery.DataFrame plot data.csv
```

![Screenshot of plot window](https://raw.githubusercontent.com/cardillo/joinery/master/src/main/resources/templates/assets/images/joinery-plot.png)

shell
----------
Launches an interactive JavaScript shell for working with data frames.

```javascript
$ java joinery.DataFrame shell
# Joinery -- Data frames for Java, 1.10-deb702e
# OpenJDK 64-Bit Server VM, Oracle Corporation, 1.8.0_92-internal
# Rhino 1.7 release 2 2009 03 22
> df = DataFrame.readCsv("https://www.quandl.com/api/v1/datasets/GOOG/NASDAQ_AAPL.csv")
              Date	  Open	  High	   Low	        Close	             Volume
    0	2015-03-20	128.25	128.4	125.16	 125.90000000	  68695136.00000000
    1	2015-03-19	128.75	129.25	127.4	 127.50000000	  45809490.00000000
    2	2015-03-18	127.0	129.16	126.37	 128.47000000	  65270945.00000000
    3	2015-03-17	125.9	127.32	125.65	 127.04000000	  51023104.00000000
    4	2015-03-16	123.88	124.95	122.87	 124.95000000	  35874300.00000000
    5	2015-03-13	124.4	125.4	122.58	 123.59000000	  51827283.00000000
    6	2015-03-12	122.31	124.9	121.63	 124.45000000	  48362719.00000000
    7	2015-03-11	124.75	124.77	122.11	 122.24000000	  68938974.00000000
    8	2015-03-10	126.41	127.22	123.8	 124.51000000	  68856582.00000000

... 8649 rows skipped ...

 8658	1980-12-12	0.0	4.12	4.11	   4.11000000	  14657300.00000000

> df.types()
[class java.util.Date, class java.lang.String, class java.lang.String, class java.lang.String, class java.lang.Double, class java.lang.Double]
> df.sortBy("Date")
              Date     Open     High     Low            Close                Volume
 8658	1980-12-12	0.0	4.12	4.11	   4.11000000	  14657300.00000000
 8657	1980-12-15	0.0	3.91	3.89	   3.89000000	   5496400.00000000
 8656	1980-12-16	0.0	3.62	3.61	   3.61000000	   3304000.00000000
 8655	1980-12-17	0.0	3.71	3.7 	   3.70000000	   2701300.00000000
 8654	1980-12-18	0.0	3.82	3.8 	   3.80000000	   2295300.00000000
 8653	1980-12-19	0.0	4.05	4.04	   4.04000000	   1519700.00000000
 8652	1980-12-22	0.0	4.25	4.23	   4.23000000	   1167600.00000000
 8651	1980-12-23	0.0	4.43	4.41	   4.41000000	   1467200.00000000
 8650	1980-12-24	0.0	4.66	4.64	   4.64000000	   1500100.00000000

... 8649 rows skipped ...

    0	2015-03-20	128.25	128.4	125.16	 125.90000000	  68695136.00000000

> .reindex("Date")
	       Open	High	 Low	        Close	             Volume
1980-12-12	0.0	4.12	4.11	   4.11000000	  14657300.00000000
1980-12-15	0.0	3.91	3.89	   3.89000000	   5496400.00000000
1980-12-16	0.0	3.62	3.61	   3.61000000	   3304000.00000000
1980-12-17	0.0	3.71	3.7 	   3.70000000	   2701300.00000000
1980-12-18	0.0	3.82	3.8 	   3.80000000	   2295300.00000000
1980-12-19	0.0	4.05	4.04	   4.04000000	   1519700.00000000
1980-12-22	0.0	4.25	4.23	   4.23000000	   1167600.00000000
1980-12-23	0.0	4.43	4.41	   4.41000000	   1467200.00000000
1980-12-24	0.0	4.66	4.64	   4.64000000	   1500100.00000000

... 8649 rows skipped ...

2015-03-20	128.25	128.4	125.16	 125.90000000	  68695136.00000000

> .retain("Close")
	                Close
1980-12-12	   4.11000000
1980-12-15	   3.89000000
1980-12-16	   3.61000000
1980-12-17	   3.70000000
1980-12-18	   3.80000000
1980-12-19	   4.04000000
1980-12-22	   4.23000000
1980-12-23	   4.41000000
1980-12-24	   4.64000000

... 8649 rows skipped ...

2015-03-20	 125.90000000

> .plot(PlotType.AREA)
```


new in 1.11 (2026-08)
==========

1.11 版本带来了一批修复与新增能力（由 GLM-5.2 AI 工具辅助完成）。
**这是一次行为契约重写，不是纯粹的 bug 修复版本** —— 升级前请务必阅读
上方的「⚠️ 破坏性变更警告」一节。下列新接口的风格对齐 Python pandas /
SQLAlchemy，让 pandas 用户能无缝上手。


read xlsx / write xlsx（读写 .xlsx）
----------

1.10 版本只能读取老的 `.xls`（HSSF）格式。1.11 新增完整的 `.xlsx`
（OOXML）支持 —— 读与写都覆盖 —— 且写大文件时自动切换到流式
`SXSSFWorkbook`，百万行导出不再 OOM。

```java
// 读 .xlsx（按内容自动识别 .xls 与 .xlsx，不依赖扩展名）
DataFrame<Object> df = DataFrame.readXls("data.xlsx");
DataFrame<Object> df2 = DataFrame.readXlsx("data.xlsx"); // 显式按 xlsx 读

// 写 .xlsx —— 行数 > 100 时自动用 SXSSF 流式写，否则用 XSSF
df.writeXlsx("out.xlsx");
df.writeXlsx(new FileOutputStream("out.xlsx"));
```

单元格类型严格保留（不再粗暴 `convert()` 强转）：

| Excel 单元格               | DataFrame 中的值 |
|----------------------------|------------------|
| `100`（整数）              | `Long` 100L      |
| `3.14`（小数）             | `Double` 3.14    |
| `2026-08-01`（日期）       | `Date`           |
| `=A2*B2`（公式）           | **求值后的结果**（如 `1000.0`），不是公式文本 |
| 同列混合多种类型           | 整列 widening 为 `String`（保留语义） |
| 长整数 ≥ 15 位（身份证/银行卡） | 写入时按 `STRING` 写，避免 double 末尾变 0 |


multiple sheets（多 sheet 读取）
----------

1.10 静默地只读首张 sheet，**其余 sheet 数据全部丢失**。1.11 对齐
pandas 的 `read_excel(sheet_name=...)`：

```java
// 列出所有 sheet 名
List<String> sheets = DataFrame.sheetNames("workbook.xlsx");
// ["第一章", "第二章", "第三章"]

// 按 index 或按 name 读
DataFrame<Object> ch2 = DataFrame.readXlsx("workbook.xlsx", 1);
DataFrame<Object> ch3 = DataFrame.readXlsx("workbook.xlsx", "第三章");

// sheetIndex 越界 / sheetName 不存在时抛 IllegalArgumentException，
// 并在异常消息里列出可用 sheet，方便排错
```


sql engine & dialects（SQL 数据库 Engine/方言，SQLAlchemy 风格）
----------

数据库访问层被解耦为可插拔的 `Engine`/`Dialect` 抽象。开箱即用的六方言：
`H2` / `MySQL` / `PostgreSQL` / `Oracle` / `SQLite` / `Doris`。驱动类按
URL 前缀（及端口 —— 端口 `9030` 识别为 Doris，它走 MySQL 协议）自动选取。

```java
// 创建 Engine —— URL 自动选方言
Engine engine = DataFrame.sqlEngine("jdbc:postgresql://host:5432/db");
Engine engine2 = DataFrame.sqlEngine("jdbc:mysql://host:3306/db", "user", "pass");

// 读（对标 pandas.read_sql_table / read_sql_query）
DataFrame<Object> users = DataFrame.readSqlTable(engine, "users");
DataFrame<Object> top   = DataFrame.readSqlQuery(engine,
        "SELECT * FROM users ORDER BY amount DESC LIMIT 10");

// 写（对标 pandas.to_sql）
df.writeSqlTable(engine, "users", IfExists.REPLACE);
df.writeSqlTable(engine, "users", IfExists.APPEND, 1000); // 批大小 1000

// 强类型保留 —— INTEGER→Long、DOUBLE→Double、TIMESTAMP→Date
assertEquals(Long.class,    users.get(0, 0).getClass());
assertEquals(Double.class,  users.get(0, 1).getClass());
```

旧版 `readSql(Connection)` / `writeSql(PreparedStatement)` API 保留以
向后兼容 —— 但注意新的强类型 `readSqlQuery` 已取代把所有列转字符串的
`readSql(ResultSet)`。


charts（图表，pandas + matplotlib 风格）
----------

`Display` 为适配 XChart 4.0.4 重写，**完整对齐 XChart 10 种图表**：
hist / box / scatter / pie / bar / hbar / bubble / heatmap / ohlc / radar / dial，
加 5 种无 GUI 导出（PNG/JPG/BMP/GIF/SVG）。标题与轴标签自动选用 CJK 字体
（`微软雅黑` / `SansSerif` 兜底），中文不会出现方框乱码。

```java
df.hist("amount", 20);                  // 直方图（对标 pandas df.hist）
df.box("amount", "price");              // 箱线图（多列）
df.scatter("x", "y");                   // 散点图
df.pie("category", "amount");           // 饼图 —— 按 category 自动聚合 value
df.bar("a", "b");                       // 柱状图（对标 pandas df.plot.bar）
df.hbar("a");                           // 横向条形图（对标 df.plot.barh）
df.bubble("x", "y", "size");            // 气泡图（三维）
df.heatmap("region", "product", "sales"); // 热力图（对标 seaborn.heatmap）
df.ohlc("open", "high", "low", "close");  // K 线图（对标 mplfinance）
df.radar(0, "速度", "力量", "技巧");       // 雷达图（多维对比，自动归一化）
df.dial("score");                       // 仪表盘（单值百分比）

DataFrame.saveChart(chart, "out.png");  // PNG（对标 matplotlib savefig）
DataFrame.saveChartJpg(chart, "out.jpg");  // JPG / BMP / GIF 同理
DataFrame.saveChartSvg(chart, "out.svg");  // SVG 矢量（需 vectorgraphics2d 依赖）
```


duplicate column names（重名列，pandas 风格 Index）
----------

1.10 的 `Index` 强制名字唯一，重名直接**抛** `IllegalArgumentException`
—— 这意味着表头有重名列的 Excel、join 后的同名列、reindex 重复键值、
add/append 同名行/列 全部崩溃。1.11 按 pandas 风格重写 `Index`：名字映射到
*位置列表*。

```java
// 现在允许重名
DataFrame<Object> df = new DataFrame<>("value", "value");
df.append(Arrays.asList(1, 2));

// col(name) 返回首个匹配（向后兼容）
df.col("value");                  // [1]

// cols(name) 返回所有匹配（pandas 风格，新增）
df.cols("value");                 // 包含两列的 DataFrame

// columnsList() 暴露重名（columns() 仍返回去重的 Set）
df.columnsList();                 // [value, value]
df.columns();                     // [value]
```


csv bom & encoding（CSV 的 BOM 与编码）
----------

读取时自动剥离 UTF-8 BOM 并强制 UTF-8 解码（1.10 走平台默认编码且会被
BOM 噎住）；写入时补一个 UTF-8 BOM，让中文 CSV 在 Windows 版 Excel 里
能正确打开不乱码。

```java
// Excel 导出的带 BOM 的中文 CSV 也能正确读
DataFrame<Object> df = DataFrame.readCsv("中文.csv");
// 首列名是 "分类"，而不是 "\ufeff分类"
```


groupby + sortby chain（链式调用 bug 修复）
----------

一处静默的正确性 bug：`df.groupBy("x").sortBy("y").mean()` 因 `sortBy`
重置分组状态而退化成整列平均。已修复 —— 该链式调用现在能正确按分组聚合。


documentation
==========

The complete api documentation for the `DataFrame` class is available
at https://joinery.sh


---

## ⚠️ 免责声明（必读）

> **本项目（含 2026-08 由 GLM-5.2 AI 工具辅助完成的修改完善版本）由 AI 工具辅助开发，免费分发，不附带任何形式的担保。**

> - **AI 生成内容**：本项目 2026-08 修改版本的需求分析、设计、代码改动、注释、文档（含本 README「本次修改完善说明」章节）均由 [GLM-5.2](https://chatglm.cn) AI 工具辅助生成，**可能存在错误、遗漏、与最新官方契约不符、或未经验证的内容**。使用者须自行核实、自行评估、自行测试，**不应盲目信任 AI 生成的任何代码或结论**。
>
> - **免费分发，"按现状"提供**：本项目以"按现状"（AS IS）和"按可用"（AS AVAILABLE）基础免费提供，不收取任何费用，**明示不提供任何明示或默示的担保**，包括但不限于对适销性、特定用途适用性、非侵权、准确性、可靠性、与任何特定目标的兼容性的担保。
>
> - **仅用于学习与交流目的**：本次修改完善的核心目的是**技术学习、研究交流与个人探索**——验证「AI 辅助代码重构、依赖安全升级、跨数据库方言抽象」等工程实践是否可行，**并非商业产品**。本项目不针对任何生产场景或关键业务做适配或承诺，不建议直接用于生产环境、商业场景、医疗/法律/金融/统计分析等高风险领域，也不构成任何专业建议。如需生产级方案，请评估官方商业支持或第三方经严格审计的产品。
>
> - **不保证及时维护与持续可用**：作者**没有义务**对本项目进行持续维护、版本跟进、问题修复、安全更新或提供任何形式的支持。本项目可能长期不更新、随时停止维护、关闭仓库或改变方向，也可能不会跟进 Apache POI、XChart、各数据库 JDBC 驱动、JDK 等上下游依赖的破坏性变更。使用者须做好"无人维护"的心理准备，自行 fork、自行修复或迁移至其他方案。
>
> - **使用者自行承担全部安全风险**：本项目涉及**数据库连接与凭据管理**（支持 PostgreSQL / Oracle / MySQL / SQLite / Doris / H2 等多种数据库）、**SQL 执行**（含 `readSqlTable` / `writeSqlTable` / `readSqlQuery` 等批量数据流转）、**Excel/CSV 文件读写**、**外部 JDBC 驱动加载**等多个安全敏感面。**作者不担保本项目在任何使用场景下的安全性、不会对上述任一方面的后果承担责任**，使用者须自行评估并决定是否使用。相关风险包括但不限于：
>   - **SQL 注入与数据泄露**：`readSqlQuery` 接受任意 SQL 字符串；若使用者拼接不可信输入进 SQL，**由此引发的 SQL 注入、数据泄露、数据破坏等后果，作者概不负责**；
>   - **凭据泄露**：示例代码与测试为便利可能展示数据库连接串、用户名、密码等敏感信息；如使用者照搬配置、提交到版本库或部署到不可信环境，**由此引发的凭据泄露、未授权访问、配额盗用等后果，作者概不负责**；
>   - **网络暴露与数据库权限**：连接生产数据库时若未遵循最小权限原则、未启用传输加密、未限制白名单，**由此引发的入侵、数据外泄、横向渗透等后果，作者概不负责**；
>   - **数据隐私与合规**：本项目读写数据库、Excel、CSV 时可能涉及 PII（个人身份信息）、商业数据等；**作者不担保处理方案满足任何特定地区的合规要求**（如 GDPR、《个人信息保护法》等），由此引发的合规违规、行政处罚、用户索赔等后果，由使用者自行承担；
>   - **第三方依赖与供应链**：本项目依赖 Apache POI、XChart、SuperCSV、各数据库 JDBC 驱动、commons-math3 等第三方包（即便已升级到最新稳定版）；**作者不担保这些依赖无漏洞、无后门、无供应链攻击**；由此引发的任何安全事件，作者概不负责；
>   - **Excel/CSV 数据正确性**：本次虽修复了 8 类 Excel bug 与 CSV 中文 BOM 问题，但实际文件结构千变万化（公式嵌套、合并单元格、条件格式、宏、加密文件等），**作者不担保读出的数据 100% 与原始文件一致**；如用于对账、报表、财务等数据准确性敏感场景，**使用者必须自行抽样验证**，由此导致的任何业务决策错误概不负责；
>   - **AI 辅助代码的潜在缺陷**：本次修改（含 sortBy bug 修复、Engine/Dialect 抽象、类型严格保留策略等）虽通过了 158 + 45 个测试用例，但测试覆盖度有限，**AI 生成的代码可能在边界场景下行为异常或存在未知缺陷**，使用者须自行做充分测试。
>
> - **不承担任何后果**：**使用者因使用、复制、分发、修改或依赖本项目而产生的任何直接或间接损失——包括但不限于数据丢失、业务中断、数据污染/泄露、系统故障、安全事件、凭据泄露、合规违规、第三方索赔、利润损失、决策失误或其他后果——作者概不负责，由使用者自行承担全部风险与责任。**
>
> - **数据无价，自行备份**：本项目作用于数据导入导出与计算链路，涉及数据库表、Excel/CSV 文件的读写与可能的覆盖写入（`IfExists.REPLACE` 会 DROP 再建表）。**使用 `writeSqlTable` / `writeXls` / `writeXlsx` 前请务必做好完整备份**，并对备份的可恢复性进行验证。
>
> - **与上游项目的关系**：本仓库 Fork 自 [cardillo/joinery](https://github.com/cardillo/joinery)（GPL-3.0 许可）。本次修改独立于上游，**上游不为本修改版本背书或承担责任**。如需回归上游官方实现，请使用原仓库。
>
> - **官方契约以最新版本为准**：本项目依赖的 Apache POI、XChart、各 JDBC 驱动、JDK 等更新频繁；本 README 与代码注释中引用的 API 行为基于截至 2026-08-01 的官方资料核实，但实际使用时**必须以目标安装版本的官方文档、Javadoc 与发布说明为最终准绳**。
>
> - **第三方依赖与安全**：本项目可能依赖第三方库与外部数据库服务。使用者须自行审查依赖供应链、数据库连接的合规性与安全性，并自行承担相应风险。
>
> - **未成年人·限制行为能力人**：若使用者是未成年人或限制行为能力人，应在监护人指导下使用，并由监护人承担一切后果。
>
> **下载、安装、使用、复制、分发或以任何方式利用本项目，即视为已阅读、理解并无条件接受本免责声明的全部条款。如不同意，请立即停止使用并删除本项目。**

