# Plan: Modernize Build and Automated Testing for Joinery

## Current State Summary

Joinery is a Java data frames library currently targeting **Java 8** (`source`/`target` 1.8) with:
- Maven build using plugins mostly from 2018
- JUnit 4 test framework (30 test files, good coverage)
- A custom **doctest framework** (`DocTestSuite`) that extracts executable code from Javadoc `{@code}` blocks and runs them as JUnit tests — currently broken on Java 9+ because it depends on the removed `com.sun.javadoc.*` API and `tools.jar`
- GitHub Actions CI that tests on Java 16/17/18 but has broken reporting (conditioned on Java 11, not in matrix)
- Several outdated dependencies (SLF4J 1.7.7, AspectJ 1.8.2, etc.)
- Outdated CI action versions (actions/cache@v2, codecov@v1.5.0)
- Doclava doclet for JavaDoc generation (discontinued, incompatible with modern Java)
- ISO-8859-1 source encoding (non-standard)

---

## Verification Gate (applies to ALL phases)

**No phase is complete until ALL of the following pass:**

1. **Compilation**: `mvn clean compile test-compile` succeeds with zero errors
2. **All tests pass**: `mvn test` reports zero failures, zero errors (including doctests)
3. **Code review**: An agent performs a thorough review of all changes in the phase, checking for correctness, regressions, and missed items

Each phase is committed only after passing this gate.

---

## Phase 1: Modernize Doctest Framework

**Goal:** Replace the broken `com.sun.javadoc.*` (old Doclet API, removed in Java 9) with the modern `javax.lang.model` + `com.sun.source.doctree` APIs so that Javadoc code examples continue to be extracted and tested on Java 11+.

### Background

The current `DocTestSuite` (`src/test/java/joinery/doctest/DocTestSuite.java`) works as follows:
1. `DataFrameDocTest` is annotated `@RunWith(DocTestSuite.class)` with `@SuiteClasses({DataFrame.class})`
2. `DocTestSuite` invokes `com.sun.tools.javadoc.Main.execute()` to run a custom doclet (`DocTestDoclet`) on the source file
3. The doclet iterates over classes/methods, finds `@code` inline tags whose text starts with `>`, extracts the code lines and expected assertion value
4. For each extracted example, it generates a `Callable<Object>` class in-memory, compiles it with `javax.tools.JavaCompiler`, executes it, and asserts the result matches
5. 63 doctest examples currently exist in `DataFrame.java`

**Problem:** `com.sun.javadoc.*` and `com.sun.tools.javadoc.Main` were deprecated in Java 9 and removed in later versions. The `build-doctest` profile in pom.xml only activates for Java `[1.6,1.8]` — meaning doctests have been silently skipped on all modern JDKs.

### Approach: Source-file parsing with `javax.tools.DocumentationTool`

Replace the old Doclet API with the modern `javax.tools.DocumentationTool` + `jdk.javadoc.doclet` API (available since Java 9, stable in 11+):

| Old (removed) | New (Java 11+) |
|---|---|
| `com.sun.javadoc.Doclet` | `jdk.javadoc.doclet.Doclet` (interface) |
| `com.sun.javadoc.RootDoc` | `jdk.javadoc.doclet.DocletEnvironment` |
| `com.sun.javadoc.ClassDoc` | `javax.lang.model.element.TypeElement` |
| `com.sun.javadoc.ProgramElementDoc` | `javax.lang.model.element.Element` |
| `com.sun.javadoc.ExecutableMemberDoc` | `javax.lang.model.element.ExecutableElement` |
| `com.sun.javadoc.Tag` (inline tags) | `com.sun.source.doctree.DocTree` / `CodeTree` / `LiteralTree` |
| `com.sun.tools.javadoc.Main.execute()` | `javax.tools.ToolProvider.getSystemDocumentationTool()` |

### Detailed changes to `DocTestSuite.java`:

1. **Replace imports**: Remove all `com.sun.javadoc.*` imports, add:
   - `jdk.javadoc.doclet.Doclet` and `jdk.javadoc.doclet.DocletEnvironment`
   - `javax.lang.model.element.*` (TypeElement, ExecutableElement, Element, ElementKind)
   - `javax.lang.model.SourceVersion`
   - `com.sun.source.doctree.*` (DocCommentTree, DocTree, CodeTree, LiteralTree)
   - `com.sun.source.util.DocTrees`
   - `javax.tools.DocumentationTool`, `javax.tools.ToolProvider`

2. **Rewrite `DocTestDoclet`** to implement `jdk.javadoc.doclet.Doclet` interface:
   - Implement `init(Locale, Reporter)` method
   - Implement `getName()` returning doclet name
   - Implement `getSupportedSourceVersion()` returning `SourceVersion.latest()`
   - Implement `getSupportedOptions()` returning empty set
   - Implement `run(DocletEnvironment env)`:
     - Get `DocTrees` from env: `DocTrees.instance(env)`
     - Iterate over `env.getIncludedElements()` to find `TypeElement` classes
     - For each class, iterate over enclosed elements (constructors, methods)
     - For each element, get `DocCommentTree` via `docTrees.getDocCommentTree(element)`
     - Walk the doc tree's body and block/inline tags looking for `DocTree.Kind.CODE` nodes
     - Extract text from `CodeTree`/`LiteralTree`, check if it starts with `>`
     - Call `generateRunner()` with the element info and tag text

3. **Adapt `generateRunner()`**:
   - Change parameter types from `ClassDoc`/`ProgramElementDoc`/`Tag` to `TypeElement`/`Element`/`String`
   - Update `getDescription()` to use `element.getSimpleName()` and `TypeElement.getQualifiedName()`
   - For executable elements, build the flat signature from `ExecutableElement.getParameters()`
   - Generated source class name: use `typeElement.getSimpleName() + "DocTest"`
   - Generated imports: use `typeElement.getQualifiedName()` instead of `cls.qualifiedName()`
   - The in-memory compilation and execution logic (javax.tools.JavaCompiler) stays **unchanged**

4. **Rewrite `generateDocTestClasses()`**:
   - Replace `com.sun.tools.javadoc.Main.execute()` with:
     ```java
     DocumentationTool docTool = ToolProvider.getSystemDocumentationTool();
     StandardJavaFileManager fm = docTool.getStandardFileManager(null, null, null);
     Iterable<? extends JavaFileObject> files = fm.getJavaFileObjects(sourceFile);
     DocumentationTool.DocumentationTask task = docTool.getTask(
         null, fm, null, DocTestDoclet.class, null, files);
     task.call();
     ```

5. **Remove `build-doctest` profile** from `pom.xml` — `javax.tools.DocumentationTool` is in the JDK standard library since Java 9, no `tools.jar` needed.

6. **Preserve all existing behavior**:
   - The `@DocTestSourceDirectory` and `@SuiteClasses` annotations remain unchanged
   - `DataFrameDocTest.java` requires zero changes
   - The `> ` line extraction logic, assertion extraction, return-injection, in-memory compilation, and JUnit runner integration all remain the same
   - All 63 existing doctests in `DataFrame.java` must continue to pass

### Verification:
- `mvn clean test -Dtest=DataFrameDocTest` passes with all 63 doctests green
- `mvn clean test` passes with all tests (including doctests) green
- Agent code review of all changes

---

## Phase 2: Update Java Target Version

**Goal:** Move from Java 8 to Java 11 as the minimum supported version.

### Changes:
- **`pom.xml`**: Change `<java.version>` from `1.8` to `11`
- **`pom.xml`**: Replace `<source>`/`<target>` compiler config with `<release>11</release>` (the modern Maven compiler approach)
- **`pom.xml`**: Update `maven-enforcer-plugin` required Maven version to `3.2.5` (needed for modern plugin compatibility)
- Verify compilation and all tests pass under Java 11+

**Why Java 11 (not 17 or 21):** Java 11 is the most conservative LTS jump — it keeps backward compatibility with the widest range of downstream consumers while still being a supported LTS release. The CI matrix will test higher versions.

### Verification:
- `mvn clean compile test-compile` succeeds
- `mvn clean test verify` — all tests pass (including doctests)
- Agent code review

---

## Phase 3: Update Maven Plugin Versions

**Goal:** Bring all Maven plugins to current stable versions.

| Plugin | Current | Target | Notes |
|--------|---------|--------|-------|
| `maven-compiler-plugin` | 3.8.0 | 3.13.0 | Switch to `<release>` element |
| `maven-surefire-plugin` | 2.22.0 | 3.5.2 | Major version bump, better JUnit support |
| `maven-failsafe-plugin` | 2.22.0 | 3.5.2 | Matches surefire |
| `maven-enforcer-plugin` | 3.0.0-M2 | 3.5.0 | Stable release |
| `maven-jar-plugin` | 3.1.0 | 3.4.2 | |
| `maven-assembly-plugin` | 3.1.0 | 3.7.1 | |
| `maven-javadoc-plugin` | 3.0.1 | 3.11.2 | Drop Doclava, use standard doclet |
| `maven-source-plugin` | 3.0.1 | 3.3.1 | |
| `maven-release-plugin` | 2.5.3 | 3.1.1 | |
| `maven-deploy-plugin` | 2.8.2 | 3.1.3 | |
| `maven-install-plugin` | 2.5.2 | 3.1.3 | |
| `maven-resources-plugin` | 3.1.0 | 3.3.1 | |
| `maven-clean-plugin` | 3.1.0 | 3.4.0 | |
| `maven-site-plugin` | 3.7.1 | 3.21.0 | |
| `maven-gpg-plugin` | 3.0.1 | 3.2.7 | |
| `jacoco-maven-plugin` | 0.8.7 | 0.8.12 | |
| `buildnumber-maven-plugin` | 1.4 | 3.2.1 | |
| `aspectj-maven-plugin` | 1.11 | 1.14 | (dev.aspectj groupId) |

### Changes:
- Update all plugin versions in `<pluginManagement>`
- For `maven-javadoc-plugin`: remove Doclava doclet configuration, use the standard doclet with `-Xdoclint:none` to avoid strict HTML checks
- For `buildnumber-maven-plugin`: update groupId to `org.codehaus.mojo` and version to `3.2.1`
- For `aspectj-maven-plugin`: update groupId to `dev.aspectj` and version to `1.14`

### Verification:
- `mvn clean test verify` — all tests pass
- Agent code review

---

## Phase 4: Update Dependency Versions

**Goal:** Bring all dependencies to current stable versions.

| Dependency | Current | Target | Notes |
|------------|---------|--------|-------|
| `junit` | 4.13.2 | 4.13.2 | Keep as-is (JUnit 5 migration is a separate effort) |
| `supercsv` | 2.4.0 | 2.4.0 | Latest available |
| `commons-math3` | 3.6.1 | 3.6.1 | Latest available |
| `poi` | 5.0.0 | 5.3.0 | Bugfixes and perf improvements |
| `xchart` | 2.5.1 | 3.8.8 | Major version bump (API changes, groupId changed to `org.knowm.xchart`) |
| `rhino` | 1.7.13 | 1.7.15 | Bugfixes |
| `jline` | 3.20.0 | 3.28.0 | Bugfixes and improvements |
| `metrics-annotation` | 4.2.0 | 4.2.28 | Bugfixes |
| `metrics-core` | 4.2.0 | 4.2.28 | Matches annotation |
| `aspectjrt` | 1.8.2 | 1.9.22.1 | Modern AspectJ |
| `slf4j-nop` | 1.7.7 | 2.0.16 | Major version bump |
| `derby` | 10.17.1.0 | 10.17.1.0 | Keep as-is (latest for Java 11+) |

### Special attention:
- **XChart 3.x** has a different groupId (`org.knowm.xchart`) and API changes — will need to review and update code in `Display.java` / plotting classes
- **SLF4J 2.x** is a clean upgrade since it's only used as `slf4j-nop` in the metrics profile

### Verification:
- `mvn clean test verify` — all tests pass
- Agent code review of any code changes needed for API compatibility

---

## Phase 5: Modernize GitHub Actions CI Workflow

**Goal:** Fix broken reporting, update action versions, expand test matrix, add PR builds.

### Changes to `.github/workflows/build.yml`:

1. **Trigger on PRs** in addition to push to master:
   ```yaml
   on:
     push:
       branches: [ master ]
     pull_request:
       branches: [ master ]
   ```

2. **Update Java matrix** to current LTS + latest:
   ```yaml
   java: [ 11, 17, 21 ]
   ```

3. **Update action versions:**
   - `actions/setup-java@v3` → `actions/setup-java@v4`
   - `actions/cache@v2` → remove (setup-java@v4 has built-in Maven caching with `cache: 'maven'`)
   - `codecov/codecov-action@v1.5.0` → `codecov/codecov-action@v5`
   - `dorny/test-reporter@v1` → `dorny/test-reporter@v1` (still latest)
   - `GabrielBB/xvfb-action@v1` → `GabrielBB/xvfb-action@v1.6` (pin to stable)

4. **Fix reporting conditions** — change `matrix.java == 11` to match an actual matrix entry (the lowest Java version):
   ```yaml
   if: ${{ runner.os == 'Linux' && matrix.java == 11 }}
   ```

### Verification:
- YAML syntax valid
- Agent code review of workflow changes

---

## Phase 6: Source Encoding and Minor Cleanup

### Changes:
- **`pom.xml`**: Change source encoding from `iso-8859-1` to `utf-8` (verify no non-ASCII issues in source files first)
- **`pom.xml`**: Clean up the `aspectj-maven-plugin` configuration to use `<release>` instead of `<target>`/`<complianceLevel>`

### Verification:
- `mvn clean test verify` — all tests pass
- Agent code review

---

## Implementation Order

The phases **must** be implemented in order — each builds on the prior:

1. **Phase 1** — Doctest modernization (highest priority, currently broken)
2. **Phase 2** — Java target version (foundational for everything else)
3. **Phase 3** — Maven plugin versions (needed before dependency changes)
4. **Phase 4** — Dependency versions (may require code changes for XChart 3.x)
5. **Phase 5** — CI workflow modernization
6. **Phase 6** — Encoding and cleanup

Each phase gets its own commit after passing the verification gate.

---

## Risk Assessment

- **Doctest rewrite (Phase 1)**: Medium-high risk. The new `jdk.javadoc.doclet` API has a significantly different shape than the old `com.sun.javadoc` API. The doc tree walking logic must be carefully mapped. However, the in-memory compilation and execution logic (which is the complex part) stays unchanged.
- **XChart 2.x → 3.x (Phase 4)**: High risk. The API changed significantly (package rename, method signature changes). Will need to update import paths and method calls in `Display.java` and related plotting code.
- **Surefire/Failsafe 2.x → 3.x (Phase 3)**: Medium risk. May change parallel test behavior; need to verify test suite still passes with parallel execution.
- **Doclava removal (Phase 3)**: Low risk. The custom Doclava templates in `src/main/resources/templates/` may become unused — determine if they serve another purpose before removing.
- **Java 11 minimum (Phase 2)**: Low risk for the library itself; intentional breaking change for downstream consumers still on Java 8.

## Out of Scope (Future Work)

- **JUnit 4 → JUnit 5 migration**: Large effort (30 test files), better as a dedicated task
- **Static analysis tools** (Checkstyle, SpotBugs, PMD): Useful but orthogonal to build modernization
- **Gradle migration**: Maven is fine for this project size
- **Module system** (`module-info.java`): Optional and can be added later
