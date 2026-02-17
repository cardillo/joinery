/*
 * Joinery -- Data frames for Java
 * Copyright (c) 2014, 2015 IBM Corp.
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

package joinery.doctest;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.security.SecureClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.DocumentationTool;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import com.sun.source.doctree.DocCommentTree;
import com.sun.source.doctree.DocTree;
import com.sun.source.doctree.LiteralTree;
import com.sun.source.util.DocTrees;

import jdk.javadoc.doclet.Doclet;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;

public class DocTestSuite
extends Suite {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface DocTestSourceDirectory {
        public String value();
    }

    public static final class DocTestDoclet
    implements Doclet {
        private static Map<String, Runner> doctests = new LinkedHashMap<>();

        @Override
        public void init(final Locale locale, final Reporter reporter) {
        }

        @Override
        public String getName() {
            return "DocTestDoclet";
        }

        @Override
        public Set<? extends Option> getSupportedOptions() {
            return Collections.emptySet();
        }

        @Override
        public SourceVersion getSupportedSourceVersion() {
            return SourceVersion.latest();
        }

        private static void generateRunner(
                final String qualifiedClassName, final String simpleClassName,
                final String memberName, final String memberSignature,
                final String tagText) {
            final List<String> lines = new LinkedList<>();
            final StringBuilder sb = new StringBuilder();
            String assertion = null;
            try (BufferedReader reader = new BufferedReader(new StringReader(tagText))) {
                for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                    line = line.trim();
                    final int index = line.indexOf(">") + 1;
                    if (index > 0) {
                        lines.add(line.substring(index));
                    } else if (lines.size() > 0) {
                        if (line.length() > 0) {
                            // record the assertion value
                            assertion = line.trim();

                            // inject return before last statement
                            int last;
                            for (last = lines.size() - 1; last > 0; last--) {
                                final String check = lines.get(last - 1);
                                if (check.contains(";") && !check.contains("return")) {
                                    break;
                                }
                            }
                            lines.set(last, "return " + lines.get(last));
                        }
                        break;
                    }
                }
            } catch (final IOException ex) {
                throw new IllegalStateException("error reading string", ex);
            }

            for (final String line : lines) {
                sb.append(line).append("\n");
            }
            if (assertion == null) {
                sb.append("return null;\n");
            }

            final String expected = assertion;
            doctests.put(memberName + memberSignature, new Runner() {
                @Override
                public Description getDescription() {
                    return Description.createTestDescription(
                            qualifiedClassName, memberName + memberSignature);
                }

                @Override
                public void run(final RunNotifier notifier) {
                    notifier.fireTestStarted(getDescription());
                    Object value = null;
                    try {
                        final String name = String.format("%sDocTest", simpleClassName);
                        final String source =
                            "import " + qualifiedClassName + ";\n" +
                            "import " + qualifiedClassName + ".*;\n" +
                            "import java.util.*;\n" +
                            "import java.sql.Connection;\n" +
                            "import java.sql.DriverManager;\n" +
                            "public class " + name + "\n" +
                            "implements java.util.concurrent.Callable<Object> {\n" +
                            "    public Object call() throws Exception {\n" +
                                     sb.toString() + "\n" +
                            "    }\n" +
                            "}\n";
                        final URI uri = URI.create(name + Kind.SOURCE.extension);
                        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
                        final JavaFileManager mgr = new ForwardingJavaFileManager<JavaFileManager>(
                                compiler.getStandardFileManager(null, null, null)) {
                            private final Map<String, ByteArrayOutputStream> classes =
                                new HashMap<>();
                            @Override
                            public ClassLoader getClassLoader(final Location location) {
                                return new SecureClassLoader() {
                                    @Override
                                    protected Class<?> findClass(final String name)
                                    throws ClassNotFoundException {
                                        final ByteArrayOutputStream bytes = classes.get(name);
                                        return super.defineClass(
                                                name, bytes.toByteArray(), 0, bytes.size());
                                    }
                                };
                            }

                            @Override
                            public JavaFileObject getJavaFileForOutput(
                                    final Location location, final String className,
                                    final Kind kind, final FileObject sibling)
                            throws IOException {
                                final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                                classes.put(className, bytes);
                                return new SimpleJavaFileObject(URI.create(className), kind) {
                                    @Override
                                    public OutputStream openOutputStream()
                                    throws IOException {
                                        return bytes;
                                    }
                                };
                            }
                        };

                        final List<JavaFileObject> files =
                            Collections.<JavaFileObject>singletonList(
                                new SimpleJavaFileObject(uri, Kind.SOURCE) {
                                    @Override
                                    public CharSequence getCharContent(final boolean ignore) {
                                        return source;
                                    }
                                });

                        final StringBuilder error = new StringBuilder();
                        final DiagnosticListener<FileObject> diags =
                                new DiagnosticListener<FileObject>() {
                                    @Override
                                    public void report(final Diagnostic<? extends FileObject> diagnostic) {
                                        if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                                            error.append("\t");
                                            error.append(diagnostic.getMessage(null));
                                            error.append("\n");
                                        }
                                    }
                            };
                        compiler.getTask(null,  mgr,  diags, null,  null, files).call();
                        if (error.length() > 0) {
                            throw new Exception("Doctest failed to compile:\n" + error.toString());
                        }

                        final Class<?> cls = mgr.getClassLoader(null).loadClass(name);
                        value = Callable.class.cast(cls.getDeclaredConstructor().newInstance()).call();

                        if (expected != null) {
                            org.junit.Assert.assertEquals(expected, String.valueOf(value));
                        }
                    } catch (final AssertionError err) {
                        notifier.fireTestFailure(new Failure(getDescription(), err));
                    } catch (final Exception ex) {
                        notifier.fireTestFailure(new Failure(getDescription(), ex));
                    } finally {
                        notifier.fireTestFinished(getDescription());
                    }
                }
            });
        }

        private static String flatSignature(final ExecutableElement method) {
            final StringBuilder sb = new StringBuilder();
            sb.append("(");
            boolean first = true;
            for (final VariableElement param : method.getParameters()) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(param.asType().toString()
                    .replaceAll("<[^>]*>", "")
                    .replaceAll("^.*\\.", ""));
            }
            sb.append(")");
            return sb.toString();
        }

        private static void processElement(
                final DocTrees docTrees, final TypeElement cls, final Element elem) {
            final DocCommentTree docComment = docTrees.getDocCommentTree(elem);
            if (docComment == null) {
                return;
            }

            final String qualifiedName = cls.getQualifiedName().toString();
            final String simpleName = cls.getSimpleName().toString();
            final String memberName = elem.getSimpleName().toString();
            final String memberSig = elem instanceof ExecutableElement ?
                flatSignature((ExecutableElement) elem) : "";

            // Walk all doc trees looking for @code inline tags
            final List<? extends DocTree> body = docComment.getFullBody();
            for (final DocTree tree : body) {
                processDocTree(qualifiedName, simpleName, memberName, memberSig, tree);
            }
            for (final DocTree tree : docComment.getBlockTags()) {
                processDocTree(qualifiedName, simpleName, memberName, memberSig, tree);
            }
        }

        private static void processDocTree(
                final String qualifiedName, final String simpleName,
                final String memberName, final String memberSig,
                final DocTree tree) {
            if (tree.getKind() == DocTree.Kind.CODE) {
                final LiteralTree codeTree = (LiteralTree) tree;
                final String text = codeTree.getBody().getBody();
                if (text.trim().startsWith(">")) {
                    generateRunner(qualifiedName, simpleName, memberName, memberSig, text);
                }
            }
        }

        @Override
        public boolean run(final DocletEnvironment env) {
            final DocTrees docTrees = env.getDocTrees();
            for (final Element elem : env.getIncludedElements()) {
                if (elem.getKind() == ElementKind.CLASS || elem.getKind() == ElementKind.INTERFACE) {
                    final TypeElement cls = (TypeElement) elem;
                    // Process class-level doc
                    processElement(docTrees, cls, cls);
                    // Process constructors and methods
                    for (final Element enclosed : cls.getEnclosedElements()) {
                        if (enclosed.getKind() == ElementKind.CONSTRUCTOR ||
                            enclosed.getKind() == ElementKind.METHOD) {
                            processElement(docTrees, cls, enclosed);
                        }
                    }
                }
            }
            return true;
        }
    }

    private static List<Runner> generateDocTestClasses(final Class<?> cls)
    throws InitializationError {
        final DocTestSourceDirectory dir = cls.getAnnotation(DocTestSourceDirectory.class);
        final SuiteClasses suiteClasses = cls.getAnnotation(SuiteClasses.class);
        if (suiteClasses == null) {
            throw new InitializationError(String.format(
                    "class '%s' must have a SuiteClasses annotation",
                    cls.getName()
                ));
        }

        final DocumentationTool docTool = ToolProvider.getSystemDocumentationTool();
        if (docTool == null) {
            throw new InitializationError(
                "SystemDocumentationTool not available (are you running on a JDK?)");
        }

        for (final Class<?> c : suiteClasses.value()) {
            final String source = dir.value() + "/" +
                                  c.getName().replaceAll("\\.", "/") +
                                  Kind.SOURCE.extension;
            try (StandardJavaFileManager fm = docTool.getStandardFileManager(null, null, null)) {
                final Iterable<? extends JavaFileObject> files =
                    fm.getJavaFileObjects(new File(source));
                final DocumentationTool.DocumentationTask task =
                    docTool.getTask(null, fm, null, DocTestDoclet.class, null, files);
                task.call();
            } catch (final IOException ex) {
                throw new InitializationError(ex);
            }
        }
        return new LinkedList<>(DocTestDoclet.doctests.values());
    }

    public DocTestSuite(final Class<?> cls, final RunnerBuilder builder)
    throws InitializationError {
        super(cls, generateDocTestClasses(cls));
    }
}
