package joinery.doctest;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.Doclet;
import com.sun.javadoc.ExecutableMemberDoc;
import com.sun.javadoc.LanguageVersion;
import com.sun.javadoc.ProgramElementDoc;
import com.sun.javadoc.RootDoc;
import com.sun.javadoc.Tag;

public class DocTestSuite
extends Suite {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface DocTestSourceDirectory {
        public String value();
    }

    public static final class DocTestDoclet
    extends Doclet {
        private static Map<String, Runner> doctests = new LinkedHashMap<>();

        public static LanguageVersion languageVersion() {
            return LanguageVersion.JAVA_1_5;
        }

        private static void generateRunner(
                final ClassDoc cls, final ProgramElementDoc member, final Tag tag) {
            final List<String> lines = new LinkedList<>();
            final StringBuilder sb = new StringBuilder();
            String assertion = null;
            try (BufferedReader reader = new BufferedReader(new StringReader(tag.text()))) {
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
                                if (lines.get(last - 1).contains(";")) {
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

            // TODO extract imports and insert at beginning of generated class
            for (final String line : lines) {
                sb.append(line).append("\n");
            }
            if (assertion == null) {
                sb.append("return null;\n");
            }

            final String expected = assertion;
            doctests.put(member.toString(), new Runner() {
                @Override
                public Description getDescription() {
                    final String sig = member instanceof ExecutableMemberDoc ?
                        ExecutableMemberDoc.class.cast(member).flatSignature() : "";
                    return Description.createTestDescription(
                            cls.qualifiedName(), member.name() + sig);
                }

                @Override
                public void run(final RunNotifier notifier) {
                    notifier.fireTestStarted(getDescription());
                    Object value = null;
                    try {
                        final String name = String.format("%sDocTest", cls.name());
                        final String source =
                            "import " + cls.qualifiedName() + ";\n" +
                            "import java.util.*;\n" +
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
                        compiler.getTask(null,  mgr,  null , null,  null, files).call();

                        final Class<?> cls = mgr.getClassLoader(null).loadClass(name);
                        value = Callable.class.cast(cls.newInstance()).call();

                        if (expected != null) {
                            org.junit.Assert.assertEquals(expected, String.valueOf(value));
                        }

                        notifier.fireTestFinished(getDescription());
                    } catch (final AssertionError err) {
                        throw err;
                    } catch (final Exception ex) {
                        notifier.fireTestFailure(new Failure(getDescription(), ex));
                    }
                }
            });
        }

        public static boolean start(final RootDoc root) {
            for (final ClassDoc cls : root.classes()) {
                final List<ProgramElementDoc> elements = new LinkedList<>();
                elements.add(cls);
                elements.addAll(Arrays.asList(cls.constructors()));
                elements.addAll(Arrays.asList(cls.methods()));
                for (final ProgramElementDoc elem : elements) {
                    for (final Tag tag : elem.inlineTags()) {
                        final String name = tag.name();
                        if (name.equals("@code") && tag.text().trim().startsWith(">")) {
                            generateRunner(cls, elem, tag);
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

        for (final Class<?> c : suiteClasses.value()) {
            final String source = dir.value() + "/" +
                                  c.getName().replaceAll("\\.", "/") +
                                  Kind.SOURCE.extension;
            com.sun.tools.javadoc.Main.execute(
                    DocTestDoclet.class.getSimpleName(),
                    DocTestDoclet.class.getName(),
                    new String[] { source }
                );
        }
        return new LinkedList<>(DocTestDoclet.doctests.values());
    }

    public DocTestSuite(final Class<?> cls, final RunnerBuilder builder)
    throws InitializationError {
        super(cls, generateDocTestClasses(cls));
    }
}
