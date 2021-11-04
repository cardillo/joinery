package joinery.js;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import joinery.DataFrame;
import joinery.impl.Shell;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.mozilla.javascript.WrappedException;


public class JavascriptExpressionSuite
extends Suite {

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface JavascriptResource {
        public String name();
    }

    private static List<Runner> runnersFromJs(final Class<?> cls)
    throws InitializationError, IOException {
        final JavascriptResource js = cls.getAnnotation(JavascriptResource.class);
        final List<Runner> runners = new LinkedList<>();
        try (final LineNumberReader reader = new LineNumberReader(
                new InputStreamReader(ClassLoader.getSystemResourceAsStream(js.name())))) {
            while (true) {
                final String expr = reader.readLine();
                final int line = reader.getLineNumber();
                if (expr == null) {
                    break;
                }

                if (!expr.trim().isEmpty() && !expr.trim().startsWith("//")) {
                    runners.add(new Runner() {
                        @Override
                        public Description getDescription() {
                            final String[] parts = expr.split(" *; *");
                            final String desc = parts[parts.length - 1];
                            return Description.createTestDescription(cls,
                                String.format("%s:%s => %s", js.name(), line, desc), js);
                        }

                        @Override
                        public void run(final RunNotifier notifier) {
                            notifier.fireTestStarted(getDescription());
                            // should really do something more generic here
                            // instead of hard-coded setup, explicit
                            // data frame construction...
                            System.setIn(new ByteArrayInputStream(
                                String.format("tmp = frames[0]; df = frames[1]; %s;", expr).getBytes()));
                            try {
                                //TODO Figure out what is the csv here meaning and if it is related with my issue?
                                //  It seems like Dataframe.readCsv is related with my issue
                                //  good start then I located Dataframe.readCsv
                                final DataFrame<Object> df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("grouping.csv"));
                                final Object result = Shell.repl(Arrays.asList(new DataFrame<>(), df));
                                if (result instanceof WrappedException) {
                                    throw WrappedException.class.cast(result).getWrappedException();
                                } else if (result instanceof Throwable) {
                                    throw Throwable.class.cast(result);
                                }
                                org.junit.Assert.assertFalse(result == null);
                            } catch (final IOException ioe) {
                                notifier.fireTestAssumptionFailed(new Failure(getDescription(), ioe));
                            } catch (final AssertionError err) {
                                notifier.fireTestFailure(new Failure(getDescription(), err));
                            } catch (final Throwable ex) {
                                notifier.fireTestFailure(new Failure(getDescription(), ex));
                            } finally {
                                notifier.fireTestFinished(getDescription());
                            }
                        }
                    });
                }
            }
        }
        return runners;
    }

    public JavascriptExpressionSuite(final Class<?> cls, final RunnerBuilder builder)
    throws InitializationError, IOException {
        super(cls, runnersFromJs(cls));
    }
}
