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

package joinery.impl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.List;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import joinery.DataFrame;

import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.NativeJavaArray;
import org.mozilla.javascript.NativeJavaClass;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

public class Shell {
    public static Object repl(final List<DataFrame<Object>> frames)
    throws IOException {
        return new Repl(frames).run();
    }

    private static class Repl
    extends ScriptableObject {
        private static final long serialVersionUID = 1L;
        private static final String PROMPT = "> ";
        private static final String PROMPT_CONTINUE = "  ";
        private static final String LAST_VALUE_NAME = "_";
        private static final String FILENAME = "<shell>";

        private final List<DataFrame<Object>> frames;
        private final boolean interactive = System.console() != null;
        private transient boolean quit = false;
        private transient int statement = 1;

        public Repl(final List<DataFrame<Object>> frames) {
            this.frames = frames;
        }

        @Override
        public String getClassName() {
            return "shell";
        }

        public Object run()
        throws IOException {
            Object result = null;
            final Console console = console();
            final Context ctx = Context.enter();

            if (interactive) {
                System.out.println(ctx.getImplementationVersion());
            }

            try {
                ctx.initStandardObjects(this);
                // add functions
                defineFunctionProperties(
                        new String[] { "print", "quit", "source" },
                        getClass(), ScriptableObject.DONTENUM
                    );
                // make data frame easily available
                put("DataFrame", this, new NativeJavaClass(this, DataFrame.class));
                // make data frame classes available as well
                for (final Class<?> cls : DataFrame.class.getDeclaredClasses()) {
                    put(cls.getSimpleName(), this, new NativeJavaClass(this, cls));
                }
                // make argument frames available
                put("frames", this, new NativeJavaArray(this, frames.toArray()));

                String expr = null;
                while (!quit && (expr = read(console)) != null) {
                    try {
                        result = eval(expr);
                        if (result != Context.getUndefinedValue()) {
                            // store last value for reference
                            put(LAST_VALUE_NAME, this, result);
                            if (interactive) {
                                System.out.println(Context.toString(result));
                            }
                        }
                    } catch (final Exception ex) {
                        if (interactive) {
                            ex.printStackTrace();
                        }
                        result = ex;
                    }
                }
            } finally {
                Context.exit();
            }
            return Context.jsToJava(result, Object.class);
        }

        public String read(final Console console)
        throws IOException {
            final Context ctx = Context.getCurrentContext();
            final StringBuilder buffer = new StringBuilder();
            String line = null;
            if ((line = console.readLine(PROMPT)) != null) {
                // apply continued lines to last value
                if (line.startsWith(".") && has(LAST_VALUE_NAME, this)) {
                    buffer.append(LAST_VALUE_NAME);
                }

                // read lines a complete statement is found or eof
                buffer.append(line);
                while (!ctx.stringIsCompilableUnit(buffer.toString()) &&
                        (line = console.readLine(PROMPT_CONTINUE)) != null) {
                    buffer.append(line);
                }

                return buffer.toString();
            }
            return null;
        }

        public Object eval(final String source) {
            final Context ctx = Context.getCurrentContext();
            return ctx.evaluateString(this, source, FILENAME, statement++, null);
        }

        @SuppressWarnings("unused")
        public static void print(final Context ctx, final Scriptable object, final Object[] args, final Function func) {
            for (int i = 0; i < args.length; i++) {
                if (i > 0) {
                    System.out.print(" ");
                }
                System.out.print(Context.toString(args[i]));
            }
            System.out.println();
        }

        @SuppressWarnings("unused")
        public void quit() {
            quit = true;
        }

        @SuppressWarnings("unused")
        public static void source(final Context ctx, final Scriptable object, final Object[] args, final Function func)
        throws Exception {
            final Repl repl = Repl.class.cast(object);
            for (int i = 0; i < args.length; i++) {
                final String file = Context.toString(args[i]);
                final SourceReader source = repl.new SourceReader(file);
                String expr = null;
                while ((expr = repl.read(source)) != null) {
                    final Object result = repl.eval(expr);
                    if (result != Context.getUndefinedValue()) {
                        // store last value for reference
                        repl.put(LAST_VALUE_NAME, repl, result);
                        if (repl.interactive) {
                            System.out.println(Context.toString(result));
                        }
                    }
                }
            }
        }

        private Console console()
        throws IOException {
            if (interactive) {
                try {
                    return new JLineConsole();
                } catch (final NoClassDefFoundError ignored) { }
            }
            return new Console();
        }

        private class Console {
            private final BufferedReader reader;

            private Console()
            throws IOException {
                this(new BufferedReader(new InputStreamReader(System.in)));
            }

            protected Console(final BufferedReader reader)
            throws IOException {
                this.reader = reader;
            }

            public String readLine(final String prompt)
            throws IOException {
                if (interactive) {
                    System.out.print(prompt);
                }
                return reader.readLine();
            }
        }

        private class SourceReader
        extends Console {
            private final String nl = System.getProperty("line.separator");

            private SourceReader(final String file)
            throws IOException {
                super(new BufferedReader(new FileReader(file)));
            }

            @Override
            public String readLine(final String prompt)
            throws IOException {
                final String line = super.readLine("");
                if (interactive && line != null) {
                    System.out.printf("%s%s\n", prompt, line, nl);
                }
                return line;
            }
        }

        private class JLineConsole
        extends Console
        implements Runnable, Completer {
            private final ConsoleReader console;

            private JLineConsole()
            throws IOException {
                console = new ConsoleReader();
                console.addCompleter(this);
                Runtime.getRuntime().addShutdownHook(new Thread(this));
            }

            @Override
            public String readLine(final String prompt)
            throws IOException {
                console.setPrompt(prompt);
                return console.readLine();
            }

            @Override
            public void run() {
                try {
                    console.getTerminal().restore();
                } catch (final Exception ignored) { }
            }

            @Override
            public int complete(final String buffer, final int cursor, final List<CharSequence> candidates) {
                final String value = buffer.substring(buffer.lastIndexOf(' ') + 1);
                final int dot = value.lastIndexOf('.') + 1;
                if (dot > 1) {
                    final String name = value.substring(0, dot - 1);
                    final String prefix = value.substring(dot);
                    final Object var = get(name, Repl.this);
                    if (var != Context.getUndefinedValue()) {
                        final Class<?> cls = Context.jsToJava(var, Object.class).getClass();
                        for (final Method m : cls.getDeclaredMethods()) {
                            if (m.getName().startsWith(prefix)) {
                                candidates.add(m.getName());
                            }
                        }
                    }
                    return dot;
                }
                return buffer.length();
            }
        }
    }
}
