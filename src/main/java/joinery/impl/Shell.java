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
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.List;
import java.util.Stack;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import jline.console.ConsoleReader;
import joinery.DataFrame;

public class Shell {
    public static void repl(final List<DataFrame<Object>> frames)
    throws IOException {
        final ScriptEngine engine = new ScriptEngineManager().getEngineByName("js");
        final ScriptEngineFactory factory = engine.getFactory();
        final Console console = console();
        final StringBuilder buffer = new StringBuilder();
        String line;

        System.out.printf("%s-%s: %s %s\n",
                factory.getLanguageName(), factory.getLanguageVersion(),
                factory.getEngineName(), factory.getEngineVersion());

        try {
            engine.eval("var DataFrame = Packages.joinery.DataFrame");
        } catch (final ScriptException se) {
            throw new RuntimeException(se);
        }

        if (!frames.isEmpty()) {
            engine.put("frames", frames.toArray());
        }

        while ((line = console.readLine("> ")) != null) {
            buffer.setLength(0);
            buffer.append(line);
            while (!complete(buffer.toString()) && (line = console.readLine("  ")) != null) {
                buffer.append(line);
            }

            try {
                final Object result = engine.eval(buffer.toString());
                if (result != null) {
                    System.out.println(result);
                }
            } catch (final Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private static Console console()
    throws IOException {
        try {
            return new JLineConsole();
        } catch (final NoClassDefFoundError ncd) {
            return new Console();
        }
    }

    private static class Console {
        private final BufferedReader reader;

        private Console()
        throws IOException {
            reader = new BufferedReader(new InputStreamReader(System.in));
        }

        public String readLine(final String prompt)
        throws IOException {
            System.out.print(prompt);
            return reader.readLine();
        }
    }

    private static class JLineConsole
    extends Console
    implements Runnable {
        private final ConsoleReader console;

        private JLineConsole()
        throws IOException {
            console = new ConsoleReader();
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
    }

    /**
     * This is a crude way to detect partial expressions, it is
     * pretty inefficient, but it is unlikely anyone can type fast
     * enough to care.
     *
     * @param expr the script expression
     * @return true if the expression appears to be complete
     */
    private static boolean complete(final String expr) {
        final Stack<Character> s = new Stack<>();
        final StringCharacterIterator it = new StringCharacterIterator(expr);
        for (char c = it.first(); c != CharacterIterator.DONE; c = it.next()) {
            switch (c) {
                case '(': case '[': case '{':
                    s.push(c);
                    break;
                case ')': case ']': case '}':
                    if (s.isEmpty()) return true;
                    s.pop();
                    break;
                case '"': case '\'':
                    final char top = s.isEmpty() ? '\0' : s.peek();
                    switch (top) {
                        case '"': case '\'':
                            if (s.isEmpty()) return true;
                            s.pop();
                            break;
                        default:
                            s.push(c);
                            break;
                    }
                    break;
                case '\\':
                    it.next();
                    break;
            }
        }
        return s.isEmpty();
    }
}
