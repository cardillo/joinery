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

package joinery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import joinery.impl.Shell;
import joinery.impl.js.DataFrameAdapter;

import org.junit.Before;
import org.junit.Test;
import org.mozilla.javascript.EvaluatorException;

public class DataFrameShellTest {
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws IOException {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("grouping.csv"));
    }

    @Test
    public void testNoInput()
    throws IOException {
        input("");
        assertNull(Shell.repl(Collections.<DataFrame<Object>>emptyList()));
    }

    @Test
    public void testNewDataFrame()
    throws IOException {
        input("new DataFrame()");
        assertEquals(
                DataFrameAdapter.class,
                Shell.repl(Collections.<DataFrame<Object>>emptyList()).getClass()
            );
    }

    @Test
    public void testMultiLineExpression()
    throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb.append("new DataFrame()\n")
          .append(".append([\n")
          .append("  1,\n")
          .append("  2,\n")
          .append("  3,\n")
          .append("  4\n")
          .append("])");
        input(sb.toString());
        assertEquals(
                DataFrameAdapter.class,
                Shell.repl(Collections.<DataFrame<Object>>emptyList()).getClass()
            );
    }

    @Test
    public void testInvalidExpressions()
    throws IOException {
        for (final String in : Arrays.asList(
                    "[", "]", "{", "}", "(", ")", "\"", "'")) {
            input(in);
            assertEquals(
                    EvaluatorException.class,
                    Shell.repl(Collections.<DataFrame<Object>>emptyList()).getClass()
                );
        }
    }

    @Test
    public void testMultiLineFunction()
    throws IOException {
        final StringBuilder sb = new StringBuilder();
        sb.append("frames[0].groupBy(function (row) {\n")
          .append("  return row.get(1)\n")
          .append("})");
        input(sb.toString());
        assertEquals(
                DataFrameAdapter.class,
                Shell.repl(Arrays.asList(df)).getClass()
            );
    }

    @Test
    public void testDataFrameArguments()
    throws IOException {
        input("frames[0]");
        assertEquals(
                DataFrameAdapter.class,
                Shell.repl(Arrays.asList(df)).getClass()
            );
    }

    @Test
    public void testLastResult()
    throws IOException {
        input("frames[0]\n_");
        assertEquals(
                DataFrameAdapter.class,
                Shell.repl(Arrays.asList(df)).getClass()
            );
    }

    private static final void input(final String input) {
        System.setIn(new ByteArrayInputStream(input.getBytes()));
    }
}
