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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

public class DataFrameManipulationTest {
    private DataFrame<Object> df;

    @Before
    public final void setUp() {
        df = new DataFrame<Object>();
    }

    @Test
    public final void testAdd() {
        df.add("test");
        assertEquals(
                "size is equal to number of columns",
                1,
                df.size()
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public final void testAddExisting() {
        df.add("test")
          .add("test");
    }

    @Test
    public final void testDrop() {
        df.add("test1").add("test2");
        final DataFrame<Object> newDf = df.drop("test1");
        assertEquals(
                "original size is equal to number of columns",
                2,
                df.size()
            );
        assertArrayEquals(
                "original column list is correct",
                new Object[] { "test1", "test2" },
                df.columns().toArray()
            );
        assertEquals(
                "new size is equal to number of columns",
                1,
                newDf.size()
            );
        assertArrayEquals(
                "new column list is correct",
                new Object[] { "test2" },
                newDf.columns().toArray()
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public final void testDropInvalid() {
        df.drop("does-not-exist");
    }

    @Test
    public final void testAppend() {
        df.add("test").append(Arrays.<Object>asList(1));
        assertEquals(
                "size is equal to number of columns",
                1,
                df.size()
            );
        assertEquals(
                "length is equal to number of rows",
                1,
                df.length()
            );
        assertArrayEquals(
                "column values are correct",
                new Object[] { 1 },
                df.col(0).toArray()
            );
    }

    @Test(expected=IllegalArgumentException.class)
    public final void testAppendExisting() {
        df.add("test")
          .append("one", Collections.emptyList())
          .append("one", Collections.emptyList());
    }
}
