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

import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class DataFrameShapingTest {
    private DataFrame<Object> df;

    @Before
    public void setUp()
    throws IOException {
        df = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("shaping.csv"));
    }

    @Test
    public void testReshapeIntAddColumns() {
        assertEquals(5, df.reshape(df.length(), 5).size());
    }

    @Test
    public void testReshapeIntTruncateColumns() {
        assertEquals(2, df.reshape(df.length(), 2).size());
    }

    @Test
    public void testReshapeIntAddRows() {
        assertEquals(10, df.reshape(10, df.size()).length());
    }

    @Test
    public void testReshapeIntTruncateRows() {
        assertEquals(4, df.reshape(4, df.size()).length());
    }

    @Test
    public void testReshapeStringAddColumns() {
        assertEquals(5, df.reshape(df.index(), Arrays.<Object>asList("a", "b", "c", "d", "e")).size());
    }

    @Test
    public void testReshapeStringTruncateColumns() {
        assertEquals(2, df.reshape(df.index(), Arrays.<Object>asList("a", "b")).size());
    }

    @Test
    public void testReshapeStringAddRows() {
        assertEquals(10, df.reshape(Arrays.<Object>asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"), df.columns()).length());
    }

    @Test
    public void testReshapeStringTruncateRows() {
        assertEquals(4, df.reshape(Arrays.<Object>asList("1", "2", "3", "4"), df.columns()).length());
    }
}
