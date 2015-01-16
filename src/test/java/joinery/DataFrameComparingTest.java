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

import java.io.IOException;

import joinery.impl.Comparing;

import org.junit.Before;
import org.junit.Test;

public class DataFrameComparingTest {
    private DataFrame<Object> left, right;

    @Before
    public void setUp()
    throws IOException {
        left = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("left.csv"));
        right = DataFrame.readCsv(ClassLoader.getSystemResourceAsStream("right.csv"));
    }

    @Test
    public void test() {
        System.out.println(Comparing.compare(left, right));
    }
}
