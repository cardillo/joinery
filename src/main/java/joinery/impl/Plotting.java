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

import java.util.ArrayList;
import java.util.List;

import joinery.DataFrame;

import com.xeiam.xchart.Chart;
import com.xeiam.xchart.ChartBuilder;
import com.xeiam.xchart.SwingWrapper;

public class Plotting {
    public static <V> void display(final DataFrame<V> df) {
        final Chart chart = new ChartBuilder().build();
        final DataFrame<Number> numeric = df.numeric();
        final List<Number> xdata = new ArrayList<>(df.length());
        for (int i = 0; i < df.length(); i++) {
            xdata.add(i);
        }
        for (final String col : numeric.columns()) {
            chart.addSeries(col, xdata, numeric.col(col));
        }
        new SwingWrapper(chart).displayChart();
    }
}
