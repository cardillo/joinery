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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import joinery.DataFrame.PlotType;

public class DataFramePlotTest {
    public static void main(final String[] args)
    throws IOException {
        final Random rnd = new Random();
        final DataFrame<Object> df = new DataFrame<>(
                Arrays.<Object>asList("one", "two", "three"),
                Arrays.<Object>asList("name", "value1", "value2", "value3", "value4"),
                Arrays.asList(
                        Arrays.<Object>asList("alpha", "beta", "delta", "gamma"),
                        Arrays.<Object>asList(
                                rnd.nextInt(100),
                                rnd.nextInt(100),
                                rnd.nextInt(100),
                                rnd.nextInt(100)
                            ),
                        Arrays.<Object>asList(
                                rnd.nextInt(50),
                                rnd.nextInt(50),
                                rnd.nextInt(50),
                                rnd.nextInt(50)
                            ),
                        Arrays.<Object>asList(
                                rnd.nextInt(25),
                                rnd.nextInt(25),
                                rnd.nextInt(25),
                                rnd.nextInt(25)
                            ),
                        Arrays.<Object>asList(
                                rnd.nextInt(10),
                                rnd.nextInt(10),
                                rnd.nextInt(10),
                                rnd.nextInt(10)
                            )
                    )
            );

        df.plot();
        df.plot(PlotType.SCATTER);
        df.plot(PlotType.SCATTER_WITH_TREND);
        df.plot(PlotType.AREA);
        df.plot(PlotType.BAR);
        df.plot(PlotType.LINE_AND_POINTS);
        df.plot(PlotType.GRID);
        df.plot(PlotType.GRID_WITH_TREND);

        final Calendar cal = Calendar.getInstance();
        cal.clear();
        cal.set(2014, 11, 15);
        final List<Object> dates = new ArrayList<>();
        for (int i = 0; i < df.length(); i++) {
            dates.add(cal.getTime());
            cal.add(Calendar.DATE, -1);
        }
        df.addColumn("date", dates).reindex("date").plot();
    }
}
