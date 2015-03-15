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

import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;

import joinery.DataFrame;
import joinery.DataFrame.PlotType;

import com.xeiam.xchart.Chart;
import com.xeiam.xchart.ChartBuilder;
import com.xeiam.xchart.StyleManager.ChartType;
import com.xeiam.xchart.XChartPanel;

public class Display {
    private static ChartType chartType(final PlotType type) {
        switch (type) {
            case AREA:      return ChartType.Area;
            case BAR:       return ChartType.Bar;
            case GRID:
            case SCATTER:   return ChartType.Scatter;
            default:        return ChartType.Line;
        }
    }

    public static <V> void plot(final DataFrame<V> df, final PlotType type) {
        final List<XChartPanel> panels = new LinkedList<>();
        final DataFrame<Number> numeric = df.numeric();
        final int rows = (int)Math.ceil(Math.sqrt(numeric.size()));
        final int cols = numeric.size() / rows + 1;

        final List<Object> xdata = new ArrayList<>(df.length());
        final Iterator<Object> it = df.index().iterator();
        for (int i = 0; i < df.length(); i++) {
            final Object value = it.hasNext() ? it.next(): i;
            if (value instanceof Number || value instanceof Date) {
                xdata.add(value);
            } else if (PlotType.BAR.equals(type)) {
                xdata.add(String.valueOf(value));
            } else {
                xdata.add(i);
            }
        }

        if (type == PlotType.GRID) {
            for (final Object col : numeric.columns()) {
                final Chart chart = new ChartBuilder()
                    .chartType(chartType(type))
                    .width(800 / cols)
                    .height(800 / cols)
                    .title(String.valueOf(col))
                    .build();
                chart.addSeries(String.valueOf(col), xdata, numeric.col(col));
                chart.getStyleManager().setLegendVisible(false);
                chart.getStyleManager().setDatePattern(dateFormat(xdata));
                panels.add(new XChartPanel(chart));
            }
        } else {
            final Chart chart = new ChartBuilder()
                .chartType(chartType(type))
                .build();

            chart.getStyleManager().setDatePattern(dateFormat(xdata));
            switch (type) {
                case SCATTER: case LINE_AND_POINTS: break;
                default: chart.getStyleManager().setMarkerSize(0); break;
            }

            for (final Object col : numeric.columns()) {
                chart.addSeries(String.valueOf(col), xdata, numeric.col(col));
            }

            panels.add(new XChartPanel(chart));
        }

        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                final JFrame frame = new JFrame(title(df));
                frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
                if (panels.size() > 1) {
                    frame.setLayout(new GridLayout(rows, cols));
                }
                for (final XChartPanel p : panels) {
                    frame.add(p);
                }
                frame.pack();
                frame.setVisible(true);
            }
        });
    }

    public static <V> void show(final DataFrame<V> df) {
        final List<Object> columns = new ArrayList<>(df.columns());
        final List<Class<?>> types = df.types();
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                final JFrame frame = new JFrame(title(df));
                final JTable table = new JTable(
                        new AbstractTableModel() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public int getRowCount() {
                                return df.length();
                            }

                            @Override
                            public int getColumnCount() {
                                return df.size();
                            }

                            @Override
                            public Object getValueAt(final int row, final int col) {
                                return df.get(row, col);
                            }

                            @Override
                            public String getColumnName(final int col) {
                                return String.valueOf(columns.get(col));
                            }

                            @Override
                            public Class<?> getColumnClass(final int col) {
                                return types.get(col);
                            }
                        }
                    );
                table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
                frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
                frame.add(new JScrollPane(table));
                frame.pack();
                frame.setVisible(true);
            }
        });
    }

    private static final String title(final DataFrame<?> df) {
        return String.format(
                "%s (%d rows x %d columns)",
                df.getClass().getCanonicalName(),
                df.length(),
                df.size()
            );
    }

    private static final String dateFormat(final List<Object> xdata) {
        final int[] fields = new int[] {
                Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH,
                Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND
            };
        final String[] formats = new String[] {
                " yyy", "-MMM", "-d", " H", ":mm", ":ss"
            };
        final Calendar c1 = Calendar.getInstance(), c2 = Calendar.getInstance();

        if (!xdata.isEmpty() && xdata.get(0) instanceof Date) {
            String format = "";
            int first = 0, last = 0;
            c1.setTime(Date.class.cast(xdata.get(0)));
            // iterate over all x-axis values comparing dates
            for (int i = 1; i < xdata.size(); i++) {
                // early exit for non-date elements
                if (!(xdata.get(i) instanceof Date)) return formats[0].substring(1);
                c2.setTime(Date.class.cast(xdata.get(i)));

                // check which components differ, those are the fields to output
                for (int j = 1; j < fields.length; j++) {
                    if (c1.get(fields[j]) != c2.get(fields[j])) {
                        first = Math.max(j - 1, first);
                        last = Math.max(j, last);
                    }
                }
            }

            // construct a format string for the fields that differ
            for (int i = first; i <= last && i < formats.length; i++) {
                format += format.isEmpty() ? formats[i].substring(1) : formats[i];
            }

            return format;
        }

        return formats[0].substring(1);
    }
}
