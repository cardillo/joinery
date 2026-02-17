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

import java.awt.Color;
import java.awt.Container;
import java.awt.GridLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.XChartPanel;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries;
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle;
import org.knowm.xchart.style.lines.SeriesLines;
import org.knowm.xchart.style.markers.SeriesMarkers;

import joinery.DataFrame;
import joinery.DataFrame.PlotType;

public class Display {
    @SuppressWarnings("unchecked")
    public static <C extends Container, V> C draw(final DataFrame<V> df, final C container, final PlotType type) {
        final List<XChartPanel<?>> panels = new LinkedList<>();
        final DataFrame<Number> numeric = df.numeric().fillna(0);
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

        if (PlotType.BAR.equals(type)) {
            final CategoryChart chart = new CategoryChartBuilder().build();
            chart.getStyler().setDatePattern(dateFormat(xdata));
            for (final Object col : numeric.columns()) {
                chart.addSeries(String.valueOf(col), xdata, numeric.col(col));
            }
            panels.add(new XChartPanel<>(chart));
        } else if (EnumSet.of(PlotType.GRID, PlotType.GRID_WITH_TREND).contains(type)) {
            for (final Object col : numeric.columns()) {
                final XYChart chart = new XYChartBuilder()
                    .width(800 / cols)
                    .height(800 / cols)
                    .title(String.valueOf(col))
                    .build();
                chart.getStyler().setDefaultSeriesRenderStyle(xyRenderStyle(type));
                final XYSeries series = chart.addSeries(String.valueOf(col), xdata, numeric.col(col));
                if (type == PlotType.GRID_WITH_TREND) {
                    addTrend(chart, series, xdata);
                    series.setLineStyle(SeriesLines.NONE);
                }
                chart.getStyler().setLegendVisible(false);
                chart.getStyler().setDatePattern(dateFormat(xdata));
                panels.add(new XChartPanel<>(chart));
            }
        } else {
            final XYChart chart = new XYChartBuilder().build();
            chart.getStyler().setDefaultSeriesRenderStyle(xyRenderStyle(type));
            chart.getStyler().setDatePattern(dateFormat(xdata));
            switch (type) {
                case SCATTER: case SCATTER_WITH_TREND: case LINE_AND_POINTS: break;
                default: chart.getStyler().setMarkerSize(0); break;
            }

            for (final Object col : numeric.columns()) {
                final XYSeries series = chart.addSeries(String.valueOf(col), xdata, numeric.col(col));
                if (type == PlotType.SCATTER_WITH_TREND) {
                    addTrend(chart, series, xdata);
                    series.setLineStyle(SeriesLines.NONE);
                }
            }

            panels.add(new XChartPanel<>(chart));
        }

        if (panels.size() > 1) {
            container.setLayout(new GridLayout(rows, cols));
        }
        for (final XChartPanel<?> p : panels) {
            container.add(p);
        }

        return container;
    }

    public static <V> void plot(final DataFrame<V> df, final PlotType type) {
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                final JFrame frame = draw(df, new JFrame(title(df)), type);
                frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
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

    private static XYSeriesRenderStyle xyRenderStyle(final PlotType type) {
        switch (type) {
            case AREA:                  return XYSeriesRenderStyle.Area;
            case GRID:
            case SCATTER:               return XYSeriesRenderStyle.Scatter;
            case SCATTER_WITH_TREND:
            case GRID_WITH_TREND:
            case LINE:
            case LINE_AND_POINTS:
            default:                    return XYSeriesRenderStyle.Line;
        }
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

    private static void addTrend(final XYChart chart, final XYSeries series, final List<Object> xdata) {
        final SimpleRegression model = new SimpleRegression();
        final double[] ydata = series.getYData();
        for (int x = 0; x < ydata.length; x++) {
            model.addData(x, ydata[x]);
        }
        final Color mc = series.getMarkerColor();
        final Color c = new Color(mc.getRed(), mc.getGreen(), mc.getBlue(), 0x60);
        final XYSeries trend = chart.addSeries(series.getName() + " (trend)",
                Arrays.asList(xdata.get(0), xdata.get(xdata.size() - 1)),
                Arrays.asList(model.predict(0), model.predict(xdata.size() - 1))
            );
        trend.setLineColor(c);
        trend.setMarker(SeriesMarkers.NONE);
    }
}
