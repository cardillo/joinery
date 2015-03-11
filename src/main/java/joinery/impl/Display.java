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

        final List<Number> xdata = new ArrayList<>(df.length());
        for (int i = 0; i < df.length(); i++) {
            xdata.add(i);
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
                panels.add(new XChartPanel(chart));
            }
        } else {
            final Chart chart = new ChartBuilder()
                .chartType(chartType(type))
                .build();

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
}
