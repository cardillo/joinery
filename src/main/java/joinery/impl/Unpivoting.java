package joinery.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import joinery.DataFrame;

public class Unpivoting {
	@SuppressWarnings("unchecked")
	public static <V> DataFrame<V> melt(
			final DataFrame<V> df, final Integer[] idVars,
			final Integer[] valueVars, final String varName,
			final String valueName) {
		Integer size = df.size();
		List<Object> dfColumns = new ArrayList<Object>(df.columns());
		List<Object> columns = new ArrayList<Object>();
		
		for (int i=0; i < valueVars.length; i++) {
			if (valueVars[i] >= size) {
				throw new IllegalArgumentException(
						"value variable at index [" + i + 
						"] does not exist. Value variables: " +
						Arrays.deepToString(valueVars));
			}
		}
		
		for (int i=0; i < idVars.length; i++) {
			if (idVars[i] >= size) {
				throw new IllegalArgumentException(
						"id variable at index [" + i + 
						"] does not exist. Id variables: " + 
						Arrays.deepToString(idVars));
			} else {
				columns.add(dfColumns.get(idVars[i]));
			}
		}
		
		columns.add(varName);			// Add Variable column name
		columns.add(valueName);		// Add Value column name
		
		System.out.println(columns);
		
		DataFrame<V> unpivot = new DataFrame<V>(columns);
				
		for (int i=0; i < df.length(); i++) {
			for (Integer valueVar : valueVars) {
				List<V> rowData = new ArrayList<V>();
				for (Integer idVar : idVars) {
					rowData.add(df.get(i, idVar));
				}
				rowData.add((V) dfColumns.get(valueVar));		// Add column name from main df under Variable Column
				rowData.add(df.get(i, valueVar));		// Add value from main df under Value Column
				unpivot.append(rowData);
			}
		}
		
		return unpivot;
	}
}
