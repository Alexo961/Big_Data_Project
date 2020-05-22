package reducer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import support.ObjectSupports;

public class ThirdJobThirdReducer extends Reducer<Text, Text, Text, Text>{
	

	private Map<String, List<String>> namesByYearVar = new HashMap<>();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) {
		
		String name = key.toString();
		
		List<String> year_vars = new ArrayList<>();
		
		for(Text value : values) {
			String year_var = value.toString();
			year_vars.add(year_var);
		}
		
		if (year_vars.size() == 3) {
			Collections.sort(year_vars);
			String year_varStr = ObjectSupports.listToString(year_vars);
			List<String> names = namesByYearVar.get(year_varStr);
			if (names == null)
				names = new ArrayList<>();
			names.add(name);
			namesByYearVar.put(year_varStr, names);
		}
	}
	
	@Override
	public void cleanup(Context context) {
		
		for(String year_vars : namesByYearVar.keySet()) {
			List<String> namesList = namesByYearVar.get(year_vars);
			if ((namesList != null)
					&& (namesList.size() > 1)) {
				
				String names = ObjectSupports.listToString(namesList);
				System.out.println(names + ": " + year_vars);
			}
		}
	}
}
