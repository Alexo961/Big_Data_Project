package reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import comparator.JobOneVariationComp;
import objects.StockObject;
import outputobjects.JobOneOutOne;
import support.JobSupports;
import support.ObjectSupports;

public class Job3Redu3Reducer_2
extends Reducer<Text, Text, Text, Text> {




		

	Map<String, List<String>> name_variations = new HashMap<>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		StringBuilder sb = new StringBuilder();

		int i = 0;
		for (Text value : values) {

			   

			
			
			sb.append(value.toString()).append("_");
			i ++;
		}
		sb.deleteCharAt(sb.length() -1);
		if(i == 3) {	
			List<String> list = name_variations.get(sb.toString());
			if (list == null)
				list = new ArrayList<>();
			list.add(key.toString());
			name_variations.put(sb.toString(), list);
		}
	}


	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {
		for (String key : name_variations.keySet()) {
			if (name_variations.get(key).size() > 1) {
				String replaced = key.replace("_", ", ");
				StringBuilder sb = new StringBuilder();
				sb.append("{");
				for (String name : name_variations.get(key)) {
					sb.append(name).append(", ");
				}
				sb.deleteCharAt(sb.length()-1);
				sb.append("}");
				context.write(new Text(sb.toString()), new Text(replaced));
			}
		}

	}
}
