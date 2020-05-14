package reducer;

import java.io.IOException;
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



	Map<Double,List<String>> map = new HashMap<Double, List<String>>();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int cont =0;
		for (Text value : values) {
			

			cont  +=1;
			if (cont == 3) {
				List<String> s = map.get(key.toString().split("_")[1]);
				s.add(key.toString().split("_")[0]);
				map.put(new Double(key.toString().split("_")[1]),s);

		}
		

		}

		



		context.write(key, new Text(map.toString()));

	}





}
