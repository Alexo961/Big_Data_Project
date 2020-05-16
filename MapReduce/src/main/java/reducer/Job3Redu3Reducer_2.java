package reducer;

import java.io.IOException;
import java.util.ArrayList;
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



	Map<Text,List<String>> map = new HashMap<Text, List<String>>();
	Map<String,List<String>> map2 = new HashMap<String, List<String>>();

	List <String> s;
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		
		 
		
		int i = 0;
		
		for (Text value : values) {
			    i ++;

			
			if(i == 3) {
			    s = map2.get((key.toString().split("_")[1]));
			    if(s == null)  s = new ArrayList<String>(); 
				
			    s.add(key.toString().split("_")[0]);
				map2.put(key.toString().split("_")[1],s);

		
		
			}
		}

		


         
		context.write(new Text(key.toString()), new Text(map2.toString()));

	}





}
