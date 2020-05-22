package reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import objects.ActionObject;
import objects.JoinObject;
import support.JobSupports;
import support.ObjectSupports;

public class ThirdJobFirstReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String byYearNameTicker = key.toString();
		String year = byYearNameTicker.split("_")[0];
		String name = byYearNameTicker.split("_")[1];
		
		System.out.println("FirstReducer");
		System.out.println(key.toString());

		JoinObject first = null;
		JoinObject last = null;
		double variation = 0;

		for (Text value : values) {
			System.out.println(value.toString());
			ActionObject ao = ObjectSupports.textToActionObject(value);
			if (ao != null && ao instanceof JoinObject) {
				JoinObject jo = (JoinObject) ObjectSupports.textToActionObject(value);
				first = JobSupports.first(first, jo);
				last = JobSupports.last(last, jo);
			}
		}
		
		variation = JobSupports.variationCalc(first, last);
		String byYearName = year + "_" + name;
		String outVal = Double.toString(variation); 
		context.write(new Text(byYearName), new Text(outVal));
		
	}
}
