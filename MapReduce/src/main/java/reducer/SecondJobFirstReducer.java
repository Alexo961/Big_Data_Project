package reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import objects.ActionObject;
import objects.JoinObject;
import support.JobSupports;
import support.ObjectSupports;

public class SecondJobFirstReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String byYearSectorTicker = key.toString();
		String year = byYearSectorTicker.split("_")[0];
		String sector = byYearSectorTicker.split("_")[1];
		
		//System.out.println("FirstReducer");
		//System.out.println(key.toString());

		JoinObject first = null;
		JoinObject last = null;
		int count = 0;
		double variation = 0;
		long volumeSum = 0;
		double quotationSum = 0;
		double volumeAvg = 0;
		double quotationAvg = 0;

		for (Text value : values) {
			//System.out.println(value.toString());
			ActionObject ao = ObjectSupports.textToActionObject(value);
			if (ao != null && ao instanceof JoinObject) {
				JoinObject jo = (JoinObject) ObjectSupports.textToActionObject(value);
				first = JobSupports.first(first, jo);
				last = JobSupports.last(last, jo);
				quotationSum += JobSupports.quotationCalc(jo);
				volumeSum += jo.getStock().getVolume();
				count++;
			}
		}
		
		variation = JobSupports.variationCalc(first, last);
		volumeAvg = (volumeSum / count);
		quotationAvg = (quotationSum / count);
		String byYearSector = year + "_" + sector;
		String outVal = volumeAvg + "_" + variation + "_" + quotationAvg; 
		context.write(new Text(byYearSector), new Text(outVal));
		
	}
}
