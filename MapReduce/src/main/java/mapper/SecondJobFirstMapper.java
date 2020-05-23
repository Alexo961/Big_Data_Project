package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import objects.ActionObject;
import objects.JoinObject;
import support.ObjectSupports;

public class SecondJobFirstMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString().split("\t")[1];
		Text realValue = new Text(line);
		
		ActionObject ao = ObjectSupports.textToActionObject(realValue);
		
		if (ao != null && ao instanceof JoinObject) {
			JoinObject jo = (JoinObject) ao;
			Integer year = jo.getStock().getDate().getYear();
			String sector = jo.getSector().getSector();
			String ticker = jo.getSector().getTicker();
			String byYearSectorTicker = year + "_" + sector + "_" + ticker;
			context.write(new Text(byYearSectorTicker), realValue);
		}
	}
}
