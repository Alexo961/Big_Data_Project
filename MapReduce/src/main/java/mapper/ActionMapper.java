package mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import objects.ActionObject;
import objects.StockObject;
import support.ObjectSupports;

public class ActionMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text ticker;

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		try {
			String line = value.toString();
			List<String> list = Arrays.asList(line.split(","));
			String tickerString = list.get(0);
			if (tickerString.equals("ticker")) {
				System.out.println("SKIPPED FIRST LINE");
			}
			else {
				ActionObject ao = ObjectSupports.textToActionObject(value);
				if (ao != null && ao.hasAllFields()) {
					if (ao instanceof StockObject) {
						StockObject so = (StockObject) ao;
						if(so.getDate().getYear() >= 2008) {
							ticker = new Text(tickerString);
							context.write(ticker, value);
						}
						/*
						else
							System.out.println("Stock older than 2008");
							*/
					}
					else {
						ticker = new Text(tickerString);
						context.write(ticker, value);
					}
				}
			}
		}

		catch(Exception exc) {
			System.out.println("BAD LINE SKIPPED");
		}

	}
}
