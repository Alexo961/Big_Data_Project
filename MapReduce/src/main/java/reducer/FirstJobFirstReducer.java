package reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import objects.StockObject;
import outputobjects.JobOneOutOne;
import support.JobOneSupports;
import support.ObjectSupports;

public class FirstJobFirstReducer
extends Reducer<Text, Text, Text, Text> {

	StockObject stock;
	double variation;
	double minimum;
	double maximum;
	double mediumVolume;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		double[] minMaxPrice = null;
		StockObject[] firstLastStock = null;
		long sumOfVolumes = 0;
		int numValues = 0;
		System.out.println(key.toString());
		for (Text value : values) {
			stock = (StockObject) ObjectSupports.textToActionObject(value);
			if (stock != null) {
				firstLastStock = JobOneSupports.firstLastStock(firstLastStock, stock);
				sumOfVolumes += stock.getVolume().longValue();
				numValues++;
				minMaxPrice = JobOneSupports
						.minMaxPrice(minMaxPrice, stock.getClose().doubleValue());
			}
		}
		
		mediumVolume = JobOneSupports.mediumVolume(sumOfVolumes, numValues);
		
		if (firstLastStock[0] == null)
			System.out.println("OPEN NULL " + key.toString());
		if (firstLastStock[1] == null)
			System.out.println("CLOSE NULL " + key.toString());
		variation = JobOneSupports
				.variationQuotation(firstLastStock[0].getOpen(), firstLastStock[1].getClose());
		minimum = minMaxPrice[0];
		maximum = minMaxPrice[1];
		context.write(key, JobOneSupports.firstOutput(variation, minimum, maximum, mediumVolume));
	}
}
