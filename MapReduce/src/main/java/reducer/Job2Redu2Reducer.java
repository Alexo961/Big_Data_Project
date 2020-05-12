package reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
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

public class Job2Redu2Reducer
extends Reducer<Text, Text, Text, Text> {



	int count;
	Double sum_vol;
	Double sum_quot;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Map<Text,Double> map_quotation = new HashMap<Text, Double>();
		sum_quot=0.0;
		count =0;
		sum_vol =0.0;
		String[] firstLast = null;

		for (Text value : values) {
			String[] split = value.toString().split("_");
			count += 1;
			sum_vol +=  Double.parseDouble(split[0]);
			sum_quot += Double.parseDouble(split[5]);
			firstLast = JobSupports.firstLast(firstLast, value.toString());

         Double vol_med = JobSupports.medVolAnn(count, sum_vol);
			map_quotation = JobSupports.media_per_ticker(new Double(split[5]), new Text(split [6]), map_quotation);

		}

		Double vol_med = JobSupports.medVolAnn(count, sum_vol);
		Double quot_med = JobSupports.medVolAnn(count, sum_quot);


		context.write(key, new Text(vol_med+","++","+JobSupports.stampa_mappa(map_quotation)));

	}





}
