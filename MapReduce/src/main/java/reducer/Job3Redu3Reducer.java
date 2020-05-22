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

public class Job3Redu3Reducer
extends Reducer<Text, Text, Text, Text> {



	int count;
	Double sum_vol;
	Double sum_quot;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		//Map<Text,Double> map_quotation = new HashMap<Text, Double>();
		sum_quot=0.0;
		count =0;

		String[] firstLast = null;
		String nome_azienda = values.iterator().next().toString().split("_")[1];
		String  anno = key.toString().split("_")[1];

		for (Text value : values) {
			
			if (value.toString().split("_").length == 4) {
		//	System.out.println(value.toString());
			String[] split = value.toString().split("_");
			
			for (String piece : split) {
				System.out.println(piece);
				if (piece == null)
					System.out.println("PIECE NULLO");
			}
			firstLast = JobSupports.firstLast(firstLast, value.toString());


			//map_quotation = JobSupports.media_per_ticker(new Double(split[5]), new Text(split [6]), map_quotation);
			}
		}

		Double variation_quot = JobSupports.variationAnnualQuotation3(firstLast);



		context.write(key, new Text(anno +"_"+variation_quot.toString()+"_"+nome_azienda));

	}





}
