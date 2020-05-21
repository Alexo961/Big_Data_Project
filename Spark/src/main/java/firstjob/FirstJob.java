package firstjob;

import java.time.LocalDate;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import functions.LineToStock;
import objects.StockObject;
import scala.Tuple2;
import supports.DateSupports;
import supports.ObjectSupports;
import supports.StockFunctions;

public class FirstJob {

	public static void main(String[] args) {
		
		String hdfsPath = "hdfs://localhost:9000/";
		String path = args[0];
		String csvFile = hdfsPath + path;
		
		SparkConf conf = new SparkConf().setAppName("FirstJob");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Data
		JavaRDD<String> data = sc.textFile(csvFile).cache();
		
		//Map(Ticker, Stock from 2008)
		JavaPairRDD<String, StockObject> stocks = data.mapToPair(new LineToStock())
				.filter(t -> t != null).filter(t -> DateSupports.fromYear2008(t._2));
		
		//Map(Ticker, first Stock from 2008)
		JavaPairRDD<String, StockObject> firsts = stocks
				.reduceByKey((a, b) -> DateSupports.firstStock(a, b));
		
		//Map(Ticker, last Stock before 2018)
		JavaPairRDD<String, StockObject> lasts = stocks
				.reduceByKey((a, b) -> DateSupports.lastStock(a, b));
		
		//Map(Ticker, (first Stock, last Stock))
		JavaPairRDD<String, Tuple2<StockObject, StockObject>> firstsLasts = firsts.join(lasts);
		
		//Map(Ticker, percentage Variation between first Stock Close and last Stock Close)
		JavaPairRDD<String, Double> variations = firstsLasts
				.mapToPair(t -> 
				new Tuple2<String, Double>(t._1, StockFunctions.totalVariation(t._2._1, t._2._2)))
				.mapToPair(t -> t.swap())
				.sortByKey()
				.mapToPair(t -> t.swap());
		
		//Map(Ticker, Volume)
		JavaPairRDD<String, Long> volumes = stocks
				.mapToPair(t -> new Tuple2<String, Long>(t._1, t._2.getVolume()));
		
		//Map(Ticker, sum of Volumes of that Ticker)
		JavaPairRDD<String, Long> sumOfVolumes = volumes
				.reduceByKey((a, b) -> (a + b));
		
		//Map(Ticker, quantity of that Ticker)
		JavaPairRDD<String, Integer> counts = stocks
				.mapToPair(t -> new Tuple2<String, Integer>(t._1, 1))
				.reduceByKey((a, b) -> (a + b));
		
		//Map(Ticker, average of Volumes of that Ticker)
		JavaPairRDD<String, Double> volMeds = sumOfVolumes.join(counts)
				.mapToPair(t -> new Tuple2<String, Double>(t._1, Double.valueOf(t._2._1 / t._2._2)));
		
		JavaPairRDD<String, Tuple2<Double, Double>> minMaxes = stocks
				.reduceByKey((a ,b) -> a)
				.mapToPair(t -> 
				new Tuple2<String, Tuple2<Double, Double>>(t._1,
						new Tuple2<Double, Double>(t._2.getLow(), t._2.getHigh())));
	
		JavaPairRDD<String, String> minMaxesString = minMaxes
				.mapToPair(t -> 
				new Tuple2<String, String>
				(t._1, new String(t._2._1.toString() + "," + t._2._2.toString())));
		
		JavaPairRDD<String, String> result1 = minMaxesString
				.join(volMeds)
				.mapToPair(t -> new Tuple2<String, String>(t._1, t._2._1.concat(",").concat(t._2._2.toString())));
		
		
		//variations.foreach(t -> System.out.println(t._1 + "\t" + t._2.toString()));
		
		
		
		JavaPairRDD<String, String> result = variations
				.join(result1)
				.mapToPair(t -> new Tuple2<String, String>(t._1, t._2._1.toString().concat(",").concat(t._2._2)));
	
		result.foreach(t -> System.out.println(t._1 + "\t" + t._2));
	}
}
