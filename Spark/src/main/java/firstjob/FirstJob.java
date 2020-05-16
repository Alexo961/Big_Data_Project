package firstjob;

import java.time.LocalDate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import functions.LineToStock;
import objects.StockObject;
import supports.ObjectSupports;

public class FirstJob {

	public static void main(String[] args) {
		
		String hdfsPath = "hdfs://localhost:9000/";
		String path = args[0];
		String csvFile = hdfsPath + path;
		
		SparkConf conf = new SparkConf().setAppName("FirstJob");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> data = sc.textFile(csvFile).cache();
		JavaPairRDD<String, StockObject> dates = data.mapToPair(new LineToStock());
	}
}
