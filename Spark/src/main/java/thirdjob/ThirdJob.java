package thirdjob;


import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import comparators.Tuple2IntDouComparator;
import functions.LineToSector;
import functions.LineToStock;
import objects.JoinObject;
import objects.SectorObject;
import objects.StockObject;
import scala.Tuple2;
import scala.Tuple3;
import supports.DateSupports;
import supports.GeneralSupports;
import supports.JoinObjectFunctions;
import supports.ObjectSupports;

public class ThirdJob {

	public static void main(String[] args) {

		String hdfsPath = "hdfs://localhost:9000/";
		String pathSector = args[0];
		String pathStock = args[1];
		String sectorFile = hdfsPath + pathSector;
		String stockFile = hdfsPath + pathStock;

		SparkConf conf = new SparkConf().setAppName("SecondJob");
		JavaSparkContext sc = new JavaSparkContext(conf);

		//Data historical_stocks and historical_stock_prices
		JavaRDD<String> dataSector = sc.textFile(sectorFile).cache();
		JavaRDD<String> dataStock = sc.textFile(stockFile).cache();

		//Map(Ticker, Stock from 2008)
		JavaPairRDD<String, StockObject> stocks = dataStock.mapToPair(new LineToStock())
				.filter(t -> t != null)
				.filter(t -> t._2.hasAllFields())
				.filter(t -> DateSupports.fromYear2016(t._2));

		//Map(Ticker, Sector)
		JavaPairRDD<String, SectorObject> sectors = dataSector.mapToPair(new LineToSector())
				.filter(t -> t != null)
				.filter(t -> t._2.hasAllFields());

		//sectors.foreach(t -> System.out.println(t._1 + "\t" + t._2.toString()));

		//Join(Sector, Stock on Ticker)
		JavaPairRDD<String, Tuple2<SectorObject, StockObject>> tupleJoin = 
				sectors.join(stocks);

		//JoinObject
		JavaPairRDD<String, JoinObject> join = 
				tupleJoin.mapToPair(t ->
				new Tuple2<String, JoinObject>(t._1, new JoinObject(t._2._1, t._2._2)));

		//Map((Name, Ticker), JoinObject))
		JavaPairRDD<Tuple2<String, String>, JoinObject> byName = join
				.mapToPair(t -> 
				new Tuple2<Tuple2<String,String>, JoinObject>(
						new Tuple2<String, String>(
								t._2.getSector().getName(),
								t._1),
						t._2));

		//Map((Year,Name,Ticker)), JoinObject)
		JavaPairRDD<Tuple3<Integer, String, String>, JoinObject> byYearByName = byName
				.mapToPair(t -> 
				new Tuple2<Tuple3<Integer, String, String>, JoinObject>(
						new Tuple3<Integer, String, String>(
								t._2.getStock().getDate().getYear(),
								t._1._1,
								t._1._2),
						t._2));

		//first joinobject by date for every year and every name and every ticker
		//Map((Year, Name, Ticker)), JoinObject)
		JavaPairRDD<Tuple3<Integer, String, String>, JoinObject> firsts = byYearByName
				.reduceByKey((one, two) -> DateSupports.firstJoin(one, two));

		//last joinobject by date for every year and every name and every ticker
		//Map((Year, Name, Ticker)), JoinObject)
		JavaPairRDD<Tuple3<Integer, String, String>, JoinObject> lasts = byYearByName
				.reduceByKey((one, two) -> DateSupports.lastJoin(one, two));

		//first and last JoinObject by date for every year and every name and every ticker
		//Map((Year, Name, Ticker), (JoinObject, JoinObject))
		JavaPairRDD<Tuple3<Integer, String, String>,
		Tuple2<JoinObject, JoinObject>> firstsLasts = firsts.join(lasts);

		JavaPairRDD<Tuple3<Integer, String, String>, Double> variationsByYearNameTicker =
				firstsLasts
				.mapToPair(t ->
				new Tuple2<Tuple3<Integer, String, String>, Double>(
						t._1,
						JoinObjectFunctions.totalVariation(t._2._1, t._2._2)));

		JavaPairRDD<Tuple2<Integer, String>, Double> variationsByYearName = variationsByYearNameTicker
				.mapToPair(t ->
				new Tuple2<Tuple2<Integer, String>, Tuple2<Double, Integer>>(
						new Tuple2<Integer, String>(
								t._1._1(),
								t._1._2()),
						new Tuple2<Double, Integer>(
								t._2(),
								1)))
				.reduceByKey((t2a, t2b) ->
				new Tuple2<Double, Integer>(
						t2a._1() + t2b._1(),
						t2a._2() + t2b._2()))
				.mapToPair(t ->
				new Tuple2<Tuple2<Integer, String>, Double>(
						t._1(),
						t._2._1() / t._2._2()));

		JavaPairRDD<Tuple2<Integer, Double>, String> byYearVariation = variationsByYearName
				.mapToPair(t -> new Tuple2<Tuple2<Integer, Double>, String>(
						new Tuple2<Integer, Double>(
								t._1._1(),
								t._2()),
						t._1._2()));

		/*
		byYearVariation
		.sortByKey(new Tuple2IntDouComparator())
		.foreach(t ->
		System.out.println(t._1._1() + " " + t._1._2() + " " + t._2()));
		*/

		//Map(Name, (Year, Variation))
		JavaPairRDD<String, Tuple2<Integer, Double>> finalByName = byYearVariation
				.mapToPair(t -> t.swap());
		
		finalByName
		.sortByKey()
		.foreach(t ->
				System.out.println(t._1() + ": " + t._2._1().toString() + " " + t._2._2().toString()));

		//Map(Name, string:Year_Variation)
		JavaPairRDD<String, String> yearVariationAsStrings = finalByName
				.mapToPair(t ->
				new Tuple2<String, String>(
						t._1(),
						new String(t._2._1().toString() + "_" + t._2._2().intValue())));

		//Map(Name, iterable:string:Year_Variation)
		JavaPairRDD<String, Iterable<String>> yearVariationsIterable = yearVariationAsStrings
				.groupByKey();
		
		//Map(Name, list:string:Year_Variation)
		JavaPairRDD<String, List<String>> yearVariationsList = yearVariationsIterable
				.mapToPair(t ->
						new Tuple2<String, List<String>>(
								t._1(),
								GeneralSupports.iterableToList(t._2())))
				.filter(t -> t._2.size() == 3);
		
		yearVariationsList
		.sortByKey()
		.foreach(t ->
				System.out.println(t._1() + ": " + t._2().toString()));
		
		//Map(Name, string:Year_Variation's)
		JavaPairRDD<String, String> yearVariationAsBigString = yearVariationsList
				.mapToPair(t ->
						new Tuple2<String, String>(
								t._1(),
								ObjectSupports.listToString(t._2())));
		
		yearVariationAsBigString
		.sortByKey()
		.foreach(t -> System.out.println(t._1() + ": " + t._2()));
		
		//Map(string:Year_Variation's, list:Name)
		JavaPairRDD<String, Iterable<String>> namesList = yearVariationAsBigString
				.mapToPair(t -> t.swap())
				.groupByKey();
		
		namesList.foreach(new ThirdJobForEach());
	}
}
