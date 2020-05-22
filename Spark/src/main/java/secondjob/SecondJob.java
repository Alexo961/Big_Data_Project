package secondjob;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import comparators.Tuple2IntStrComparator;
import functions.ByYearBySectorFunction;
import functions.LineToSector;
import functions.LineToStock;
import objects.JoinObject;
import objects.SectorObject;
import objects.StockObject;
import scala.Tuple2;
import scala.Tuple3;
import supports.DateSupports;
import supports.JoinObjectFunctions;

public class SecondJob {

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
				.filter(t -> DateSupports.fromYear2008(t._2));
		
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
		
		//Map((Sector, Ticker), JoinObject))
		JavaPairRDD<Tuple2<String, String>, JoinObject> bySector = join
				.mapToPair(t -> 
				new Tuple2<Tuple2<String,String>, JoinObject>(
						new Tuple2<String, String>(
								t._2.getSector().getSector(),
								t._1),
						t._2));
		
		//Map((Year,Sector,Ticker)), JoinObject)
		JavaPairRDD<Tuple3<Integer, String, String>, JoinObject> byYearBySector = bySector
				.mapToPair(t -> 
				new Tuple2<Tuple3<Integer, String, String>, JoinObject>(
						new Tuple3<Integer, String, String>(
								t._2.getStock().getDate().getYear(),
								t._1._1,
								t._1._2),
						t._2));
		
		//first joinobject by date for every year and every sector and every ticker
		//Map((Year, Sector, Ticker)), JoinObject)
		JavaPairRDD<Tuple3<Integer, String, String>, JoinObject> firsts = byYearBySector
				.reduceByKey((one, two) -> DateSupports.firstJoin(one, two));
		
		//last joinobject by date for every year and every sector and every ticker
				//Map((Year, Sector, Ticker)), JoinObject)
		JavaPairRDD<Tuple3<Integer, String, String>, JoinObject> lasts = byYearBySector
				.reduceByKey((one, two) -> DateSupports.lastJoin(one, two));
		
		//first and last JoinObject by date for every year and every sector and every ticker
		//Map((Year, Sector, Ticker), (JoinObject, JoinObject))
		JavaPairRDD<Tuple3<Integer, String, String>,
			Tuple2<JoinObject, JoinObject>> firstsLasts = firsts.join(lasts);
		
		JavaPairRDD<Tuple3<Integer, String, String>, Double> variationsByYearSectorTicker =
				firstsLasts
				.mapToPair(t ->
				new Tuple2<Tuple3<Integer, String, String>, Double>(
						t._1,
						JoinObjectFunctions.totalVariation(t._2._1, t._2._2)));
		
		/*
		variations.foreach(t ->
			System.out.println(t._1._1() + " " + t._1._2() + " " + t._1._3() + ": " + t._2.toString()));
		*/
		
		JavaPairRDD<Tuple3<Integer, String, String>, Tuple2<Long, Integer>> volumesCount = byYearBySector
				.mapToPair(t ->
						new Tuple2<Tuple3<Integer, String, String>, Tuple2<Long, Integer>>
							(t._1,
							new Tuple2<Long, Integer>(
									t._2.getStock().getVolume(), 1)))
				.reduceByKey((t2a, t2b) -> new Tuple2<Long, Integer>(
						t2a._1 + t2b._1, t2a._2 + t2b._2));
		
		JavaPairRDD<Tuple3<Integer, String, String>, Double> volumesAveragesByYearSectorTicker = volumesCount
				.mapToPair(t ->
						new Tuple2<Tuple3<Integer, String, String>, Double>(
								t._1,
								Double.valueOf(t._2._1 / t._2._2)));
		
		JavaPairRDD<Tuple3<Integer, String, String>, Double> dailyQuotationByYearSectorTicker = byYearBySector
				.mapToPair(t ->
				new Tuple2<Tuple3<Integer, String, String>, Double>(
						t._1,
						JoinObjectFunctions.dailyQuotation(t._2)));
		
		JavaPairRDD<Tuple2<Integer, String>, Double> dailyQuotationByYearSector = dailyQuotationByYearSectorTicker
				.mapToPair(t ->
				new Tuple2<Tuple2<Integer, String>, Tuple2<Double, Integer>>(
						new Tuple2<Integer, String>(
								t._1._1(),
								t._1._2()),
						new Tuple2<Double, Integer>(
								t._2(), 1)))
				.reduceByKey((t2a, t2b) ->
						new Tuple2<Double, Integer>(
								t2a._1() + t2b._1(),
								t2a._2() + t2b._2()))
				.mapToPair(t -> 
						new Tuple2<Tuple2<Integer, String>, Double>(
								t._1(),
								t._2._1() / t._2._2()));
		
		JavaPairRDD<Tuple2<Integer, String>, Double> volumesAveragesByYearSector = volumesAveragesByYearSectorTicker
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
		
		JavaPairRDD<Tuple2<Integer, String>, Double> variationsByYearSector = variationsByYearSectorTicker
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
		
		JavaPairRDD<Tuple2<Integer, String>, String> result = variationsByYearSector
				.join(volumesAveragesByYearSector)
				.mapToPair(t ->
						new Tuple2<Tuple2<Integer, String>, String>(
								t._1,
								new String(
										t._2._1().toString()
										+ ", "
										+ t._2._2().toString())))
				.join(dailyQuotationByYearSector)
				.mapToPair(t ->
						new Tuple2<Tuple2<Integer, String>, String>(
								t._1,
								new String(
										t._2._1()
										+ ", "
										+ t._2._2().toString())));
		
		result
		.sortByKey(new Tuple2IntStrComparator())
		.foreach(new FinalForEach());
	}
}
