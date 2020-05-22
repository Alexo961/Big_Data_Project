package functions;

import org.apache.spark.api.java.function.PairFunction;

import objects.JoinObject;
import scala.Tuple2;

public class ByYearBySectorFunction
		implements PairFunction<Tuple2<String, JoinObject>,
		Integer,
		Tuple2<String, JoinObject>> {

	@Override
	public Tuple2<Integer, Tuple2<String, JoinObject>> 
		call(Tuple2<String, JoinObject> t) throws Exception {
		Integer year = t._2.getStock().getDate().getYear();
		
		return new Tuple2<Integer, Tuple2<String, JoinObject>>(
				year, t);
	}

	
}
