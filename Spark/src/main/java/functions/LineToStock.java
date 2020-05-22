package functions;

import org.apache.spark.api.java.function.PairFunction;

import objects.ActionObject;
import objects.StockObject;
import scala.Tuple2;
import supports.ObjectSupports;

public class LineToStock implements PairFunction<String, String, StockObject>{

	@Override
	public Tuple2<String, StockObject> call(String t) throws Exception {
		StockObject so = null;
		ActionObject ao = ObjectSupports.stringToActionObject(t);
		if (ao != null && ao instanceof StockObject)
			so = (StockObject) ao;
		if (so != null)
			return new Tuple2<String, StockObject>(so.getTicker(), so);
		return null;
	}

}
