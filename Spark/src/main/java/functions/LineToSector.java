package functions;

import org.apache.spark.api.java.function.PairFunction;

import objects.ActionObject;
import objects.SectorObject;
import scala.Tuple2;
import supports.ObjectSupports;

public class LineToSector implements PairFunction<String, String, SectorObject>{

	@Override
	public Tuple2<String, SectorObject> call(String t) throws Exception {
		SectorObject so = null;
		ActionObject ao = ObjectSupports.stringToActionObject(t);
		if (ao != null && ao instanceof SectorObject)
			so = (SectorObject) ao;
		if (so != null) {
			return new Tuple2<String, SectorObject>(so.getTicker(), so);
		}
		return null;
	}

}
