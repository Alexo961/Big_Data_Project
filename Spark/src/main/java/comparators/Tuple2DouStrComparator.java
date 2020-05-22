package comparators;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class Tuple2DouStrComparator implements Serializable, Comparator<Tuple2<Double, String>> {

	@Override
	public int compare(Tuple2<Double, String> one, Tuple2<Double, String> two) {
		if (one._1().equals(two._1())) {
			return one._2().compareTo(two._2);
		}
		else
			return one._1().compareTo(two._1());
	}

}
