package comparators;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class Tuple2IntDouComparator implements Serializable, Comparator<Tuple2<Integer, Double>> {

	@Override
	public int compare(Tuple2<Integer, Double> one, Tuple2<Integer, Double> two) {
		if (one._1().equals(two._1())) {
			return one._2().compareTo(two._2());
		}
		else
			return one._1().compareTo(two._1());
	}

}
