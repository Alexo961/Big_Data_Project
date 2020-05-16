package supports;

import objects.StockObject;

public class DateSupports {

	public static StockObject last(StockObject one, StockObject two) {
		if (one.getDate().compareTo(two.getDate()) > 0)
			return one;
		return two;
	}
	
	public static StockObject first(StockObject one, StockObject two) {
		if (one.getDate().compareTo(two.getDate()) < 0)
			return one;
		return two;
	}
}
