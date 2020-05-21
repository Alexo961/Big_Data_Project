package supports;

import objects.JoinObject;
import objects.StockObject;

public class DateSupports {

	public static StockObject lastStock(StockObject one, StockObject two) {
		if (one.getDate().compareTo(two.getDate()) > 0)
			return one;
		return two;
	}
	
	public static StockObject firstStock(StockObject one, StockObject two) {
		if (one.getDate().compareTo(two.getDate()) < 0)
			return one;
		return two;
	}
	
	public static boolean fromYear (StockObject so, int year) {
		return (so.getDate().getYear() >= year);
	}
	
	public static boolean fromYear2008 (StockObject so) {
		return fromYear(so, 2008);
	}
	
	public static boolean fromYear2016 (StockObject so) {
		return fromYear(so, 2016);
	}
	
	public static JoinObject firstJoin(JoinObject one, JoinObject two) {
		if (one.getStock().getDate().compareTo(two.getStock().getDate()) < 0)
			return one;
		return two;
	}
	
	public static JoinObject lastJoin(JoinObject one, JoinObject two) {
		if (one.getStock().getDate().compareTo(two.getStock().getDate()) > 0)
			return one;
		return two;
	}
	
}
