package supports;

import objects.JoinObject;

public class JoinObjectFunctions {

	public static Double totalVariation(JoinObject first, JoinObject last) {
		double ft = first.getStock().getClose();
		double lt = last.getStock().getClose();
		double variation = Math.round((((lt - ft)/ft) * 100) * 10) / 10.0;
		return Double.valueOf(variation);
	}
	
	public static Double dailyQuotation(JoinObject jo) {
		double open = jo.getStock().getOpen();
		double close = jo.getStock().getClose();
		double result = Math.round((((close - open) / open) * 100) * 10) / 10.0;
		return result;
	}
}
