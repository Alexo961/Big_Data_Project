package supports;

import objects.StockObject;

public class StockFunctions {

	public static Double totalVariation(StockObject first, StockObject last) {
		double ft = first.getClose().doubleValue();
		double lt = last.getClose().doubleValue();
		double variation = Math.round((((lt - ft)/ft) * 100) * 10) / 10.0;
		return Double.valueOf(variation);
	}
	
	public static Long sumOfVolumes(StockObject a, StockObject b) {
		return a.getVolume() + b.getVolume();
	}
}
