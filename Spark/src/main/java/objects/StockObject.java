package objects;

import java.io.Serializable;
import java.time.LocalDate;

public class StockObject implements ActionObject, Serializable{
	
	private static final int NUM_FIELDS = 8;

	private String ticker;
	private Double open;
	private Double close;
	private Double adj;
	private Double low;
	private Double high;
	private Long volume;
	private LocalDate date;
	
	public StockObject(String ticker, Double open, Double close, Double adj, Double low, Double high, Long volume,
			LocalDate date) {
		super();
		this.ticker = ticker;
		this.open = open;
		this.close = close;
		this.adj = adj;
		this.low = low;
		this.high = high;
		this.volume = volume;
		this.date = date;
	}

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public Double getOpen() {
		return open;
	}

	public void setOpen(Double open) {
		this.open = open;
	}

	public Double getClose() {
		return close;
	}

	public void setClose(Double close) {
		this.close = close;
	}

	public Double getAdj() {
		return adj;
	}

	public void setAdj(Double adj) {
		this.adj = adj;
	}

	public Double getLow() {
		return low;
	}

	public void setLow(Double low) {
		this.low = low;
	}

	public Double getHigh() {
		return high;
	}

	public void setHigh(Double high) {
		this.high = high;
	}

	public Long getVolume() {
		return volume;
	}

	public void setVolume(Long volume) {
		this.volume = volume;
	}

	public LocalDate getDate() {
		return date;
	}

	public void setDate(LocalDate date) {
		this.date = date;
	}
	
	public int getNumFields() {
		return NUM_FIELDS;
	}
	
	@Override
	public int hashCode() {
		return ticker.hashCode() 
				+ open.hashCode() + close.hashCode()
				+ adj.hashCode()
				+ low.hashCode() + high.hashCode()
				+ volume.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof StockObject) {
			StockObject that = (StockObject) o;
			return (ticker.equals(that.getTicker())
					&& open.equals(that.getOpen())
					&& close.equals(that.getClose())
					&& adj.equals(that.getAdj())
					&& low.equals(that.getLow())
					&& high.equals(that.getHigh())
					&& volume.equals(that.getVolume()));
		}
		return false;
	}

}
