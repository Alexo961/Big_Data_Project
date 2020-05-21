package objects;

import java.io.Serializable;

public class SectorObject implements ActionObject, Serializable, Comparable{
	
	private static final int NUM_FIELDS = 5;
	private static final String NULL_FIELD = "N/A";
	
	private String ticker;
	private String exchange;
	private String name;
	private String sector;
	private String industry;
	
	public SectorObject(String ticker, String exchange, String name, String sector, String industry) {
		super();
		this.ticker = ticker;
		this.exchange = exchange;
		this.name = name;
		this.sector = sector;
		this.industry = industry;
	}

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSector() {
		return sector;
	}

	public void setSector(String sector) {
		this.sector = sector;
	}

	public String getIndustry() {
		return industry;
	}

	public void setIndustry(String industry) {
		this.industry = industry;
	}
	
	public int getNumFields() {
		return NUM_FIELDS;
	}
	
	@Override
	public String toString() {
		return ticker + ","
				+ exchange + ","
				+ name + ","
				+ sector + ","
				+ industry;
	}
	
	@Override
	public int hashCode() {
		return ticker.hashCode()
				+ name.hashCode()
				+ exchange.hashCode()
				+ sector.hashCode()
				+ industry.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof SectorObject) {
			SectorObject that = (SectorObject) o;
			return (ticker.equals(that.getTicker())
					&& exchange.equals(that.getExchange())
					&& name.equals(that.getName())
					&& sector.equals(that.getSector())
					&& industry.equals(that.getIndustry()));
		}
		else
			return false;
	}
	
	@Override
	public int compareTo(Object o) {
		if (o instanceof SectorObject) {
			SectorObject that = (SectorObject) o;
			if (ticker.equals(that.getTicker())){
				if (exchange.equals(that.getExchange())) {
					if (name.equals(that.getName())) {
						if (sector.equals(that.getSector())) {
							return industry.compareTo(that.getIndustry());
						}
						else
							return sector.compareTo(that.getSector());
					}
					else
						return name.compareTo(that.getName());
				}
				else
					return exchange.compareTo(that.getExchange());
			}
			else
				return ticker.compareTo(that.getTicker());
		}
		else
			return 1;
	}
	
	@Override
	public boolean hasNullFields() {
		return (ticker == null || ticker.equals(NULL_FIELD))
				|| (exchange == null || exchange.equals(NULL_FIELD))
				|| (name == null || name.equals(NULL_FIELD))
				|| (sector == null || name.equals(NULL_FIELD))
				|| (industry == null || industry.equals(NULL_FIELD));
	}
	
	@Override
	public boolean hasAllFields() {
		return !hasNullFields();
	}

}
