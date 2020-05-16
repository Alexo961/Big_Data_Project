package objects;

import java.io.Serializable;

public class SectorObject implements ActionObject, Serializable{
	
	private static final int NUM_FIELDS = 5;
	
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

}
