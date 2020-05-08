package outputobjects;

public class JobOneOutOne {
	
	private String ticker;
	private Double variation;
	private Double minimum;
	private Double maximum;
	private Double meanVolume;
	
	public JobOneOutOne(String ticker, Double variation, Double minimum, Double maximum, Double meanVolume) {
		super();
		this.ticker = ticker;
		this.variation = variation;
		this.minimum = minimum;
		this.maximum = maximum;
		this.meanVolume = meanVolume;
	}

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public Double getVariation() {
		return variation;
	}

	public void setVariation(Double variation) {
		this.variation = variation;
	}

	public Double getMinimum() {
		return minimum;
	}

	public void setMinimum(Double minimum) {
		this.minimum = minimum;
	}

	public Double getMaximum() {
		return maximum;
	}

	public void setMaximum(Double maximum) {
		this.maximum = maximum;
	}

	public Double getMeanVolume() {
		return meanVolume;
	}

	public void setMeanVolume(Double meanVolume) {
		this.meanVolume = meanVolume;
	}

}
