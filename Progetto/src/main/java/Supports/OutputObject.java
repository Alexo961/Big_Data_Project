package Supports;

import org.apache.hadoop.io.Text;

public class OutputObject {
	
	private Text ticker;
	private Double variazione;
	private Double minimo;
	private Double massimo;
	private Double volmed;
	
	public OutputObject(Text ticker, Double variazione, Double minimo, Double massimo, Double volmed) {
		super();
		this.ticker = ticker;
		this.variazione = variazione;
		this.minimo = minimo;
		this.massimo = massimo;
		this.volmed = volmed;
	}

	public Text getTicker() {
		return ticker;
	}

	public void setTicker(Text ticker) {
		this.ticker = ticker;
	}

	public Double getVariazione() {
		return variazione;
	}

	public void setVariazione(Double variazione) {
		this.variazione = variazione;
	}

	public Double getMinimo() {
		return minimo;
	}

	public void setMinimo(Double minimo) {
		this.minimo = minimo;
	}

	public Double getMassimo() {
		return massimo;
	}

	public void setMassimo(Double massimo) {
		this.massimo = massimo;
	}

	public Double getVolmed() {
		return volmed;
	}

	public void setVolmed(Double volmed) {
		this.volmed = volmed;
	}
	
	@Override
	public String toString() {
		return ticker.toString() + ", " +
				variazione.toString() + ", " +
				minimo.toString() + ", " +
				massimo.toString() + ", " +
				volmed.toString();
	}
}
