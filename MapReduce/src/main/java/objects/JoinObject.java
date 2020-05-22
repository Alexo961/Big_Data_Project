package objects;

import java.io.Serializable;

public class JoinObject implements ActionObject{
	
	private static final int NUM_FIELDS = 13;

	private SectorObject sector;
	private StockObject stock;
	
	public JoinObject(SectorObject sector, StockObject stock) {
		super();
		this.sector = sector;
		this.stock = stock;
	}

	public SectorObject getSector() {
		return sector;
	}

	public void setSector(SectorObject sector) {
		this.sector = sector;
	}

	public StockObject getStock() {
		return stock;
	}

	public void setStock(StockObject stock) {
		this.stock = stock;
	}
	
	@Override
	public String toString() {
		return sector.toString() + "," + stock.toString();
	}
	
	@Override
	public int hashCode() {
		return sector.hashCode() + stock.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof JoinObject) {
			JoinObject that = (JoinObject) o;
			return sector.equals(that.getSector()) && stock.equals(that.getStock());
		}
		else
			return false;
	}

	@Override
	public int getNumFields() {
		return NUM_FIELDS;
	}

	@Override
	public boolean hasNullFields() {
		return stock.hasAllFields() || sector.hasNullFields();
	}

	@Override
	public boolean hasAllFields() {
		return stock.hasAllFields() && sector.hasAllFields();
	}
	
	
}
