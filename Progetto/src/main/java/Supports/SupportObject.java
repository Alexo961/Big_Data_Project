package Supports;

import java.util.Date;

class SupportObject {
	
	
	private Double ogettoDouble ;
	private Date ogettoDate;
	
	public SupportObject(Double ogettoDouble, Date ogettoDate) {
		super();
		this.ogettoDouble = ogettoDouble;
		this.ogettoDate = ogettoDate;
	}

	public Double getOgettoDouble() {
		return ogettoDouble;
	}

	public void setOgettoDouble(Double ogettoDouble) {
		this.ogettoDouble = ogettoDouble;
	}

	public Date getOgettoDate() {
		return ogettoDate;
	}

	public void setOgettoDate(Date ogettoDate) {
		this.ogettoDate = ogettoDate;
	}

}
