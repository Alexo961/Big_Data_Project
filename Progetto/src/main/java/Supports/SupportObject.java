package Supports;

import java.time.LocalDate;
import java.util.Date;

class SupportObject {
	
	
	private Double ogettoDouble ;
	private LocalDate ogettoDate;
	
	public SupportObject(Double ogettoDouble, LocalDate ogettoDate) {
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

	public LocalDate getOgettoDate() {
		return ogettoDate;
	}

	public void setOgettoDate(LocalDate ogettoDate) {
		this.ogettoDate = ogettoDate;
	}

}
