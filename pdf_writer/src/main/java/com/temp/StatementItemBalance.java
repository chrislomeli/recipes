package com.temp;

import java.math.BigDecimal;

/**
 * StatementRateLineSubtotal
 * 
 * This is a pure data object representing balance strictly at the RateLine-only level
 * 
 * @author clomeli
 *
 */
public class StatementItemBalance {

	
	String displayName;
	BigDecimal quantity =  new BigDecimal(0);
	BigDecimal rateUsed =  new BigDecimal(0);
	BigDecimal ratedTotal =  new BigDecimal(0);
	
	public StatementItemBalance(String name, String quant, String rate ) {
		displayName = name;
		quantity = new BigDecimal(quant);
		rateUsed =  new BigDecimal(rate);
		ratedTotal = quantity.multiply(rateUsed);
	}

	/*
	 * 
	 */
	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	public BigDecimal getQuantity() {
		return quantity;
	}

	public void setQuantity(BigDecimal quantity) {
		this.quantity = quantity;
	}

	public BigDecimal getRateUsed() {
		return rateUsed;
	}

	public void setRateUsed(BigDecimal rateUsed) {
		this.rateUsed = rateUsed;
	}

	public BigDecimal getRatedTotal() {
		return ratedTotal;
	}

	public void setRatedTotal(BigDecimal ratedTotal) {
		this.ratedTotal = ratedTotal;
	}
	
	
}

