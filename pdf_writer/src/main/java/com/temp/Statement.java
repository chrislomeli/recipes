package com.temp;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

public class Statement implements Serializable {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String billNo;
	DateTime startDate;
	DateTime endDate;
	String customerName;
	String currencyCode;
	String language;
	BigDecimal taxRate = new BigDecimal(0.00);
	BigDecimal grossCharge = new BigDecimal(0.00);
	BigDecimal adjustments = new BigDecimal(0.00);
	BigDecimal finalCharge = new BigDecimal(0.00);

	List<StatementItemBalance> itemLines = new ArrayList<>();

	public void calculate() {
		adjustments = grossCharge.multiply(taxRate);
		finalCharge = grossCharge.add(adjustments);
	}
	
	public void addBalance(StatementItemBalance stmt) {
		grossCharge = grossCharge.add(stmt.ratedTotal);
		itemLines.add(stmt);
	}

	public String getBillNo() {
		return billNo;
	}

	public void setBillNo(String billNo) {
		this.billNo = billNo;
	}

	public DateTime getStartDate() {
		return startDate;
	}

	public void setStartDate(DateTime startDate) {
		this.startDate = startDate;
	}

	public DateTime getEndDate() {
		return endDate;
	}

	public void setEndDate(DateTime endDate) {
		this.endDate = endDate;
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustomerName(String customerName) {
		this.customerName = customerName;
	}

	public String getCurrencyCode() {
		return currencyCode;
	}

	public void setCurrencyCode(String currencyCode) {
		this.currencyCode = currencyCode;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public BigDecimal getGrossCharge() {
		return grossCharge;
	}

	public void setGrossCharge(BigDecimal grossCharge) {
		this.grossCharge = grossCharge;
	}

	public BigDecimal getFinalCharge() {
		return finalCharge;
	}

	public void setFinalCharge(BigDecimal finalCharge) {
		this.finalCharge = finalCharge;
	}

	public List<StatementItemBalance> getItemLines() {
		return itemLines;
	}

	public void setItemLines(List<StatementItemBalance> itemLines) {
		this.itemLines = itemLines;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	public BigDecimal getAdjustments() {
		return adjustments;
	}

	public void setTaxRate(String trate) {
		this.adjustments = new BigDecimal(trate);
	}
	
	
}
