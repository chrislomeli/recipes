package com.temp;

import java.io.IOException;

import org.joda.time.DateTime;

import com.itextpdf.text.DocumentException;

public class StatementApp {

	public static void main(String[] args) {
		Statement smt = new Statement();
		smt.setBillNo("B-010");
		smt.setCurrencyCode("USD");
		smt.setCustomerName("Acme Company");
		smt.setLanguage("en-US");
		smt.setStartDate(DateTime.now().minusMonths(1));
		smt.setEndDate(DateTime.now());
		smt.addBalance(new StatementItemBalance("Tent", "1", "155.99"));
		smt.addBalance(new StatementItemBalance("Tent poles", "3", "5.99"));
		smt.addBalance(new StatementItemBalance("Stove", "1", "85.95"));
		smt.calculate();

		String outputfile = String.format("C:\\temp\\bill_%s.pdf", smt.getBillNo());
		String logoimage = "PlaceholderLogoBlue.jpg";
		
		try {
		
			new PDFStatementFormatter().createPdf(smt, outputfile, logoimage);
			System.out.println("Created statement "+outputfile);
			
		} catch (IOException | DocumentException e) {
			e.printStackTrace();
		}
		
		
	}

}
