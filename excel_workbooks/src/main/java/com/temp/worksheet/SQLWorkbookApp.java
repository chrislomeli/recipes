package com.temp.worksheet;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.temp.sql.DataSource;
import com.temp.sql.SQLCustomerQuery;
import com.temp.sql.SQLFileQuery;


public class SQLWorkbookApp {
	
	public static void runPeriodicReports(String fileName, Date startDate, Date endDate) {
		
		try {
			// sql array
			List<SQLStatementAdapter> statements = new ArrayList<>();
			statements.add(new SQLFileQuery("files_summary", startDate, endDate));
			statements.add(new SQLCustomerQuery("customer_summary", startDate, endDate));
			
			// create workbook
			SQLWorkbookService wb = new SQLWorkbookService();
			wb.createWorkbook(fileName, statements);
		
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	

	public static void main(final String[] commandLineArguments) throws ParseException {

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

		String path = "c:\\temp\\myworkbook.xlsx";
		Date startDate = format.parse("2017-05-01");
		Date endDate = format.parse("2017-06-01");
		
		DataSource.setup();
		
		runPeriodicReports(path, startDate, endDate);
		System.out.println("Done... ");

	}

	

	
}
