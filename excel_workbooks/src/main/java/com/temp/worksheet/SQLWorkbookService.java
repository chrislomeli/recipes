package com.temp.worksheet;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;




/**
 * RateScheduleDao
 * 
 * Straightforward access class for rate information using plain old jdbc and unpacking to classes.
 * Replace with ibatis, spring, hibernate, whatever
 * 
 * @author clomeli
 *
 */
public class SQLWorkbookService  {
	
	SQLWorkbook workbook;
	

	/**
	 * createWorkbook
	 * 
	 */
	public void createWorkbook(String fileName, List<SQLStatementAdapter> sqlStatements) throws SQLException, IOException, ClassNotFoundException {
		
		System.out.println("Creating workbook "+fileName);
		
		// create workbook
		workbook =  new SQLWorkbook();
		
		 // create each worksheet using the sql passed in
		for (SQLStatementAdapter sql : sqlStatements) {
			System.out.println("process sheet: "+sql.getLabelName());
			this.createWorksheet(sql);
		}
		
		// print workbook
		System.out.println("printing "+fileName);
		workbook.print(fileName);
		
	}
	
	/**
	 * createWorksheet
	 * 
	 */
	public void createWorksheet(SQLStatementAdapter sql) throws IOException, SQLException, ClassNotFoundException {
		
		String sheetId=sql.getLabelName();
		

		/*
		 * Get sql statement
		 */
		String sqlStatement = sql.getSqlStatement();

		/*
		 * 
		 */
		workbook.addWorksheet(sheetId);

		Connection dbConnection = sql.getConnection();
		
		PreparedStatement preparedStmt = dbConnection
				.prepareStatement(sqlStatement
						,  ResultSet.TYPE_SCROLL_SENSITIVE
                        , ResultSet.CONCUR_UPDATABLE);
		
		for (int i=1;  sql.arguments != null && i <= sql.arguments.size(); i++) {

			Object o = sql.arguments.get(i-1);
			
			if (o.getClass().equals(Integer.class)) {
				preparedStmt.setInt(i, (int)o);
		    }
		    else if (o.getClass().equals(String.class)) {
		    	preparedStmt.setString(i, (String)o);
		    }
		    else if (o.getClass().equals(java.sql.Date.class)) {
		    	preparedStmt.setDate(i, (java.sql.Date)o  );
		    }
		    else if (o instanceof java.util.Date) {
		    	preparedStmt.setDate(i, (java.sql.Date) sql.convertDate((Date)o)  );
		    }
		    else if (o.getClass().equals(BigDecimal.class)) {
		    	preparedStmt.setBigDecimal(i, (BigDecimal)o );
		    }
		}
		
		
		/*
		 * Execute Query
		 */
		ResultSet rs = preparedStmt.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();
		
		
		/*
		 * Set header list
		 */
		List<String> labels = new ArrayList<>();
		boolean printHeading = true;

		// each row
		rs.beforeFirst();
		while (rs.next()) {

			List<Object> columns = new ArrayList<>();
			
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {

				int type = rsmd.getColumnType(i);
				String labelName = rsmd.getColumnLabel(i);
				
				if (printHeading) labels.add(labelName);
				
				if (type == Types.VARCHAR || type == Types.CHAR) 
				{
					columns.add( rs.getString(i) );
				} 
				else if (type == Types.DATE || type == Types.TIMESTAMP) 
				{
					columns.add( rs.getTimestamp(i) );
				} 
				else if (type == Types.INTEGER ) 
				{
					columns.add( rs.getInt(i) );
				} 
				else if (type == Types.DECIMAL || type == Types.NUMERIC) 
				{
					columns.add( rs.getBigDecimal(i) );
				} else {
					columns.add( "<unsuported-type>" );
				}
			} // columns
			
			
			/*
			 * write one row
			 */
			if (printHeading) {
				workbook.setHeading(sheetId, labels);
				printHeading = false;
			}
			workbook.setRow(sheetId, columns);
			
		} //row
		
		preparedStmt.close();
	}
	
	
}
