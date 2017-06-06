package com.temp.worksheet;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.temp.sql.DataSource;


/**
 * RateScheduleDao
 * 
 * Straightforward access class for rate information using plain old jdbc and unpacking to classes.
 * Replace with ibatis, spring, hibernate, whatever
 * 
 * @author clomeli
 *
 */
public abstract class SQLStatementAdapter  {
	
	/* replace Columns with string labels */
	protected String sqlStatement = null;

	
	/* Array of Arguments */
	protected List<Object> arguments = null;
	
	/* from, where clause */
	protected String sqlChooser = null;
	
	/* worksheet label */
	protected String labelName=null;

	/* formats */
	protected NumberFormat 	fmt_integer = new DecimalFormat("#000");
	protected DecimalFormat 	fmt_decimal3 = new DecimalFormat("#0.000");
	protected DecimalFormat 	fmt_decimal5 = new DecimalFormat("#0.00000");
	protected NumberFormat 	fmt_currency = new DecimalFormat("#0.00");
	protected DateFormat 		fmt_date = new SimpleDateFormat("MM/dd/yyyy");
	
	
	public Connection getConnection() throws ClassNotFoundException, SQLException {
		return DataSource.connectJDBC();
	}
	
	
	public SQLStatementAdapter(String label) {
		this.labelName = label;
	}
	
	protected void addSQLStatement(String sql){
		this.sqlStatement = sql;
	}
	
	
	protected java.sql.Date convertDate(Date utilDate) {

		java.util.Calendar cal = Calendar.getInstance();
		cal.setTime(utilDate);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);    
		java.sql.Date sqlDate = new java.sql.Date(cal.getTime().getTime()); // your sql date
		
		return sqlDate;
	}


	public List<Object> getArguments() {
		return arguments;
	}

	public String getLabelName() {
		return labelName;
	}


	public void setArguments(List<Object> arguments) {
		this.arguments = arguments;
	}


	public void setSqlChooser(String sqlChooser) {
		this.sqlChooser = sqlChooser;
	}


	public String getSqlStatement() {
		return sqlStatement;
	}


}
