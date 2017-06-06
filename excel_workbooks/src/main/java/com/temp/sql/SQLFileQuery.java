package com.temp.sql;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import com.temp.worksheet.Column;
import com.temp.worksheet.SQLStatementAdapter;



/**
 * RateScheduleDao
 * 
 * Straightforward access class for rate information using plain old jdbc and unpacking to classes.
 * Replace with ibatis, spring, hibernate, whatever
 * 
 * @author clomeli
 *
 */
public class SQLFileQuery extends SQLStatementAdapter {
	

	public SQLFileQuery(String label, Date startDate, Date endDate) {
		super(label);

		this.addSQLStatement(
				 "select  "
						+ " created_date, status, filename  "
						+ " from files p  "
						+ " where p.created_date >= ? and p.created_date < ? "
					);
	
		/*
		 * Ordered arguments
		 */
		this.arguments = new ArrayList<>();
		arguments.add(startDate);
		arguments.add(endDate);
	
		return;
	}
	
}
