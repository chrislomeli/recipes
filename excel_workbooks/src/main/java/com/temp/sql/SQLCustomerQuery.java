package com.temp.sql;

import java.util.ArrayList;
import java.util.Date;

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
public class SQLCustomerQuery extends SQLStatementAdapter {
	
	/*
	 * 
		--daily sum--
		select  r.channel, p.gateway, r.currency_name, trunc(p.payment_date),  r.rate_type,    sum(amount) "Cash",  sum(r.allocation) "Realized AR", (sum(amount) - sum(r.allocation)) "Unallocated Cash"   
		from itac_ledger_revenue_t r, itac_ledger_payments_t p
		where p.transfer_obj_id0=r.transfer_obj_id0
		and r.payment_date >= '01 APR 2015' and r.payment_date < '01 MAY 2015'
		group by r.channel, p.gateway, r.currency_name, trunc(p.payment_date),  r.rate_type
		order by  r.channel, p.gateway, r.currency_name, trunc(p.payment_date),  r.rate_type;

	 */
	
	public SQLCustomerQuery(String label, Date startDate, Date endDate) {
		super(label);

		this.addSQLStatement(
				 "select  "
						 + " transaction_date "
						 + ", first_name "
						 + " , last_name "
						 + " , address "
						 + " from customer p  "
						 + " where p.transaction_date >= ? and p.transaction_date < ? "
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
