package com.temp.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * DataSource
 * 
 * Very sloppy data source, just for POC.  Not sure whether this would be in an application server ot binary
 * Just to get the data for now.   Separation of data from application is not done well
 * here.
 * 	public static String RED_DB_CONNECTION = "jdbc:redshift://bi-prod-redshift-cluster.ctzjqu1zsbry.us-west-2.redshift.amazonaws.com:5439/mount_olympus";
	public static String RED_DB_USER = "bi_etl";
	public static String RED_DB_PASS = "Mtwthfss!123";

 * @author clomeli
 *
 */
public class DataSource {
	
 	public static java.sql.Connection connectJDBC() throws java.sql.SQLException, ClassNotFoundException {
    	String redUser = "masteruser";
    	String redPassword = "Mtwthfss123";
    	String redConnectionString = "jdbc:postgresql://apollo.cqde1lqc3wzg.us-west-2.rds.amazonaws.com:5432/apollo";

		// connect
       Class.forName("org.postgresql.Driver");
       Properties props = new Properties();
       props.setProperty("user", redUser);
       props.setProperty("password", redPassword);
       return java.sql.DriverManager.getConnection(redConnectionString, props);
	}
 

	static public void setup() {
		Connection connection = null;
		try {
			connection = connectJDBC();
			Statement stmt = connection.createStatement();

			stmt.executeUpdate("create table if not exists files(created_date date, status int, filename varchar(255),  CONSTRAINT utestKey PRIMARY KEY(filename) )" );
			stmt.executeUpdate("insert into files (created_date, status, filename) values('2017-05-01',0, '/temp/testfile.json') ON CONFLICT DO NOTHING" );
			
			stmt.executeUpdate("create table if not exists customer ("
					+ "transaction_date date"
					+ ",first_name varchar(30)"
					+ ",last_name varchar(30)"
					+ ",address varchar(30)"
					+ ",city varchar(30)"
					+ ",state varchar(30)"
					+ " )" );
			stmt.executeUpdate("insert into customer (transaction_date, first_name, last_name, address, city, state) values("
					+ "'2017-05-01','john', 'doe','5020 Boston Blvd.', 'Los Angeles', 'California')" );
			stmt.executeUpdate("insert into customer (transaction_date, first_name, last_name, address, city, state) values("
					+ "'2017-05-01','jane', 'doe','5020 Wierd Ave.', 'Los Angeles', 'California')" );
			
			stmt.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}			
		}
	}
	
	
 }

