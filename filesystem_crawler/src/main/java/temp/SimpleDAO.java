package temp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class SimpleDAO  {

	
	 	public static java.sql.Connection connectJDBC() throws java.sql.SQLException, ClassNotFoundException {
	    	String redUser = "[YOUR-DB-USERNAME]";
	    	String redPassword = "[YOUR-DB-PASSWORD]";
	    	String redConnectionString = "[YOUR-JDBC-URL]";

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
				stmt.executeUpdate("create table if not exists my_example(status int, filename varchar(255),  CONSTRAINT utestKey PRIMARY KEY(filename) )" );
				stmt.executeUpdate("insert into my_example (status, filename) values(0, '/temp/testfile.json') ON CONFLICT DO NOTHING" );
				/* autocommit is on for this db */ 
				//connection.commit();
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
		
		static public void insertfile(String basefilename, int status) {
			Connection connection = null;
			try {
				connection = connectJDBC();
				Statement stmt = connection.createStatement();
				stmt.executeUpdate(String.format("insert into my_example (status, filename) values(%s, '%s')", status, basefilename) );
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

		static public int fileExists(String baseFilename) {
			Connection connection = null;
			int status = 0;
			try {
				connection = connectJDBC();
				PreparedStatement stmt = connection.prepareStatement("select coalesce(status,0) as status from my_example where filename=?  limit 1");
				stmt.setString(1, baseFilename);
	
				ResultSet rs = stmt.executeQuery();
				if (rs.next()) {
					status = rs.getInt("status");
				}
				rs.close();
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
			return status;
		}
	
}
