package phoenix.datatorrent.operator.output.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TestJdbcConnection {


  // JDBC driver name and database URL
  static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
  static final String DB_URL = "jdbc:mysql://localhost:3306/phoenix";

  // Database credentials
  static final String USER = "kdbuser";
  static final String PASS = "KdBuSeR12!";


  public static void main(String[] args) {


    Connection conn = null;
    Statement stmt = null;
    try {
      // STEP 2: Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver").newInstance();

      // STEP 3: Open a connection
      System.out.println("Connecting to a selected database...");
      conn = DriverManager.getConnection(DB_URL, USER, PASS);
      System.out.println("Connected database successfully...");

      // STEP 4: Execute a query
      System.out.println("Inserting records into the table...");
      stmt = conn.createStatement();

      String sql = "INSERT INTO test_event_table " + "VALUES (1)";
      stmt.executeUpdate(sql);
      sql = "INSERT INTO test_event_table " + "VALUES (2)";
      stmt.executeUpdate(sql);
      sql = "INSERT INTO test_event_table " + "VALUES (3)";
      stmt.executeUpdate(sql);
      sql = "INSERT INTO test_event_table " + "VALUES(4)";
      stmt.executeUpdate(sql);
      System.out.println("Inserted records into the table...");

    } catch (SQLException se) {
      // Handle errors for JDBC
      se.printStackTrace();
    } catch (Exception e) {
      // Handle errors for Class.forName
      e.printStackTrace();
    } finally {
      // finally block used to close resources
      try {
        if (stmt != null)
          conn.close();
      } catch (SQLException se) {
      }// do nothing
      try {
        if (conn != null)
          conn.close();
      } catch (SQLException se) {
        se.printStackTrace();
      }// end finally try
    }// end try
    System.out.println("Goodbye!");
  }// end main



}
