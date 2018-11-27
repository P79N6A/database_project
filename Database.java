import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;

import java.io.File;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.IOException;

import java.util.Scanner;

/**
 * <h1>Abstract class to represent a table in the database.</h1>
 *
 * @author Hayder
 * @version 1.0
 */

abstract class Table {

  final String name;
  final String definition;
  final boolean isTemp;

  /**
   * Constructs a new table.
   *
   * @param name String represents name of the table.
   * @param definition String showing the definition of the table creation.
   * @param isTemp boolean representing whether table is temporary or not.
   * @throws SQLException
   */
  public Table(String name, String definition, boolean isTemp) throws SQLException {
    this.name = name;
    this.definition = definition;
    this.isTemp = isTemp;
    initialise();
  }

  /**
   * Abstract method to insert data in table which is implemented differently
   * depending on the table.
   *
   * @throws SQLException
   */

  abstract void insertData() throws SQLException;

  /**
   * Checks if table already exists before creating it. If it does, drop the table first.
   *
   * @throws SQLException
   */

  public void initialise() throws SQLException {
    if (alreadyExists()) {
      drop();
      create();
    } else {
      create();
    }
  }

  /**
   * Checks if table already exsts in database.
   *
   * @return true if table already exists in database otherwise return false.
   * @throws SQLException
   */

  public boolean alreadyExists() throws SQLException {
    boolean exists = false;
    ResultSet rs = Database.connection.getMetaData().getTables(null, null, this.name, null);
    while (rs.next()) {
        String name = rs.getString("TABLE_NAME");
        if (name != null && name.equals(this.name)) {
          exists = true;
          break;
        }
    }
    return exists;
  }

  /**
   * Drops table from database.
   *
   * @throws SQLException
   */

  public void drop() throws SQLException {
    Statement st = Database.connection.createStatement();
    st.execute("DROP TABLE " + this.name + " CASCADE;");
    st.close();
  }

  /**
   * Creates table in database.
   *
   * @throws SQLException
   */

  public void create() throws SQLException {
    Statement st = Database.connection.createStatement();
    String createQuery;

    if (isTempTable()) {
      createQuery = "CREATE TEMP TABLE " + this.name + " (" + this.definition + ");";
    } else {
      createQuery = "CREATE TABLE " + this.name + " (" + this.definition + ");";
    }
    st.execute(createQuery);
    st.close();
  }

  /**
   * Checks if table is temporary or not.
   *
   * @return true if table is temporary, otherwise return false.
   */
  public boolean isTempTable() {
    return this.isTemp;
  }

}

/**
 * <h1>Abstract class to represent a view in the database.</h1>
 *
 * @author Hayder
 * @version 1.0
 */

abstract class View {

  final String name;
  final String definition;

  /**
   * Constructs a new view.
   *
   * @param name String represents name of the view.
   * @param definition String showing the definition of the view creation.
   * @throws SQLException
   */
  public View(String name, String definition) throws SQLException {
    this.name = name;
    this.definition = definition;
    initialise();
  }

  /**
   * Checks if view already exists before creating it. If it does, drop the view first.
   *
   * @throws SQLException
   */

  public void initialise() throws SQLException {
    if (alreadyExists()) {
      drop();
      create();
    } else {
      create();
    }
  }

  /**
   * Checks if view already exsts in database.
   *
   * @return true if view already exists in database otherwise return false.
   * @throws SQLException
   */

  public boolean alreadyExists() throws SQLException {
    boolean exists = false;
    ResultSet rs = Database.connection.getMetaData().getTables(null, null, this.name, new String[]{"VIEW"});
    while (rs.next()) {
        String name = rs.getString("TABLE_NAME");
        if (name != null && name.equals(this.name)) {
          exists = true;
          break;
        }
    }
    return exists;
  }

  /**
   * Drops view from database.
   *
   * @throws SQLException
   */

  public void drop() throws SQLException {
    Statement st = Database.connection.createStatement();
    st.execute("DROP VIEW " + this.name + " CASCADE;");
    st.close();
  }

  /**
   * Creates view in database.
   *
   * @throws SQLException
   */

  public void create() throws SQLException {
    Statement st = Database.connection.createStatement();
    st.execute("CREATE VIEW " + this.name + " AS " + this.definition + ";");
    st.close();
  }

}

/**
 * Class to represent the mapping table in database. Implements {@link Table} abstract class.
 *
 * @author Hayder
 * @version 1.0
 *
 * @see Table
 */

class Mapping extends Table {

  public final static String name = "mapping";
  public final static String definition = "tld VARCHAR(15), description VARCHAR(200) NOT NULL, PRIMARY KEY (tld)";
  public final static boolean isTemp = false;

  public final static String insertQuery = "INSERT INTO mapping (tld, description) VALUES (?, ?) "
      + "ON CONFLICT DO NOTHING";

  public String filepath = new File("").getAbsolutePath();
  public String mapping_file = filepath + "/mapping";

  /**
   * Constructor calls parent constructor to set up table.
   *
   * @throws SQLException
   *
   * @see Table#Table(String, String, boolean)
   */

  public Mapping() throws SQLException {
    super(name, definition, isTemp);
    insertData();
  }

  /**
   * Inserts data from mapping file into table.
   *
   * @throws SQLException
   */

  @Override
  public void insertData() throws SQLException {
	  PreparedStatement preparedStatement;
	  preparedStatement = Database.connection.prepareStatement(insertQuery);

	  BufferedReader reader;

	    try {
	      reader = new BufferedReader(new FileReader(mapping_file));
	      String line = reader.readLine();
	      while (line != null) {

	        String[] values = line.split("\t");
	        int i = 1;
	        for (String value : values) {
	        	preparedStatement.setString(i++, value);
	        }

	        preparedStatement.addBatch();

	        line = reader.readLine();
	      }

	      preparedStatement.executeBatch();
	      preparedStatement.close();
	      reader.close();

	    } catch (IOException e) {
	      e.printStackTrace();
	    	System.out.println("Error reading file");
	    }


  }

}

/**
 * Class to represent the url_temp table in database. Implements {@link Table} abstract class.
 *
 * @author Hayder
 * @version 1.0
 *
 * @see Table
 */

class UrlTemp extends Table {

  public final static String name = "url_temp";
  public final static String definition = "pos INT,\n" +
			"  domain_name VARCHAR(50),\n" +
			"  tld1 VARCHAR(15) NOT NULL,\n" +
			"  tld2 VARCHAR(15)";

  public final static boolean isTemp = true;

  public String insertQuery = "INSERT INTO url_temp (pos, domain_name, tld1, tld2) "
  		+ "VALUES (?, ?, ?, ?)";

  public String filepath = new File("").getAbsolutePath();
  public String file = filepath + "/TopURLs";

  /**
   * Constructor calls parent constructor to set up table.
   *
   * @throws SQLException
   *
   * @see Table#Table(String, String, boolean)
   */

  public UrlTemp() throws SQLException {
    super(name, definition, isTemp);
    insertData();
  }

  /**
   * Inserts data from TopURLs file into table.
   *
   * @throws SQLException
   */

  @Override
  public void insertData() throws SQLException {
  	  PreparedStatement preparedStatement;
  	  preparedStatement = Database.connection.prepareStatement(insertQuery);

  	  Scanner scanner;

  	    try {
  	    	scanner = new Scanner(new FileInputStream(file));

  		    while(scanner.hasNextLine()) {
  		    	String line = scanner.nextLine();
  		    	String[] values = line.split("\t");
  		        int id = Integer.parseInt(values[0]);
  		        String domain = values[1];
  		        String tld1 = values[2];
  		        String tld2;

  		        // check if the line has two tlds or one and handle accordingly
  		        if (values.length == 4) {
  		        	tld2 = values[3];
  		        } else {
  		        	tld2 = "";
  		        }

  		        preparedStatement.setInt(1, id);
  		        preparedStatement.setString(2, domain);
  		        preparedStatement.setString(3, tld1);
  		        preparedStatement.setString(4, tld2);
  		        preparedStatement.addBatch();
  		    }

  	      preparedStatement.executeBatch();
  	      preparedStatement.close();
  	      scanner.close();

  	    } catch (IOException e) {
  	      e.printStackTrace();
  	    	System.out.println("Error reading file");
  	    }

    }

}

/**
 * Class to represent the tld table in database. Implements {@link Table} abstract class.
 *
 * @author Hayder
 * @version 1.0
 *
 * @see Table
 */

class Tld extends Table {

  public final static String name = "tld";
  public final static String definition = "tld_id INT, tld1 VARCHAR(15) NOT NULL, tld2 VARCHAR(15), PRIMARY KEY (tld_id)";
  public final static boolean isTemp = false;

  public final static String insertQuery = "SELECT row_number() OVER (ORDER BY min(pos)), tld1, tld2\n" +
  "FROM url_temp\n" +
  "GROUP BY tld1, tld2";

  /**
   * Constructor calls parent constructor to set up table.
   *
   * @throws SQLException
   *
   * @see Table#Table(String, String, boolean)
   */

  public Tld() throws SQLException {
    super(name, definition, isTemp);
    insertData();
  }

  /**
   * Inserts data from url table into this table.
   *
   * @throws SQLException
   */

  @Override
  public void insertData() throws SQLException {
	  Statement st = Database.connection.createStatement();
	  st.executeUpdate("INSERT INTO " + name + " " + insertQuery + " ON CONFLICT DO NOTHING;");
	  st.close();
  }

}

/**
 * Class to represent the domain table in database. Implements {@link Table} abstract class.
 *
 * @author Hayder
 * @version 1.0
 *
 * @see Table
 */

class Domain extends Table {

  public final static String name = "domain";
  public final static String definition = "domain_name VARCHAR(50), PRIMARY KEY (domain_name)";
  public final static boolean isTemp = false;

  public final static String insertQuery = "SELECT domain_name\n" +
      "FROM url_temp\n" +
      "GROUP BY domain_name\n" +
      "ORDER BY domain_name";

  /**
   * Constructor calls parent constructor to set up table.
   *
   * @throws SQLException
   *
   * @see Table#Table(String, String, boolean)
   */

  public Domain() throws SQLException {
    super(name, definition, isTemp);
    insertData();
  }

  /**
   * Inserts data from url table into this table.
   *
   * @throws SQLException
   */

  @Override
  public void insertData() throws SQLException {
	  Statement st = Database.connection.createStatement();
	  st.executeUpdate("INSERT INTO " + name + " " + insertQuery + " ON CONFLICT DO NOTHING;");
	  st.close();
  }

}

/**
 * Class to represent the url table in database. Implements {@link Table} abstract class.
 *
 * @author Hayder
 * @version 1.0
 *
 * @see Table
 */

class Url extends Table {

  public final static String name = "url";
  public final static String definition = "domain_name VARCHAR(50) NOT NULL, tld_id INT NOT NULL,\n" +
			"  position INT NOT NULL, PRIMARY KEY (domain_name, tld_id), "
			+ "FOREIGN KEY (domain_name) REFERENCES domain,\n" +
			"  FOREIGN KEY (tld_id) REFERENCES tld,\n" +
			"  UNIQUE (position),\n" +
			"  CHECK (position >= 1 AND position <= 10000)";

  public final static boolean isTemp = false;

  public final static String insertQuery = "INSERT INTO url (position, domain_name, tld_id) VALUES (?, ?, ?) ON CONFLICT DO NOTHING";

  /**
   * Constructor calls parent constructor to set up table.
   *
   * @throws SQLException
   *
   * @see Table#Table(String, String, boolean)
   */

  public Url() throws SQLException {
    super(name, definition, isTemp);
    insertData();
  }

  /**
   * Inserts data from url_temp into table.
   *
   * @throws SQLException
   */

  @Override
  public void insertData() throws SQLException {
    PreparedStatement preparedStatement;
	  preparedStatement = Database.connection.prepareStatement(insertQuery);
	  ResultSet rs = Database.executeSelect("SELECT pos, domain_name, tld_id "
		  		+ "FROM url_temp "
		  		+ "NATURAL JOIN tld "
		  		+ "WHERE url_temp.tld1 = tld.tld1 AND url_temp.tld2 = tld.tld2 "
		  		+ "ORDER BY pos;");

	  try {
	    while (rs.next()) {
		  preparedStatement.setInt(1, rs.getInt(1));
		  preparedStatement.setString(2, rs.getString(2));
		  preparedStatement.setInt(3, rs.getInt(3));
		  preparedStatement.addBatch();
	    }
	  } catch (SQLException e) {
	    e.printStackTrace();
	  }
	  rs.close();

	  preparedStatement.executeBatch();
	  preparedStatement.close();
  }

}

/**
 * Class to represent the url view in database. Implements {@link View} abstract class.
 *
 * @author Hayder
 * @version 1.0
 *
 * @see View
 */

class UrlView extends View {

  public final static String name = "top_10_urls";
  public final static String definition = "SELECT position, domain_name, tld1, tld2\n" +
			"  FROM url\n" +
			"  NATURAL JOIN tld\n" +
			"  ORDER BY position\n" +
			"  LIMIT 10;";

  /**
   * Constructor calls parent constructor to set up view.
   *
   * @throws SQLException
   *
   * @see View#View(String, String)
   */

  public UrlView() throws SQLException {
    super(name, definition);
  }

}

/**
 * Class to represent the url view in database. Implements {@link View} abstract class.
 *
 * @author Hayder
 * @version 1.0
 *
 * @see View
 */

class TldView extends View {

  public final static String name = "top_10_tlds";
  public final static String definition = "SELECT min(position) AS best_position, tld1, tld2, description\n" +
			"    FROM url\n" +
			"    NATURAL JOIN tld, mapping\n" +
			"    WHERE\n" +
			"    (\n" +
			"      CASE\n" +
			"        WHEN tld.tld2 = '' THEN tld.tld1\n" +
			"        ELSE tld.tld2\n" +
			"      END\n" +
			"    )  = mapping.tld\n" +
			"    GROUP BY tld1, tld2, description\n" +
			"    ORDER BY best_position\n" +
			"    LIMIT 10;";

  /**
   * Constructor calls parent constructor to set up view.
   *
   * @throws SQLException
   *
   * @see View#View(String, String)
   */

  public TldView() throws SQLException {
    super(name, definition);
  }

}

/**
 * Class to represent the url view in database. Implements {@link View} abstract class.
 *
 * @author Hayder
 * @version 1.0
 *
 * @see View
 */

class DomainView extends View {

  public final static String name = "top_10_repeated_domains";
  public final static String definition = "SELECT min(position) AS best_position, domain_name\n" +
			"  FROM url\n" +
			"  GROUP BY domain_name\n" +
			"  HAVING count(*) > 1\n" +
			"  ORDER BY best_position\n" +
			"  LIMIT 10;";

  /**
   * Constructor calls parent constructor to set up view.
   *
   * @throws SQLException
   *
   * @see View#View(String, String)
   */

  public DomainView() throws SQLException {
    super(name, definition);
  }

}

/**
 * <h1>Class to represent a query in the database.</h1>
 *
 * @author Hayder
 * @version 1.0
 */

class Query {

  private String title;
  private String query;
  private String formatting;

  /**
   * Constructor initialises the instance fields of this class.
   *
   * @param title String represents the title to be displayed for this query.
   * @param query String represents the actual statment to query.
   * @param formatting String represents how the query results should be formatted.
   * @throws SQLException
   */

  public Query(String title, String query, String formatting) throws SQLException {
    this.title = title;
    this.query = query;
    this.formatting = formatting;
  }

  /**
   * Iterates through resultset of query and prints the formatted results to console.
   *
   * @throws SQLException
   */
  public void print() throws SQLException {
    ResultSet rs = Database.executeSelect(this.query);
    ResultSetMetaData rsmd = rs.getMetaData();

    int columns = rsmd.getColumnCount();
    String[] format = this.formatting.split(" ");

    System.out.println("\n##" + this.title + "##\n");

    for (int i = 0; i < columns; i++) {
      System.out.printf(format[i], rsmd.getColumnName(i + 1).toUpperCase());
    }

    try {
      while (rs.next()) {
        for (int i = 1; i <= columns; i++) {
          if (rs.getObject(i) instanceof Integer) {
            System.out.printf(format[i-1], rs.getInt(i));
          } else {
            System.out.printf(format[i-1], rs.getString(i));
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    rs.close();
    System.out.println("\n");
  }

}

/**
 * Conntains main method and is where program is launched from.
 *
 * @author Hayder
 * @version 1.0
 */

public class Database {

  public static Connection connection;

  /**
   * Establishes a connection to database.
   *
   * @param user String represents username for database.
   * @param password String represents the password for user.
   * @param database String represents the name of database (including the username).
   * @return a {@link Connection} if database connection successful, otherwise return null.
   */
  public static Connection connectToDatabase(String user, String password, String database) {
  Connection connection = null;
    try {
      connection=DriverManager.getConnection(database, user, password);
  } catch (SQLException e) {
    System.out.println("Connection Failed! Check output console");
    e.printStackTrace();
  }
    return connection;
  }

  /**
   * Shows the resultset of a query.
   *
   * @param query String represents the statement to query.
   * @return the resultset of the query.
   * @throws SQLException
   */
  public static ResultSet executeSelect (String query) throws SQLException {
    Statement st = null;
    st = connection.createStatement();
    ResultSet rs = null;
    rs = st.executeQuery(query);
  return rs;
  }

  /**
   * Loads database by initialising all tables and views as well printing out the queries.
   *
   * @throws SQLException
   */
  public static void loadDatabase() throws SQLException {
    Table mapping = new Mapping();
    Table url_temp = new UrlTemp();
    Table tld = new Tld();
    Table domain = new Domain();
    Table url = new Url();

    View top_10_urls = new UrlView();
    View top_10_tlds = new TldView();
    View top_10_repeated_domains = new DomainView();

    Query queryOne = new Query(
        "Query 1: 10 most popular URLs in descending order of popularity",
        "SELECT * FROM top_10_urls;",
        "%-10.10s %-15.50s %-7.15s %-7.15s%n");

    Query queryTwo = new Query(
        "Query 2: 10 distinct most popular top level domains in descending order of popularity",
        "SELECT tld1, tld2 FROM top_10_tlds;",
        "%-7.15s %-7.15s%n");

    Query queryThree = new Query(
        "Query 3: 10 distinct most popular descriptions of the rightmost part of tld in descending order of popularity",
        "SELECT description FROM top_10_tlds;",
        "%-50.200s%n");

    Query queryFour = new Query(
        "Query 4: top 10 distinct domain names that appear more than once, ordered by popularity",
        "SELECT domain_name FROM top_10_repeated_domains;",
        "%-15.50s%n");

    Query[] queries = {queryOne, queryTwo, queryThree, queryFour};

    for (Query query : queries) {
      query.print();
    }
  }

  /**
   * Main method launches program.
   *
   * @param args String array requires exactly two arguments, username and password.
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {

    String user;
    String password;
    String database;

    if (args.length != 2) {
      System.out.println("Please enter your username and password as command line arguments.");
    } else {
      user = args[0];
      password = args[1];
      database = "jdbc:postgresql://localhost/CS2855/" + user;
      connection = connectToDatabase(user, password, database);
    }

    if (connection != null) {
      loadDatabase();
    } else {
      System.out.println("Failed to make connection!");
    }

  }
}
