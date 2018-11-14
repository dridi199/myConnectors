package fr.edf.dco.common.connector.jdbc;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import fr.edf.dco.common.connector.base.Constants;

/**
 * SQLServer database access connector
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class HiveJdbcConnector extends AbstractJdbcConnector {

  //-----------------------------------------------------------------
  // CONSTRUCTOR
  //-----------------------------------------------------------------

  /**
   * Constructs an instance of SQLServercontainer
   * @param host        : sql server host adress
   * @param port        : sql server port
   * @param user        : database user name
   * @param password    : database user password
   * @param database    : database schema name
   */
  public HiveJdbcConnector(String host,
      int port,
      String user,
      String password,
      String database) 
  {
    super(host, port, user, password, database);
  }

  /**
   * Constructs an instance of SQLServercontainer using default port
   * @param host        : sql server host adress
   * @param user        : database user name
   * @param password    : database user password
   * @param database    : database schema name
   */
  public HiveJdbcConnector(String host,
      String instance,
      String user,
      String password,
      String database) 
  {
    super(host, Constants.SQL_SERVER_DEFAULT_PORT, user, password, database);

    driverClassName = "org.apache.hive.jdbc.HiveDriver";
    connectionUrl = "jdbc:hive2://noeyy5j6.noe.edf.fr:2181,noeyy5jg.noe.edf.fr:2181,noeyy5jj.noe.edf.fr:2181/;principal=hive/_HOST@HADOOP_RE7.EDF.FR;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;ssl=true;transportMode=http;httpPath=cliservice?tez.queue.name=dco_batch";
  }
  
  //-----------------------------------------------------------------
  // IMPLEMENTATION
  //-----------------------------------------------------------------

  /**
   * Calling procedures
   * TODO: IMPLEMENT
   * 
   * @param proc
   */
  public ResultSet procedure(String proc) throws SQLException {
    CallableStatement proc_stm = connection.prepareCall("{? = call " + proc + "() }");
    proc_stm.registerOutParameter(1, Types.INTEGER);
    proc_stm.execute();

    proc_stm.getMoreResults();
    ResultSet result = proc_stm.getResultSet();

    return result;
  }

  /**
   * Execute SQL Query
   * @param query query to execute
   */
  public void executeUpdate(String query) throws SQLException {
    Statement statement = connection.createStatement();
    statement.executeUpdate(query);
    statement.close();
  }
}
