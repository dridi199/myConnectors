package fr.edf.dco.common.connector.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.conf.Configuration;

import fr.edf.dco.common.connector.base.ConnectorException;
import fr.edf.dco.common.connector.base.Constants;

/**
 * Hive tables acess connector
 *  
 * @author fahd-externe.essid@edf.fr
 *
 */
public class HiveConnector extends AbstractHadoopConnector {

  //------------------------------------------------------------------
  // CONSTRUCTOR
  //------------------------------------------------------------------

  public HiveConnector(String core, String base, String krb5, String user, String keytab) {
    super(core, base, krb5, user, keytab);
  }

  /**
   * Creating hive connector using configuration xml files
   * Non secured
   */
  public HiveConnector(String core, String hive) {
    super(core, hive);
  }

  /**
   * Create a hive connector with default parameters
   * @param user
   * @param keytab
   * @param secured
   */
  public HiveConnector(String user, String keytab, boolean secured) {
    super(Constants.DEFAULT_CORE_SITE, Constants.DEFAULT_HIVE_SITE, Constants.DEFAULT_KRB_CONF, user, keytab);
    this.secured = secured;
  }
  //------------------------------------------------------------------
  // IMPLEMENTATION
  //------------------------------------------------------------------

  /**
   * Configure hive connector
   */
  private void configure() throws IOException, ConnectorException {
    conf = new Configuration();
    if (secured) {
      secure();
    }

    conf.addResource(coreSiteFile);
    conf.addResource(baseSiteFile);
  }

  /**
   * Connect to Hbase
   */
  public void connect() throws ConnectorException {
    try {
      configure();
    } catch (IOException e) {
      throw new ConnectorException(e.getMessage()); 
    }

    String driverClassName = "org.apache.hive.jdbc.HiveDriver";
    String connectionUrl = "jdbc:hive2://noeyy5j6.noe.edf.fr:2181,noeyy5jg.noe.edf.fr:2181,noeyy5jj.noe.edf.fr:2181/;principal=hive/_HOST@HADOOP_RE7.EDF.FR;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;ssl=true;transportMode=http;httpPath=cliservice?tez.queue.name=dco_batch";

    try {
      Class.forName(driverClassName);
      connection = DriverManager.getConnection(connectionUrl);
    } catch (ClassNotFoundException e) {
      throw new ConnectorException("JDBC driver not found in classpath : " + e.getMessage());
    } catch (SQLException e) {
      throw new ConnectorException("Error while trying to connect to Server : " + e.getMessage());
    }   
  }

  public void disconnect() throws ConnectorException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new ConnectorException(e.getMessage());
    }
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
  
  public Connection connection;
}
