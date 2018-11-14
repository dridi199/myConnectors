package fr.edf.dco.common.connector.hadoop;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;

import fr.edf.dco.common.connector.base.ConnectorException;
import fr.edf.dco.common.connector.base.Constants;
import fr.edf.dco.common.connector.base.Utils;

/**
 * Hbase tables access connector
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class HbaseConnector extends AbstractHadoopConnector {

  //------------------------------------------------------------------
  // CONSTRUCTOR
  //------------------------------------------------------------------

  /**
   * Creating Hbase connector using configuration xml files
   * and kerberos user and keytab
   */
  public HbaseConnector(String core,
      String hbase,
      String krb5,
      String user,
      String keytab)
  {
    super(core, hbase, krb5, user, keytab);
  }

  /**
   * Creating Hbase connector using configuration xml files
   * Non secured
   */
  public HbaseConnector(String core, String hbase) {
    super(core, hbase);
  }
  
  /**
   * Create a hbase connector with default parameters
   * @param user
   * @param keytab
   * @param secured
   */
  public HbaseConnector(String user, String keytab, boolean secured) {
    super(Constants.DEFAULT_CORE_SITE, Constants.DEFAULT_HBASE_SITE, Constants.DEFAULT_KRB_CONF, user, keytab);
    this.secured = secured;
  }

  //------------------------------------------------------------------
  // IMPLEMENTATION
  //------------------------------------------------------------------

  /**
   * Configure Hbase connector
   */
  private void configure() throws IOException, ConnectorException {
    conf = HBaseConfiguration.create();

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
      connection = ConnectionFactory.createConnection(conf);
    } catch (IOException e) {
      throw new ConnectorException(e.getMessage()); 
    }
  }

  /**
   * Disconnect HBase
   */
  public void disconnect() throws ConnectorException {
    try {
      connection.close();
    } catch (IOException e) {
      throw new ConnectorException(e.getMessage());
    }
  }

  /********************************************************************
   PUT OPERATIONS
   ********************************************************************/

  public void put(String tableName,
      String row,
      String family,
      String column,
      String value)
          throws IOException
  {
    BufferedMutator table = connection.getBufferedMutator(TableName.valueOf(tableName));

    Put put = new Put(Utils.getBytes(row));
    put.addColumn(Utils.getBytes(family), Utils.getBytes(column), Utils.getBytes(value));
    table.mutate(put);
    table.flush();
    table.close();
  }

  public void put(String row, String column, String value) throws IOException {
    put(htable, row, columnFamily, column, value);
  }

  public void put(String column, String value) throws IOException{
    put(htable, row, columnFamily, column, value);
  }

  public void putWithTimestamp(String column, String value, long timestamp) throws IOException {
    BufferedMutator table = connection.getBufferedMutator(TableName.valueOf(htable));

    Put put = new Put(Utils.getBytes(row));
    put.addColumn(Utils.getBytes(columnFamily), Utils.getBytes(column), timestamp, Utils.getBytes(value));
    table.mutate(put);
    table.flush();
    table.close();
  }

  public void multiPut(List<Put> puts) throws IOException {
    multiMutate(puts);
  }
  
  public void multiDelete(List<Delete> deletes) throws IOException {
    multiMutate(deletes);
  }
  
  public void multiMutate(List<? extends Mutation> mutations) throws IOException {
    BufferedMutator table = connection.getBufferedMutator(TableName.valueOf(htable));

    table.mutate(mutations);
    table.flush();
    table.close();    
  }
  
  /********************************************************************
   GET OPERATIONS
   ********************************************************************/

  public Result getRow(String key) throws IOException {
    Table table = connection.getTable(TableName.valueOf(htable));
    Get get = new Get(Utils.getBytes(key));
    return table.get(get);
  }

  public Result getRowVersions(String key) throws IOException {
    Table table = connection.getTable(TableName.valueOf(htable));
    Get get = new Get(Utils.getBytes(key));
    get.setMaxVersions();
    return table.get(get);
  }

  public String get(String tableName,
      String row,
      String family,
      String column)
          throws IOException
  {
    Table table = connection.getTable(TableName.valueOf(tableName));
    Get get = new Get(Utils.getBytes(row));
    Result result = table.get(get);    
    byte [] val = result.getValue(Utils.getBytes(family),Utils.getBytes(column));
    return val == null ? null : new String(val, Charset.forName("UTF-8"));    
  }

  public String get(String row, String family, String column) throws IOException {
    return get(htable, row, family, column);
  }

  public String get(String column) throws IOException {
    return get(htable, row, columnFamily, column);
  }

  public boolean exists(String key) throws IOException {
    Table table = connection.getTable(TableName.valueOf(htable));
    Get get = new Get(Utils.getBytes(key));
    return table.exists(get);
  }

  public ResultScanner scan(boolean allVersions) throws IOException {
    Table table  = connection.getTable(TableName.valueOf(htable));
    Scan scan = new Scan();

    if (allVersions) {
      scan.setMaxVersions();
    }

    return table.getScanner(scan);
  }

  public ResultScanner prefixFilterScan(String prefix, boolean allVersions) throws IOException {
    Table table  = connection.getTable(TableName.valueOf(htable));
    Scan scan = new Scan();

    if (allVersions) {
      scan.setMaxVersions();
    }

    scan.setRowPrefixFilter(Utils.getBytes(prefix));

    return table.getScanner(scan);
  }

  public ResultScanner filteredScan(FilterList filters, String prefix, boolean allVersions) throws IOException {
    Table table  = connection.getTable(TableName.valueOf(htable));
    Scan scan = new Scan();

    if (allVersions) {
      scan.setMaxVersions();
    }

    if (prefix != null) {
      scan.setRowPrefixFilter(Utils.getBytes(prefix));
    }

    if (filters != null) {
      scan.setFilter(filters);
    }   

    return table.getScanner(scan);
  }

  /**
   * Truncate hbase table
   */
  public void truncate() throws IOException {
    truncate(htable);
  }

  /**
   * Truncate hbase table given its name
   */
  public void truncate(String table) throws IOException{
    Admin admin = getConnection().getAdmin();
    TableName tableName = TableName.valueOf(table);
    
    if(admin.isTableEnabled(tableName)) {
      admin.disableTable(tableName);
    }
    
    admin.truncateTable(tableName, false);
    
    if (admin.isTableDisabled(tableName)) {
      admin.enableTable(tableName);
    }
  }

  /**
   * Row key prefix based bulk delete
   * @param prefix  row key prefix to delete 
   * @throws IOException
   */
  public void bulkDelete(String prefix) throws IOException {
    List<Delete> listOfBatchDelete = new ArrayList<Delete>();
    ResultScanner scanner = prefixFilterScan(prefix, true);
    
    int i = 0;
    for (Result row : scanner) {
      System.out.println(i++);
      Delete d = new Delete(row.getRow());
      listOfBatchDelete.add(d);
      
      if (listOfBatchDelete.size() >= 100000) {
        connection.getTable(TableName.valueOf(htable)).delete(listOfBatchDelete);
        listOfBatchDelete  = new ArrayList<Delete>();;
      }
    }
    
    connection.getTable(TableName.valueOf(htable)).delete(listOfBatchDelete);
  }
  
  public void delete(Delete d) throws IOException {
    connection.getTable(TableName.valueOf(htable)).delete(d);
  }

  //------------------------------------------------------------------
  // ACCESSORS
  //------------------------------------------------------------------

  /**
   * Set hbase table name
   */
  public void setTable(String table) {
    this.htable = table;
  }

  /**
   * Set hbase column family
   */
  public void setColumnFamily(String family) {
    this.columnFamily = family;
  }

  /**
   * Set hbase row id
   */
  public void setRow(String row) {
    this.row = row;
  }

  /**
   * Returns HbaseConnection
   * @return
   */
  public Connection getConnection() {
    return this.connection;
  }

  //------------------------------------------------------------------
  // DATA_MEMBERS
  //------------------------------------------------------------------

  private Connection            connection;
  private String                htable;
  private String                columnFamily;
  private String                row;
}
