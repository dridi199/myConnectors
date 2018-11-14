package fr.edf.dco.common.connector.server;

import fr.edf.dco.common.connector.Connectable;

/**
 * Server like connector abstraction
 * 
 * @author fahd-externe.essid@edf.fr
 */
public abstract class AbstractServerConnector implements Connectable {
  
  //-----------------------------------------------------------------
  // CONSTRUCTOR
  //-----------------------------------------------------------------
  
  /**
   * Creates an AbstractServerConnector
   * @param host      host name or ip
   * @param port      server port
   * @param user      username
   * @param password  user pwd
   */
  public AbstractServerConnector(String host, int port, String user, String password) {
    this.port = port;
    this.host = host;
    this.user = user;
    this.password = password;
  }
  
  //-----------------------------------------------------------------
  // DATA MEMBERS
  //-----------------------------------------------------------------
  
  protected int                       port;
  protected String                    host;
  protected String                    user;
  protected String                    password;
}

