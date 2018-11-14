package fr.edf.dco.common.connector;

import fr.edf.dco.common.connector.base.ConnectorException;

/**
 * Abstraction Interface for data connectors
 * 
 * @author fahd-externe.essid@edf.fr
 */
public interface Connectable {

  //-----------------------------------------------------------------
  // ABSTRACTION
  //-----------------------------------------------------------------
  
  /**
   * Data connector connection operation
   */
  public void connect() throws ConnectorException;
  
  /**
   * Data connector disconnection operation
   */
  public void disconnect() throws ConnectorException;
}
