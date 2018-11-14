package fr.edf.dco.common.connector.base;

/**
 * Custom Connection exceptions
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class ConnectorException extends Exception {

  //---------------------------------------------------------------------------
  // CONSTRUCTOR
  //---------------------------------------------------------------------------

  /**
   * Create a new ConnectionException with a given Exception message
   * @param message error message
   */
  public ConnectorException(String message) {
    super(message);
  }

  //---------------------------------------------------------------------------
  // DATA MEMBERS
  //---------------------------------------------------------------------------
 
  private static final long serialVersionUID = 7982667692478300307L;
}
