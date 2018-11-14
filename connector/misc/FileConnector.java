package fr.edf.dco.common.connector.misc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;

import fr.edf.dco.common.connector.Connectable;
import fr.edf.dco.common.connector.base.ConnectorException;

/**
 * File connector to read/write into files
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class FileConnector implements Connectable {

  //-------------------------------------------------------------------
  // CONSTRUCTOR
  //-------------------------------------------------------------------

  /**
   * Constructs a FileConnector from file name
   * @param fileName      the name of the file
   */
  public FileConnector(String fileName) {
    this.fileName = fileName;
  }

  //-------------------------------------------------------------------
  // IMPLEMENTATION
  //-------------------------------------------------------------------

  public void connect() throws ConnectorException {
    try {
      FileOutputStream fileOutputStream = new FileOutputStream(this.fileName, true);
      OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream, "utf-8");
      this.writer = new BufferedWriter(outputStreamWriter);
      this.reader = new BufferedReader(new FileReader(fileName));
    } catch (IOException e) {
      throw new ConnectorException("Error During File Connection: " + fileName + " " + e.getMessage());
    }
  }

  public void disconnect() throws ConnectorException {
    try {
      if (writer != null) {
        writer.close(); 
      }

      if (reader != null) {
        reader.close();
      }
    } catch (IOException e) {
      throw new ConnectorException("Error While Closing File Connection to : " + fileName + " " + e.getMessage());
    }
  }

  /**
   * Store a Single  Entry in the End of the  File.
   */
  public void write(String data) throws IOException {
    if (data != null) {
      writer.write(data);
      writer.newLine();
      writer.flush();
    }
  }

  public void emptyLine() throws IOException {
    writer.newLine();
    writer.flush();
  }

  /**
   * Read a Single  Entry in the End of the  File.
   */
  public String read() throws IOException {
    return reader.readLine();
  }

  //-------------------------------------------------------------------
  // ACCESSORS
  //-------------------------------------------------------------------

  public String getFileName() {
    return fileName;
  }

  //-------------------------------------------------------------------
  // DATA MEMBERS
  //-------------------------------------------------------------------

  private String                                fileName;        // the  File path
  private BufferedWriter                        writer;          // a buffered writer for the  file input stream
  private BufferedReader                        reader;          // a buffered reader for the  file output stream
}
