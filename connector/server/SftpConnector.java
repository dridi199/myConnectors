package fr.edf.dco.common.connector.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import fr.edf.dco.common.connector.base.ConnectorException;

/**
 * Connector class for Sftp connection and read write operations handling
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class SftpConnector extends AbstractServerConnector {

  //-----------------------------------------------------------------
  // CONSTRUCTOR
  //-----------------------------------------------------------------

  public SftpConnector(String host, int port, String user, String password) {
    super(host, port, user, password);
  }

  //-----------------------------------------------------------------
  // IMPLEMENTATION
  //-----------------------------------------------------------------

  public void connect() throws ConnectorException {
    JSch jsch = new JSch();

    try {
      session = jsch.getSession(user, host, port);
      session.setPassword(password);

      Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      session.connect();
      channel = session.openChannel("sftp");
      channel.connect();
      channelSftp = (ChannelSftp)channel;
    } catch (JSchException e) {
      throw new ConnectorException("Unable to connect to sftp server : " + e.getMessage());
    } 
  }

  public void disconnect() {
    channelSftp.exit();
    channel.disconnect();
    session.disconnect(); 
  }

  public void sendFile(String localFile, String workingDir) throws SftpException, FileNotFoundException {
    channelSftp.cd(workingDir);
    File f = new File(localFile);
    channelSftp.put(new FileInputStream(f), f.getName());
  }

  //-----------------------------------------------------------------
  // DATA MEMBERS
  //-----------------------------------------------------------------

  private Session                         session;
  private Channel                         channel;
  private ChannelSftp                     channelSftp;
}
