package fr.edf.dco.common.connector.server;

import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.NoSuchProviderException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import fr.edf.dco.common.connector.base.ConnectorException;

/**
 * Connector class for Smtp connection and read write operations handling
 *
 * @author fahd-externe.essid@edf.fr
 */
public class SmtpConnector extends AbstractServerConnector {

  //-----------------------------------------------------------------
  // CONSTRUCTOR
  //-----------------------------------------------------------------

  public SmtpConnector(String host, int port, String adress, String password) {
    super(host, port, adress, password);
  }

  //-----------------------------------------------------------------
  // IMPLEMENTATION
  //-----------------------------------------------------------------

  public void connect() throws ConnectorException {
    Properties props = System.getProperties();
    props.put("mail.smtp.host", host);
    props.put("mail.smtp.port", port);

    if (password != null) {
      props.put("mail.smtp.auth", "true");
      props.put("mail.smtp.password", password);
    }

    props.put("mail.smtp.user", user);

    session = Session.getDefaultInstance(props, new SmtpAuthenticator());
    session.setDebug(true);

    try {
      transport = session.getTransport("smtp");
      transport.connect(host, port, user, password);
    } catch (NoSuchProviderException e) {
      throw new ConnectorException("Unable to establish SMTP connection : " + e.getMessage());
    } catch (MessagingException e) {
      throw new ConnectorException("Unable to establish SMTP connection : " + e.getMessage());
    } 
  }

  public void disconnect() throws ConnectorException {
    try {
      transport.close();
    } catch (MessagingException e) {
      throw new ConnectorException("Unable to close SMTP Transport : " + e.getMessage());
    }
  }

  public void sendEmail(String[] addresses, String mailBody, String mailSubject, String fileName) throws AddressException, MessagingException {
    MimeMessage message = new MimeMessage(session);

    message.setSubject(mailSubject);    
    message.setFrom(new InternetAddress(user));

    for (String address : addresses) {
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(address));
    }

    if (fileName != null) {
      BodyPart messageBody = new MimeBodyPart();
      messageBody.setText(mailBody);
      Multipart multipart = new MimeMultipart();
      multipart.addBodyPart(messageBody);

      messageBody = new MimeBodyPart();
      DataSource source = new FileDataSource(fileName);
      messageBody.setDataHandler(new DataHandler(source));
      messageBody.setFileName(fileName);
      multipart.addBodyPart(messageBody);

      message.setContent(multipart);        
    } else {
      message.setText(mailBody);
    }

    transport.sendMessage(message, message.getAllRecipients());
  }

  public void sendEmail(String address, String mailBody, String mailSubject, String fileName) throws AddressException, MessagingException {
    sendEmail(address.split(";", -1), mailBody, mailSubject, fileName);
  }

  //-----------------------------------------------------------------
  // INNER CLASS : SmtpAuthenticator
  //-----------------------------------------------------------------

  public class SmtpAuthenticator extends Authenticator {
    public SmtpAuthenticator() {
      super();
    }

    @Override
    public PasswordAuthentication getPasswordAuthentication() {
      return new PasswordAuthentication(user, password);
    }
  }

  //-----------------------------------------------------------------
  // DATA MEMBERS
  //-----------------------------------------------------------------

  private Session                                   session;
  private Transport                                 transport;
}
