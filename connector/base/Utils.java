package fr.edf.dco.common.connector.base;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility class containing static methods that can be used by any class
 * here are implemented general purpose methods
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class Utils {

  /**
   * Set System Kerberos file
   * @param krb5  the krb5.ini configuration file path
   */
  public static void configureKerberos(String krb5) throws ConnectorException {
    if (krb5 != null) {
      System.setProperty("java.security.krb5.conf", krb5);
    } else {
      throw new ConnectorException("Kerberos configuration file not found");
    }
  }

  /**
   * Convert from internal Java String format to UTF-8
   * @param s java internal string
   * @return  the UTF-8 converted String 
   */
  public static String convertToUTF8(String s) throws UnsupportedEncodingException {
    String out = null;

    if(s != null) {
      out = new String(s.getBytes("UTF-8"), "ISO-8859-1");
    }

    return out;
  }

  /**
   * Format a date given its original date format
   * @param stringDate  date String value
   * @param inputFormat original date format
   * @param outputFormat  returned date format
   * @return  formated String date
   */
  public static String formatDate(String stringDate, String inputFormat, String outputFormat) throws ParseException {
    SimpleDateFormat inFormatter = new SimpleDateFormat(inputFormat);
    SimpleDateFormat outFormatter = new SimpleDateFormat(outputFormat);
    String result = "";

    Date date = inFormatter.parse(stringDate);
    result =  outFormatter.format(date);
    return result;
  }

  /**
   * Creates a date from String
   * @param stringDate  date String value
   * @param format  date format
   * @return Date value 
   */
  public static Date dateFromString(String stringDate, String format) throws ParseException {
    SimpleDateFormat form = new SimpleDateFormat(format);

    return form.parse(stringDate);
  }
  
  public static String stringFromDate(Date date, String format) {
    SimpleDateFormat form = new SimpleDateFormat(format);
    return form.format(date);
  }

  /**
   * Return Timestamp from String date
   * @param date  date String value
   * @param dateFormat  date original format
   * @return  result as Timestamp value
   */
  public static long getLongTimestamp(String date, String dateFormat) {
    long result = 0;

    if (date != null) {
      SimpleDateFormat format = new SimpleDateFormat(dateFormat);
      Date d = format.parse(date, new ParsePosition(0));
      result = d!= null ? d.getTime() : 0;
    }

    return result;
  } 
  public static long getTimestamp(Object date, String dateFormat) {
    long result = 0;

    if (date != null) {
      SimpleDateFormat format = new SimpleDateFormat(dateFormat);
      Date d = format.parse(date.toString(), new ParsePosition(0));
      result = d!= null ? d.getTime() : 0;
    }

    return result;
  }

  /**
   * Tests wether a String is empty or null
   * @param value String value
   * @return  result as boolean
   */
  public static boolean isNotEmptyOrSpace(String value) {
    return (value != null) && (!value.isEmpty()) && (!value.equals("null"));
  }
  
  /**
   * Return bytes from String value
   * @param s
   * @return
   */
  public static byte[] getBytes(String s) {
    if (s != null) {
      return Bytes.toBytes(s);
    } else {
      return Bytes.toBytes("");
    }
  }
  
  public static byte[] getBytes(Object s) {
    if (s != null) {
      return Bytes.toBytes(s.toString());
    } else {
      return Bytes.toBytes("");
    }
  }
}
