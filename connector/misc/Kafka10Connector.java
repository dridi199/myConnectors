package fr.edf.dco.common.connector.misc;


import java.util.HashMap;
import java.util.Map;

import fr.edf.dco.common.connector.Connectable;
import fr.edf.dco.common.connector.base.ConnectorException;
import fr.edf.dco.common.connector.base.Constants;

/**
 * Kafka producer and consumer connector
 * 
 * @author ahmed-externe.dridi@edf.fr
 */
public class Kafka10Connector implements Connectable {

  //-------------------------------------------------------------------
  // CONSTRUCTOR
  //-------------------------------------------------------------------

  /**
   * Create a new KafkaConnector instance in batchMode
   * @param zookeeperQuorum   Zookeeper quorum for consumer configuration
   * @param brokersList       Kafka broker list for producer configuration
   * @param groupId           consumer group id
   * @param offsetrest        earliest or latest or none
   * @param mode              stream or batch mode
   */
  public Kafka10Connector(String zookeeperQuorum, String brokersList, String groupId,String offsetrest, boolean mode) {
    this.zookeeperQuorum = zookeeperQuorum; 
    this.brokersList = brokersList;
    this.groupId = groupId;
    this.offsetreset=offsetrest;
    this.batchMode = mode;
  }

  /**
   * Create a new KafkaConnector instance in batchMode
   * @param zookeeperQuorum   Zookeeper quorum for consumer configuration
   * @param brokersList       Kafka broker list for producer configuration
   * @param groupId           consumer group id
   * @param 
   */
  public Kafka10Connector(String zookeeperQuorum, String brokersList, String groupId,String offsetreset) {
    this(zookeeperQuorum, brokersList, groupId, offsetreset, true);
  }
  
  public Map<String, Object> getKafka10Params() {
    Map<String, Object> kafkaParams = new HashMap<String, Object>();
    kafkaParams.put("bootstrap.servers", brokersList);
    kafkaParams.put("security.protocol", "SASL_SSL");
    kafkaParams.put("ssl.truststore.location", "./truststore");
    kafkaParams.put("ssl.truststore.password", "ryba123");
    kafkaParams.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
    kafkaParams.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
    kafkaParams.put("enable.auto.commit", false);
    kafkaParams.put("auto.offset.reset", offsetreset);
    kafkaParams.put("group.id", "fixedgroup");
    return kafkaParams;
  }
  
  
  @Override
  public void disconnect() throws ConnectorException {
    
  }



  //-------------------------------------------------------------------
  // DATA MEMBERS
  //-------------------------------------------------------------------

  private int                                   position = Constants.KAFKA_DEFAULT_POSITION;

  private String                                topic;                  // the kafka topic name
  private String                                offsetreset;
  private String                                zookeeperQuorum;        // zookeeper hosts list (separator is ',')
  private String                                brokersList;            // Brokers host list (if not specified then its zookeeper quorum)
  private String                                groupId;                // groupId in the kafka topic
  private boolean                               batchMode;              // if set to true exceptions will be thrown when ne messages are left in the topic while reading
  @Override
  public void connect() throws ConnectorException {
    // TODO Auto-generated method stub
    
  }

}
