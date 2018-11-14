package fr.edf.dco.common.connector.misc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import fr.edf.dco.common.connector.Connectable;
import fr.edf.dco.common.connector.base.Constants;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Kafka producer and consumer connector
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class KafkaConnector implements Connectable {

  //-------------------------------------------------------------------
  // CONSTRUCTOR
  //-------------------------------------------------------------------

  /**
   * Create a new KafkaConnector instance in batchMode
   * @param zookeeperQuorum   Zookeeper quorum for consumer configuration
   * @param brokersList       Kafka broker list for producer configuration
   * @param groupId           consumer group id
   * @param mode              stream or batch mode
   */
  public KafkaConnector(String zookeeperQuorum, String brokersList, String groupId, boolean mode) {
    this.zookeeperQuorum = zookeeperQuorum; 
    this.brokersList = brokersList;
    this.groupId = groupId;
    this.batchMode = mode;
  }

  /**
   * Create a new KafkaConnector instance in batchMode
   * @param zookeeperQuorum   Zookeeper quorum for consumer configuration
   * @param brokersList       Kafka broker list for producer configuration
   * @param groupId           consumer group id
   */
  public KafkaConnector(String zookeeperQuorum, String brokersList, String groupId) {
    this(zookeeperQuorum, brokersList, groupId, true);
  }

  //-------------------------------------------------------------------
  // IMPLEMENTATION
  //-------------------------------------------------------------------

  /**
   * Connecting to Kafka topic in writing mode (producer)
   */
  public void connect() {
    createProducer();
    createConsumer();
  }

  private void createProducer() {
    // Creating producer
    Properties producerProp = new Properties();
    ProducerConfig produceronfig;

    producerProp.put("metadata.broker.list", brokersList);

    produceronfig = new ProducerConfig(producerProp);
    this.producer = new Producer<byte[], byte[]>(produceronfig);
  }

  private void createConsumer() {
    // Setting consumer configuration
    Properties consumerProp = new Properties();
    ConsumerConfig consumerConfig;
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap;
    KafkaStream<byte[], byte[]> stream;

    consumerProp.put("zookeeper.connect", zookeeperQuorum);
    consumerProp.put("group.id", groupId);
    consumerProp.put("auto.offset.reset", "smallest");

    if (batchMode) {
      consumerProp.put("consumer.timeout.ms", "5000");
      consumerProp.put("zookeeper.session.timeout.ms", "1000");    
    }

    // Creating the consumer at a given position of the topic
    consumerConfig = new ConsumerConfig(consumerProp);
    this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    topicCountMap.put(this.topic, this.position);

    // Getting the kafka topic messages stream
    consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    stream =  consumerMap.get(this.topic).get(0);

    this.consumer = stream.iterator(); 
  }

  /**
   * Closing Kafka producer
   */
  public void disconnect() {
    this.producer.close();
    this.consumerConnector.shutdown();
  }

  /**
   * add a message to the producer
   * the key is set to null
   */
  public void put(String message) {
    KeyedMessage<byte[], byte[]> keyedMessage = new KeyedMessage<byte[], byte[]>(this.topic, message.getBytes());
    this.producer.send(keyedMessage);
  }

  /**
   * add a keyed message to the producer
   */
  public void put(String key, String message) {
    KeyedMessage<byte[], byte[]> keyedMessage = new KeyedMessage<byte[], byte[]>(this.topic, key.getBytes(), message.getBytes());
    this.producer.send(keyedMessage);
  }

  /**
   * read a message from the consumer
   * returns a string with the consumed message
   */
  public String get() throws ConsumerTimeoutException {  
    String line = null;   
    if (consumer.hasNext()) {
      line = new String(consumer.next().message());
    } 

    return line;
  }

  /**
   * read a message and its metadata (key...) from the consumer
   * return a KafkaMessage object
   */
  public KafkaMessage getKeyedMessage() throws ConsumerTimeoutException {
    KafkaMessage message = null;
    if (consumer.hasNext()) {
      MessageAndMetadata<byte[], byte[]> next = consumer.next();
      message = new KafkaMessage(new String(next.message()), next.key() != null ? new String(next.key()) : ""); 
    } 

    return message;
  }

  //-------------------------------------------------------------------
  // ACCESSORS
  //-------------------------------------------------------------------

  public void setPosition(int pos) {
    this.position = pos;
  }

  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getBrokers() {
    return this.brokersList;
  }

  public HashMap<String, String> getKafkaParams(String offsetType, boolean batch) {
    HashMap<String, String> kafkaParams = new HashMap<String, String>();

    kafkaParams.put("metadata.broker.list",brokersList);
    kafkaParams.put("zookeeper.connect", zookeeperQuorum);

    kafkaParams.put("group.id", UUID.randomUUID().toString());
    kafkaParams.put("auto.offset.reset", offsetType);


    if (batch) {
      kafkaParams.put("consumer.timeout.ms", "5000");
      kafkaParams.put("zookeeper.session.timeout.ms", "1000");    
    }

    return kafkaParams;
  }

  /**
   * Return topic partitions
   * @return an integer list with topic partitions 
   */
  public List<Integer> getPartitionsForTopic() {

    List<Integer> partitions = new ArrayList<>();

    SimpleConsumer consumer = getSimpleConsumer().get(0);
    List<String> topics = Collections.singletonList(topic);

    TopicMetadataRequest req = new TopicMetadataRequest(topics);
    kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

    List<TopicMetadata> metaData = resp.topicsMetadata();
    for (TopicMetadata item : metaData) {          
      for (PartitionMetadata part : item.partitionsMetadata()) {
        partitions.add(part.partitionId());
      }
    }

    return partitions;
  }

  /**
   * Return all partitions offsets
   * @param partition
   * @return
   */
  public List<Long> findAllOffsets(int partition) {

    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    List<SimpleConsumer> consumers = getSimpleConsumer();
    List<Long> offsetsList = new ArrayList<Long>();
    
    for (SimpleConsumer consumer : consumers) {
      PartitionOffsetRequestInfo partitionOffsetRequestInfo = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), Integer.MAX_VALUE);
      OffsetRequest offsetRequest = new OffsetRequest(ImmutableMap.of(topicAndPartition, partitionOffsetRequestInfo), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());
      long[] offsets = consumer.getOffsetsBefore(offsetRequest).offsets(topic, partition);
      
      for (long l : offsets) {
        offsetsList.add(l);
      }
    } 
    
    return offsetsList; 
  }

  /**
   * return a simple consumer for current topic
   * @return
   */
  private List<SimpleConsumer> getSimpleConsumer() {
    List<SimpleConsumer> consumers = new ArrayList<SimpleConsumer>();
    List<BrokerInfo> seeds = getBrokersInfo();
    
    for (BrokerInfo seed : seeds) {
      consumers.add(new SimpleConsumer(seed.getHost(), seed.getPort(), 20000, 128 * 1024, "partitionLookup"));
    }
    
    return consumers;
  }

  /**
   * Return the brokers list
   * @return
   */
  public List<BrokerInfo> getBrokersInfo() {
    List<BrokerInfo> seedBrokers = new ArrayList<BrokerInfo>();

    for (String broker : brokersList.split(",", -1)) {
      String[] params = broker.split(":", -1);
      seedBrokers.add(new BrokerInfo(params[0], new Integer(params [1])));
    }

    return seedBrokers;
  }

  //-------------------------------------------------------------------
  // INNER CLASS : BrokerInfo
  //-------------------------------------------------------------------

  /**
   * BrokerInfo Inner Class
   */
  public static class BrokerInfo {

    //-------------------------------------------------------------------
    // CONSTRUCTOR
    //-------------------------------------------------------------------

    public BrokerInfo(String host, int port) {
      this.host = host;
      this.port = port;
    }

    //-------------------------------------------------------------------
    // CONSTRUCTOR
    //-------------------------------------------------------------------


    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }


    private String    host;
    private int       port; 
  }

  //-------------------------------------------------------------------
  // INNER CLASS : KafkaMessage
  //-------------------------------------------------------------------

  /**
   * A simple representation of a kafka keyed message
   */
  public static class KafkaMessage {

    //-------------------------------------------------------------------
    // CONSTRUCTOR
    //-------------------------------------------------------------------

    /**
     * Constructs a new KafkaMessage given its message and key
     */
    public KafkaMessage(String message, String key) {
      this.message = message;
      this.key = key;
    }

    //-------------------------------------------------------------------
    // ACCESSORS
    //-------------------------------------------------------------------

    public String getMessage() {
      return message;
    }

    public String getKey() {
      return key;
    }

    //-------------------------------------------------------------------
    // DATA MEMBERS
    //-------------------------------------------------------------------

    private String          message;
    private String          key;
  }

  //-------------------------------------------------------------------
  // DATA MEMBERS
  //-------------------------------------------------------------------

  private int                                   position = Constants.KAFKA_DEFAULT_POSITION;

  private String                                topic;                  // the kafka topic name
  private String                                zookeeperQuorum;        // zookeeper hosts list (separator is ',')
  private String                                brokersList;            // Brokers host list (if not specified then its zookeeper quorum)
  private String                                groupId;                // groupId in the kafka topic
  private boolean                               batchMode;              // if set to true exceptions will be thrown when ne messages are left in the topic while reading
  private Producer<byte[], byte[]>              producer;               // kafka producer handler
  private ConsumerConnector                     consumerConnector;      // kafka consumer messages streamer
  private ConsumerIterator<byte[], byte[]>      consumer;               // kafka consumer handler
}
