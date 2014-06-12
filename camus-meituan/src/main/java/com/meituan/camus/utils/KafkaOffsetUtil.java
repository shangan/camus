package com.meituan.camus.utils;

import com.meituan.camus.conf.Configuration;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;


/**
 * Created by chenshangan on 14-6-10.
 */
public class KafkaOffsetUtil {

  private Properties props;
  private final static Logger log = Logger.getLogger(KafkaOffsetUtil.class);

  public KafkaOffsetUtil(Properties props){
    this.props = props;
  }

  public long[] partitionOffsetRange(String topic, String partition) {
    long[] offsets = new long[2];

    return offsets;

  }


  public List<Long[]> topicOffsetRange(String topic) {

    List<Long[]> topicOffsetRange = new ArrayList<Long[]>();


    return topicOffsetRange;
  }

  private SimpleConsumer createConsumer(Properties props, String broker) {
    String[] hostPort = broker.split(":");
    SimpleConsumer consumer = new SimpleConsumer(
            hostPort[0],
            Integer.valueOf(hostPort[1]),
            Integer.valueOf(props.getProperty(Configuration.KAFKA_TIMEOUT_VALUE)),
            Integer.valueOf(props.getProperty(Configuration.KAFKA_FETCH_BUFFER_SIZE)),
            props.getProperty(Configuration.KAFKA_CLIENT_NAME));
    return consumer;
  }

  private List<TopicMetadata> getKafkaMetadata(Properties props) {
    ArrayList<String> metaRequestTopics = new ArrayList<String>();
    String brokerString = props.getProperty(Configuration.KAFKA_BROKERS);
    List<String> brokers = Arrays.asList(brokerString.split("\\s*,\\s*"));
    Collections.shuffle(brokers);
    boolean fetchMetaDataSucceeded = false;
    int i = 0;
    List<TopicMetadata> topicMetadataList = null;
    Exception savedException = null;
    while (i < brokers.size() && !fetchMetaDataSucceeded) {
      SimpleConsumer consumer = createConsumer(props, brokers.get(i));
      log.info(String.format("Fetching metadata from broker %s with client id %s for %d topic(s) %s",
              brokers.get(i), consumer.clientId(), metaRequestTopics.size(), metaRequestTopics));
      try {
        topicMetadataList = consumer.send(new TopicMetadataRequest(metaRequestTopics)).topicsMetadata();
        fetchMetaDataSucceeded = true;
      } catch (Exception e) {
        savedException = e;
        log.warn(String.format("Fetching topic metadata with client id %s for topics [%s] from broker [%s] failed",
                consumer.clientId(), metaRequestTopics, brokers.get(i)), e);
      } finally {
        consumer.close();
        i++;
      }
    }
    if (!fetchMetaDataSucceeded) {
      throw new RuntimeException("Failed to obtain metadata!", savedException);
    }
    return topicMetadataList;
  }


  public static void main(String[] args) throws IOException, ParseException {

    Properties props = new Properties();
    Options options = new Options();

    options.addOption("P", true, "external properties filename");

    options.addOption(OptionBuilder.withArgName("property=value")
            .hasArgs(2).withValueSeparator()
            .withDescription("use value for given property").create("D"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    if (!(cmd.hasOption('p') || cmd.hasOption('P'))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("KafkaOffsetUtil.java", options);
      System.exit(0);
    }

    if (cmd.hasOption('P')) {
      File file = new File(cmd.getOptionValue('P'));
      FileInputStream fStream = new FileInputStream(file);
      props.load(fStream);
    }

    props.putAll(cmd.getOptionProperties("D"));


    KafkaOffsetUtil util = new KafkaOffsetUtil(props);


  }

}
