package com.meituan.camus.utils;

import com.meituan.camus.bean.LeaderInfo;
import com.meituan.camus.bean.TopicInfo;
import com.meituan.camus.conf.Configuration;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;


/**
 * Created by chenshangan on 14-6-10.
 */
public class KafkaOffsetUtil {

  private Properties props;
  private final static Logger log = Logger.getLogger(KafkaOffsetUtil.class);

  public KafkaOffsetUtil(Properties props) {
    this.props = props;
  }

  public TopicInfo getTopicInfo(String topic) throws Exception {

    TopicInfo topicInfo = new TopicInfo(topic);
    List<TopicMetadata> topicMetadataList = getKafkaMetadata(props);
    Map<LeaderInfo, List<TopicAndPartition>> offsetRequestInfo =
      new HashMap<LeaderInfo, List<TopicAndPartition>>();

    TopicMetadata expectTopicMetadata = null;
    for (TopicMetadata topicMetadata : topicMetadataList) {
      String crtTopic = topicMetadata.topic();
      if (crtTopic.equalsIgnoreCase(topic)) {
        expectTopicMetadata = topicMetadata;
        break;
      }
    }

    if (expectTopicMetadata != null) {
      for (PartitionMetadata partitionMetadata : expectTopicMetadata
        .partitionsMetadata()) {
        if (partitionMetadata.errorCode() != ErrorMapping
          .NoError()) {
          log.info("Fail to fetch topic meta.Topic : "
            + expectTopicMetadata.topic()
            + " and Partition : "
            + partitionMetadata.partitionId()
            + " Exception : "
            + ErrorMapping
            .exceptionFor(partitionMetadata
              .errorCode()));
          continue;
        } else {
          LeaderInfo leader = new LeaderInfo(new URI("tcp://"
            + partitionMetadata.leader()
            .getConnectionString()),
            partitionMetadata.leader().id());
          List<TopicAndPartition> topicAndPartitions = offsetRequestInfo.get(leader);
          if (topicAndPartitions == null) {
            topicAndPartitions = new ArrayList<TopicAndPartition>();
            offsetRequestInfo.put(leader, topicAndPartitions);
          }
          topicAndPartitions.add(new TopicAndPartition(
            expectTopicMetadata.topic(),
            partitionMetadata.partitionId()));
        }
      }
      return fetchTopicOffset(offsetRequestInfo).get(topic);
    }
    return topicInfo;

  }

  public Map<String, TopicInfo> fetchTopicOffset(
    Map<LeaderInfo, List<TopicAndPartition>> offsetRequestInfo) {

    Map<String, TopicInfo> topicInfoMap = new HashMap<String, TopicInfo>();

    for (LeaderInfo leader : offsetRequestInfo.keySet()) {
      SimpleConsumer consumer = new SimpleConsumer(leader.getUri()
        .getHost(), leader.getUri().getPort(),
        Integer.valueOf(props.getProperty(Configuration.KAFKA_TIMEOUT_VALUE, "5000")),
        Integer.valueOf(props.getProperty(Configuration.KAFKA_FETCH_BUFFER_SIZE, "10000000")),
        props.getProperty(Configuration.KAFKA_CLIENT_NAME));
      // Latest Offset
      PartitionOffsetRequestInfo partitionLatestOffsetRequestInfo = new PartitionOffsetRequestInfo(
        kafka.api.OffsetRequest.LatestTime(), 1);
      // Earliest Offset
      PartitionOffsetRequestInfo partitionEarliestOffsetRequestInfo = new PartitionOffsetRequestInfo(
        kafka.api.OffsetRequest.EarliestTime(), 1);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> latestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      Map<TopicAndPartition, PartitionOffsetRequestInfo> earliestOffsetInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      List<TopicAndPartition> topicAndPartitions = offsetRequestInfo
        .get(leader);
      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
        latestOffsetInfo.put(topicAndPartition,
          partitionLatestOffsetRequestInfo);
        earliestOffsetInfo.put(topicAndPartition,
          partitionEarliestOffsetRequestInfo);

      }

      OffsetResponse latestOffsetResponse = consumer
        .getOffsetsBefore(new OffsetRequest(latestOffsetInfo,
          kafka.api.OffsetRequest.CurrentVersion(),
          props.getProperty(Configuration.KAFKA_CLIENT_NAME)));
      OffsetResponse earliestOffsetResponse = consumer
        .getOffsetsBefore(new OffsetRequest(earliestOffsetInfo,
          kafka.api.OffsetRequest.CurrentVersion(),
          props.getProperty(Configuration.KAFKA_CLIENT_NAME)));

      consumer.close();
      for (TopicAndPartition topicAndPartition : topicAndPartitions) {
        String topic = topicAndPartition.topic();
        long latestOffset = latestOffsetResponse.offsets(
          topicAndPartition.topic(),
          topicAndPartition.partition())[0];
        long earliestOffset = earliestOffsetResponse.offsets(
          topicAndPartition.topic(),
          topicAndPartition.partition())[0];
        TopicInfo topicInfo = topicInfoMap.get(topic);
        if (topicInfo == null) {
          topicInfo = new TopicInfo(topic);
          topicInfoMap.put(topic, topicInfo);
        }
        topicInfo.setPartitionInfo(topicAndPartition.partition(), earliestOffset, latestOffset, 0);

      }
    }

    return topicInfoMap;


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


  public static void main(String[] args) throws Exception {

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

    TopicInfo topicInfo = util.getTopicInfo(props.getProperty("topic"));
    if (topicInfo != null) {
      System.out.println(topicInfo.toString());
    }


  }

}
