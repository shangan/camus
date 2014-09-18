package com.meituan.camus.etl.kafka.coders;

import com.linkedin.camus.coders.Partitioner;
import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.kafka.common.DateUtils;
import com.linkedin.camus.etl.kafka.mapred.EtlMultiOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;


/**
 * Created by chenshangan on 14-5-20.
 */
public class MeituanLogPartitioner implements Partitioner {

  private final static Logger logger = Logger.getLogger(MeituanLogPartitioner.class);
  protected static final String OUTPUT_DATE_FORMAT = "YYYYMMdd/HH";
  protected DateTimeFormatter outputDateFormatter = null;

  @Override
  public String encodePartition(JobContext context, IEtlKey etlKey) {
    long outfilePartitionMs = EtlMultiOutputFormat.getEtlOutputFileTimePartitionMins(context) * 60000L;
    return "" + DateUtils.getPartition(outfilePartitionMs, etlKey.getTime());
  }

  @Override
  public String generatePartitionedPath(JobContext context, String topic, int brokerId, int partitionId, String encodedPartition) {
    // sample topic: org.nginx
    if (outputDateFormatter == null) {
      outputDateFormatter = DateUtils.getDateTimeFormatter(
        OUTPUT_DATE_FORMAT,
        DateTimeZone.forID(EtlMultiOutputFormat.getDefaultTimeZone(context))
      );
    }
    String category = topic;
    if (topic.startsWith("org_")) {
      // '.' in topic is replaced by '_', convert it back
      String tokens[] = topic.split("_", 2);
      if (tokens.length == 2) {
        category = tokens[1] + "org";
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append(category).append("/");

    String bucket = outputDateFormatter.print(Long.valueOf(encodedPartition));
    String tokens[] = bucket.split("/");
    sb.append("dt=").append(tokens[0]).append("/hour=").append(tokens[1]);
    System.out.println(sb.toString());

    return sb.toString();


  }
}
