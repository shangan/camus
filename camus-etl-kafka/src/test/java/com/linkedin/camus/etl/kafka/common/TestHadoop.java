package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.kafka.mapred.EtlInputFormat;
import com.meituan.camus.etl.kafka.coders.MeituanLogPartitioner;
import com.meituan.data.zabbix.ZabbixSender;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * Created by chenshangan on 14-5-16.
 */
public class TestHadoop {

  @Test
  public void testPath() {
    Path path = new Path("parent",
      "children/c1");
    Assert.assertEquals("parent/children/c1", path.toString());

  }

  @Test
  public void testPartitioner() throws IOException {
    MeituanLogPartitioner partitioner = new MeituanLogPartitioner();
    Job job = new Job(new JobConf());
    job.getConfiguration().set("etl.default.timezone", "Asia/Shanghai");
    partitioner.generatePartitionedPath(job, "org_nginx", 1, 1, String.valueOf(System.currentTimeMillis()));
  }

  @Test
  public void testEtlInputFormat() throws IOException {
    EtlInputFormat inputFormat = new EtlInputFormat();
    Job job = new Job(new JobConf());
    job.getConfiguration().set("monitor.zabbix.server", "zabbix.lan.sankuai.com:10051");
    job.getConfiguration().set("monitor.item.host", "data-log01.lf.sankuai.com");
    ZabbixSender zabbixSender = inputFormat.createZabbixSender(job);
    zabbixSender.send("camusJob-loadLog", "invalid offset");
  }

}
