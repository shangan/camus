package com.linkedin.camus.etl.kafka.common;

import com.linkedin.camus.etl.kafka.coders.MeituanLogPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.Assert;
import static org.mockito.Mockito.*;

import java.io.IOException;

/**
 * Created by chenshangan on 14-5-16.
 */
public class TestHadoop {

  @Test
  public void testPath(){
    Path path = new Path("parent",
            "children/c1");
    Assert.assertEquals("parent/children/c1", path.toString());

  }

  @Test
  public void testPartitioner(){
    MeituanLogPartitioner partitioner = new MeituanLogPartitioner();
    JobContext context = mock(JobContext.class);
    partitioner.generatePartitionedPath(context, "org_nginx", 1, 1, String.valueOf(System.currentTimeMillis()));
  }

}
