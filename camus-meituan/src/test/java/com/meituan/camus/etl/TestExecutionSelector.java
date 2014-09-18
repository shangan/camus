package com.meituan.camus.etl;

import com.meituan.camus.conf.Configuration;
import junit.framework.Assert;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;


/**
 * Created by chenshangan on 14-6-25.
 */
public class TestExecutionSelector {

  private final static Logger log = Logger.getLogger(TestExecutionSelector.class);

  private String testPath;


  private void dirCleanup() {
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);
      Path dirPath = new Path(testPath);
      if (fs.exists(dirPath)) {
        fs.delete(dirPath, true);
      }
    } catch (IOException eIO) {
      log.warn("IO Error in test cleanup", eIO);
    }
  }

  @Before
  public void setUp() {

    log.debug("Starting...");
    testPath = "file:///tmp/camus-test."
      + Calendar.getInstance().getTimeInMillis() + "."
      + Thread.currentThread().getId();
  }

  @After
  public void tearDown() {
    if (System.getenv("hdfs_keepFiles") == null) {
      dirCleanup();
    }
  }

  @Test
  public void testDeltaReload() throws IOException, ParseException {
    Properties props = new Properties();
    int delta = 0;
    props.setProperty(Configuration.ETL_EXECUTION_DELTA_HOUR, String.valueOf(delta));
    org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(testPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);
    String dateFormat = props.getProperty(Configuration.ETL_OUTPUT_FILE_DATETIME_FORMAT, "yyyy-MM-dd-HH-mm-ss");
    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    Calendar calendar = Calendar.getInstance();
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    Path expected = null;
    for (int i = 0; i < 24; i++) {

      calendar.set(Calendar.HOUR_OF_DAY, i);
      Path p = new Path(dirPath, sdf.format(calendar.getTime()));
      fs.create(p);
      fs.close();
      if (i == hour - delta) {
        expected = p;
      }
    }

    FileStatus[] executions = fs.listStatus(dirPath);

    FileStatus previous = MeituanExecutionSelector.select(props, fs, executions, true);
    Assert.assertNotNull(previous);
    Assert.assertEquals(previous.getPath(), expected);

    props.setProperty(Configuration.ETL_RELOAD, "true");
    previous = MeituanExecutionSelector.select(props, fs, executions, true);
    Assert.assertNull(previous);

  }
}
