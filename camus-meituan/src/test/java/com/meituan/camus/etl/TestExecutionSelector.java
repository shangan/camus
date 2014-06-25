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
	public void setUp(){

		log.debug("Starting...");
		testPath = "file:///tmp/camus-test."
				+ Calendar.getInstance().getTimeInMillis() + "."
				+ Thread.currentThread().getId();
	}

	@After
	public void tearDown() {
		if (System.getenv("hdfs_keepFiles") == null){
			dirCleanup();
		}
	}

	// TODO: find a way to create file with different modify time
	@Test
	public void testDeltaReload() throws IOException {
		Properties props = new Properties();
		props.setProperty(Configuration.ETL_EXECUTION_DELTA_HOUR, "1");
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path dirPath = new Path(testPath);
		fs.delete(dirPath, true);
		fs.mkdirs(dirPath);
		for(int i = 0; i < 10; i++){
			Path p = new Path(dirPath, "p" + i);
			fs.create(p);
			fs.close();
		}

		FileStatus[] executions = fs.listStatus(dirPath);

		FileStatus previous = MeituanExecutionSelector.select(props, executions);
		Assert.assertEquals(previous, null);

	}
}
