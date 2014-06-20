package com.linkedin.camus.etl.kafka.common;

import com.meituan.camus.bean.PartitionInfo;
import com.meituan.camus.bean.TopicInfo;
import com.meituan.camus.conf.Configuration;
import com.meituan.camus.utils.HdfsUtil;
import com.meituan.camus.utils.KafkaOffsetUtil;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by chenshangan on 14-6-18.
 */
public class CamusUtil {

	public static final String OFFSET_PREFIX = "offsets";
	private Properties props;
	private KafkaOffsetUtil kafkaOffsetUtil;
	private final static Logger log = Logger.getLogger(CamusUtil.class);

	public CamusUtil(Properties props) {
		this.props = props;
		kafkaOffsetUtil = new KafkaOffsetUtil(props);
	}

	@Deprecated
	public TopicInfo getTopicInfo(String topicName) throws Exception {
		//需要使用camus-etl-kafka下的EtlKey类反序列化，包名需要一致
		TopicInfo topicInfo = kafkaOffsetUtil.getTopicInfo(topicName);
		if (topicInfo == null) {
			return topicInfo;
		}

		String execHistoryStr = Configuration.getEtlExecutionHistoryPath(
				props.getProperty(Configuration.ETL_EXECUTION_HISTORY_PATH),
				props.getProperty(Configuration.CAMUS_JOB_NAME));
//        Path execHistory = new Path(execHistoryStr);
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration(true);
		HdfsUtil hdfsUtil = new HdfsUtil(conf);
		FileStatus[] fileStatuses = hdfsUtil.getDirectoryFromHdfs(execHistoryStr);
		if (fileStatuses.length > 0) {
			Path lastExecutionPath = fileStatuses[fileStatuses.length - 1].getPath();
			FileSystem fs = lastExecutionPath.getFileSystem(conf);
			log.info("Last execution path: " + lastExecutionPath.toString());

			for (FileStatus f : fs.listStatus(lastExecutionPath, new OffsetFileFilter())) {
				log.info("read file: " + f.getPath().toString());
				SequenceFile.Reader reader = new SequenceFile.Reader(fs,
						f.getPath(), conf);
				EtlKey key = new EtlKey();
				while (reader.next(key, NullWritable.get())) {
					String crtTopic = key.getTopic();
					if (crtTopic.equalsIgnoreCase(topicName)) {
						PartitionInfo partitionInfo = topicInfo.getPartitionInfo(key.getPartition());
						if (partitionInfo != null) {
							if (partitionInfo.getCurrentOffset() < key.getOffset()) {
								partitionInfo.setCurrentOffset(key.getOffset());
							}
						} else {
							topicInfo.setPartitionCurrentOffset(key.getPartition(), key.getOffset());
						}
					}
					key = new EtlKey();
				}
				reader.close();
			}
		}

		return topicInfo;

	}

	private class OffsetFileFilter implements PathFilter {

		@Override
		public boolean accept(Path arg0) {
			return arg0.getName()
					.startsWith(OFFSET_PREFIX);
		}
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


		CamusUtil util = new CamusUtil(props);

		TopicInfo topicInfo = util.getTopicInfo(props.getProperty("topic"));
		if (topicInfo != null) {
			System.out.println(topicInfo.toString());
		}


	}


}
