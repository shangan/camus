package com.linkedin.camus.etl.kafka;


import com.google.common.base.Preconditions;
import com.meituan.data.zabbix.ZabbixSender;
import com.meituan.hadoop.SecurityUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Properties;

import com.meituan.camus.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Created by chenshangan on 14-5-27.
 */
public class MTCamusJob extends CamusJob {

	private Logger logger = Logger.getLogger(MTCamusJob.class);
	private final static int ZKLOCK_EXIST = 1;
	private final static int ZKLOCK_FAIL = 2;
	private final static int ZKLOCK_SUCCESS = 3;

	private Properties localProps;
	private String zkLockPath = "";
	private ZkClient zkClient = null;

	public MTCamusJob(String[] args) throws IOException, ParseException {
		super();
		localProps = new Properties();
		loadConf(args);

		zkLockPath = localProps.getProperty(Configuration.ZOOKEEPER_BASE_PATH)
				+ localProps.getProperty(Configuration.CAMUS_JOB_NAME);
	}

	private ZkClient createZkClient() throws IOException {
		String zkServers = localProps.getProperty(Configuration.ZOOKEEPER_HOSTS);
		int sessionTimeout = Integer.valueOf(localProps.getProperty(Configuration.ZOOKEEPER_SESSION_TIMEOUT, "5000"));
		int connectTimeout = Integer.valueOf(localProps.getProperty(Configuration.ZOOKEEPER_CONNECT_TIMEOUT, "5000"));
		ZkClient zkClient = new ZkClient(zkServers, sessionTimeout, connectTimeout);

		return zkClient;
	}

	public int addZkLock() {

		try {
			zkClient = createZkClient();
			if (zkClient.exists(zkLockPath)) {
				logger.error("zklock exist: " + zkLockPath);
				return ZKLOCK_EXIST;
			} else {
				zkClient.createEphemeral(zkLockPath, "lock");
			}
		} catch (Exception ex) {
			logger.error("Fail to create zklock: " + zkLockPath, ex);
			return ZKLOCK_FAIL;
		}
		return ZKLOCK_SUCCESS;
	}

	public int releaseZkLock() {

		if (zkClient != null) {
			zkClient.close();
			return ZKLOCK_SUCCESS;
		}

		return ZKLOCK_SUCCESS;
	}


	private void loadConf(String[] args) throws ParseException, IOException {
		Options options = new Options();

		options.addOption("p", true, "properties filename from the classpath");
		options.addOption("P", true, "external properties filename");

		options.addOption(OptionBuilder.withArgName("property=value")
				.hasArgs(2).withValueSeparator()
				.withDescription("use value for given property").create("D"));

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);

		if (!(cmd.hasOption('p') || cmd.hasOption('P'))) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("CamusJob.java", options);
		}

		if (cmd.hasOption('p'))
			localProps.load(this.getClass().getClassLoader().getResourceAsStream(
					cmd.getOptionValue('p')));

		if (cmd.hasOption('P')) {
			File file = new File(cmd.getOptionValue('P'));
			FileInputStream fStream = new FileInputStream(file);
			localProps.load(fStream);
		}

		localProps.putAll(cmd.getOptionProperties("D"));
	}

	private ZabbixSender createZabbixSender() {
		String zabbixServer = localProps.getProperty(Configuration.MONITOR_ZABBIX_SERVER);
		String itemHostName = localProps.getProperty(Configuration.MONITOR_ITEM_HOST);
		Preconditions.checkNotNull(zabbixServer != null, "ZabbixServer can not be null");
		Preconditions.checkNotNull(itemHostName != null, "Zabbix item host can not be null");

		String tokens[] = zabbixServer.split(":");
		Preconditions.checkArgument(tokens.length == 2,
				"Zabbix server is invalid, expect:[host:port], actual: " + zabbixServer);
		ZabbixSender zabbixSender = new ZabbixSender(tokens[0], Integer.parseInt(tokens[1]), itemHostName);
		return zabbixSender;
	}


	public void start(String[] args) throws Exception {

		ZabbixSender zabbixSender = createZabbixSender();
		String camusJobName = localProps.getProperty(Configuration.CAMUS_JOB_NAME);

		try{

			int status = addZkLock();
			if (status == ZKLOCK_SUCCESS) {
				ToolRunner.run(this, args);
			} else {
				if (status == ZKLOCK_EXIST) {
					zabbixSender.send(Configuration.ZABBIX_ITEM_COMMON_KEY,
							String.format("ERROR, job[%s], zklock[%s] exist", camusJobName, zkLockPath));
				} else {
					zabbixSender.send(Configuration.ZABBIX_ITEM_COMMON_KEY,
							String.format("ERROR, job[%s] fail to create zklock[%s]", camusJobName, zkLockPath));
				}
				logger.error("Fail to start job, exit program");
				System.exit(-1);
			}

		}catch(Exception ex){
			throw ex;
		}finally {
			if (releaseZkLock() != ZKLOCK_SUCCESS) {
				// ephemeral node, will be deleted automatically after a session timeout
				zabbixSender.send(Configuration.ZABBIX_ITEM_COMMON_KEY,
						String.format("ERROR, job[%s] fail to release zklock[%s]", camusJobName, zkLockPath));
			}
		}

	}

	public static void main(String[] args) throws IOException {
		final String[] allArgs = args;
		SecurityUtils.doAs("hadoop-data/_HOST@SANKUAI.COM", "/etc/hadoop/keytabs/hadoop-data.keytab",
				null, new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				MTCamusJob job = null;
				try {
					job = new MTCamusJob(allArgs);
					job.start(allArgs);
				} catch (Exception e) {
					e.printStackTrace(System.err);
				}
				return null;
			}
		});
	}
}
