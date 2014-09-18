package com.meituan.camus.conf;

/**
 * Created by chenshangan on 14-6-10.
 */
public class Configuration {

  public static final String CAMUS_JOB_NAME = "camus.job.name";
  public static final String ETL_EXECUTION_BASE_PATH = "etl.execution.base.path";
  public static final String ETL_EXECUTION_HISTORY_PATH = "etl.execution.history.path";
  public static final String ETL_COUNTS_PATH = "etl.counts.path";
  public static final String ETL_KEEP_COUNT_FILES = "etl.keep.count.files";
  public static final String ETL_OUTPUT_FILE_DATETIME_FORMAT = "etl.output.file.datetime.format";
  public static final String ETL_DEFAULT_TIMEZONE = "etl.default.timezone";
  public static final String ETL_EXECUTION_HISTORY_MAX_OF_QUOTA = "etl.execution.history.max.of.quota";

  public static final String ZK_AUDIT_HOSTS = "zookeeper.audit.hosts";
  public static final String KAFKA_MONITOR_TIER = "kafka.monitor.tier";
  public static final String CAMUS_MESSAGE_ENCODER_CLASS = "camus.message.encoder.class";
  public static final String BROKER_URI_FILE = "brokers.uri";
  public static final String POST_TRACKING_COUNTS_TO_KAFKA = "post.tracking.counts.to.kafka";
  public static final String KAFKA_FETCH_REQUEST_MAX_WAIT = "kafka.fetch.request.max.wait";
  public static final String KAFKA_FETCH_REQUEST_MIN_BYTES = "kafka.fetch.request.min.bytes";
  public static final String KAFKA_FETCH_REQUEST_CORRELATION_ID = "kafka.fetch.request.correlationid";
  public static final String KAFKA_CLIENT_NAME = "kafka.client.name";
  public static final String KAFKA_FETCH_BUFFER_SIZE = "kafka.fetch.buffer.size";
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_HOST_URL = "kafka.host.url";
  public static final String KAFKA_HOST_PORT = "kafka.host.port";
  public static final String KAFKA_TIMEOUT_VALUE = "kafka.timeout.value";
  public static final String ZOOKEEPER_HOSTS = "zookeeper.hosts";
  public static final String ZOOKEEPER_SESSION_TIMEOUT = "zookeeper.session.timeout";
  public static final String ZOOKEEPER_CONNECT_TIMEOUT = "zookeeper.connect.timeout";

  public static final String KAFKA_BLACKLIST_TOPIC = "kafka.blacklist.topics";
  public static final String KAFKA_WHITELIST_TOPIC = "kafka.whitelist.topics";

  public static final String KAFKA_MOVE_TO_LAST_OFFSET_LIST = "kafka.move.to.last.offset.list";

  public static final String KAFKA_CLIENT_BUFFER_SIZE = "kafka.client.buffer.size";
  public static final String KAFKA_CLIENT_SO_TIMEOUT = "kafka.client.so.timeout";

  public static final String KAFKA_MAX_PULL_HRS = "kafka.max.pull.hrs";
  public static final String KAFKA_MAX_PULL_MINUTES_PER_TASK = "kafka.max.pull.minutes.per.task";
  public static final String KAFKA_MAX_HISTORICAL_DAYS = "kafka.max.historical.days";

  public static final String CAMUS_MESSAGE_DECODER_CLASS = "camus.message.decoder.class";
  public static final String ETL_IGNORE_SCHEMA_ERRORS = "etl.ignore.schema.errors";
  public static final String ETL_AUDIT_IGNORE_SERVICE_TOPIC_LIST = "etl.audit.ignore.service.topic.list";

  // meituan defined configuration
  public static final String KAFKA_BEGIN_TIMESTAMP = "kafka.begin.timestamp";
  public static final String ETL_FAIL_INVALID_OFFSET = "etl.fail.invalid.offset";
  public static final String MONITOR_ZABBIX_SERVER = "monitor.zabbix.server";
  public static final String MONITOR_ITEM_HOST = "monitor.item.host";
  public static final String ZOOKEEPER_BASE_PATH = "zookeeper.base.path";
  public static final String CAMUS_MESSAGE_DELTA_MILLIS = "camus.message.delta.millis";
  public static final String CAMUS_MESSAGE_DELTA_MILLIS_IGNORE_MIN = "camus.message.delta.millis.ignore.min";
  public static final String ETL_TARGET_TOPIC = "etl.target.topic";
  public static final String KAFKA_WHITELIST_TOPIC_STRICT = "kafka.whitelist.topic.strict";
  public static final String ETL_RELOAD = "etl.reload";
  public static final String ETL_EXECUTION_DELTA_HOUR = "etl.execution.delta.hour";
  public static final String ETL_EXECUTION_CURRENT_DATE = "etl.execution.current.date";
  public static final String ETL_TEST_SWITCH = "etl.test.switch";

  public static final long CURRENT_TIMESTAMP = System.currentTimeMillis();


  public static final String ZABBIX_ITEM_COMMON_KEY = "camus.etl.failinfo";
  public static final String CAMUS_JOB_NAME_VARIABLE = "{" + CAMUS_JOB_NAME + "}";

  public static String getEtlExecutionBasePath(String basePath, String jobName) {
    return basePath.replace(CAMUS_JOB_NAME_VARIABLE, jobName.trim());
  }

  public static String getEtlExecutionHistoryPath(String historyPath, String jobName) {
    return historyPath.replace(CAMUS_JOB_NAME_VARIABLE, jobName.trim());
  }

}
