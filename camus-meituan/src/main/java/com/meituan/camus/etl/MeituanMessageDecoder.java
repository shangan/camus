package com.meituan.camus.etl;

import com.linkedin.camus.coders.MessageDecoder;
import com.meituan.camus.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Properties;

/**
 * Created by chenshangan on 14-6-27.
 */
public abstract class MeituanMessageDecoder extends MessageDecoder<byte[], String> {

  private final static Logger logger = Logger.getLogger(MeituanMessageDecoder.class);
  protected long DELTA_MILLIS = 0;
  protected boolean ignoreDeltaMillis = false;
  protected long beginTimeMillis;

  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    beginTimeMillis = Long.valueOf(props.getProperty(Configuration.ETL_CURRENT_TIMESTAMP));
    DELTA_MILLIS = Long.valueOf(
      props.getProperty(Configuration.CAMUS_MESSAGE_DELTA_MILLIS, "3600000"));
    int ignoreMin = Integer.valueOf(
      props.getProperty(Configuration.CAMUS_MESSAGE_DELTA_MILLIS_IGNORE_MIN, "60"));
    logger.info("etl_current_timestamp = " + beginTimeMillis);
    Date beginDate = new Date(beginTimeMillis);
    int minute = beginDate.getMinutes();
    logger.info("beginDate = " + beginDate + " minute=" + minute);
    if (minute > ignoreMin) {
      ignoreDeltaMillis = true;
    }
  }

}
