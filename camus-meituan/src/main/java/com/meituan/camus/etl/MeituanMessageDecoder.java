package com.meituan.camus.etl;

import com.linkedin.camus.coders.MessageDecoder;
import com.meituan.camus.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Calendar;
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
    if (!Boolean.parseBoolean(props.getProperty(Configuration.ETL_STARTTIME_ON, "false"))) {
      beginTimeMillis = Configuration.CURRENT_TIMESTAMP;
      DELTA_MILLIS = Long.valueOf(props.getProperty(Configuration.CAMUS_MESSAGE_DELTA_MILLIS, "3600000"));
      int ignoreMin = Integer.valueOf(props.getProperty(Configuration.CAMUS_MESSAGE_DELTA_MILLIS_IGNORE_MIN, "60"));
      Calendar calendar = Calendar.getInstance();
      int minute = calendar.get(Calendar.MINUTE);
      if (minute > ignoreMin) {
        ignoreDeltaMillis = true;
      }
    } else {
      if (props.getProperty(Configuration.ETL_STARTTIME) == null) {
        throw new RuntimeException("configured etl.starttime.on, but no configure etl.starttime");
      }

      logger.info("etl starttime = " + props.getProperty(Configuration.ETL_STARTTIME));
      beginTimeMillis = Long.valueOf(props.getProperty(Configuration.ETL_STARTTIME)) * 1000;

      DELTA_MILLIS = Long.valueOf(props.getProperty(Configuration.CAMUS_MESSAGE_DELTA_MILLIS, "3600000"));
    }
  }

}
