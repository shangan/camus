package com.meituan.camus.etl;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.meituan.camus.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.Properties;


/**
 * Created by chenshangan on 14-5-15.
 */
public class SimpleStringMessageDecoder extends MeituanMessageDecoder {
  private final static Logger logger = Logger.getLogger(SimpleStringMessageDecoder.class);

  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    if (Boolean.parseBoolean(props.getProperty(Configuration.ETL_STARTTIME_ON, "false"))) {
      if (props.getProperty(Configuration.ETL_STARTTIME) == null) {
        throw new RuntimeException("configured etl.starttime.on, but no configure etl.starttime");
      }
      logger.info("etl starttime = " + props.getProperty(Configuration.ETL_STARTTIME));
      beginTimeMillis = Long.valueOf(props.getProperty(Configuration.ETL_STARTTIME)) * 1000;
      ignoreDeltaMillis = false;
      DELTA_MILLIS = 3600000;
    }
  }

  @Override
  public CamusWrapper<String> decode(byte[] payload) {

    String payloadString = new String(payload);
    if (!payloadString.endsWith("\n")) {
      payloadString += "\n";
    }
    long timestamp = beginTimeMillis;
    if (!ignoreDeltaMillis) {
      timestamp -= DELTA_MILLIS;
    }
    return new CamusWrapper<String>(payloadString, timestamp);

  }

}
