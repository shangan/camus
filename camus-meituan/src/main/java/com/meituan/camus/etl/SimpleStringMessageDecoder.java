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
  }

  @Override
  public CamusWrapper<String> decode(byte[] payload) {

    String payloadString = new String(payload);
    long timestamp = beginTimeMillis;
    if (!ignoreDeltaMillis) {
      timestamp -= DELTA_MILLIS;
    }
    return new CamusWrapper<String>(payloadString, timestamp);

  }

}
