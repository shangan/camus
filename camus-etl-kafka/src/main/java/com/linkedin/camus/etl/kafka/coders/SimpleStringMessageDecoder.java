package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.meituan.camus.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Properties;


/**
 * Created by chenshangan on 14-5-15.
 */
public class SimpleStringMessageDecoder extends MessageDecoder<byte[], String> {
  private final static Logger logger = Logger.getLogger(SimpleStringMessageDecoder.class);
  private long DELTA_TIMEMILLIS = 0;
  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);
    DELTA_TIMEMILLIS = Long.valueOf(
            props.getProperty(Configuration.CAMUS_MESSAGE_DELTA_MILLIS, "0"));


  }
  @Override
  public CamusWrapper<String> decode(byte[] payload) {

    String payloadString = new String(payload);
    long timestamp = System.currentTimeMillis() - DELTA_TIMEMILLIS;
    return new CamusWrapper<String>(payloadString, timestamp);


  }

}
