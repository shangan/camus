package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.meituan.camus.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.Properties;


/**
 * Created by chenshangan on 14-5-15.
 */
public class SimpleStringMessageDecoder extends MessageDecoder<byte[], String> {
  private final static Logger logger = Logger.getLogger(SimpleStringMessageDecoder.class);
  private long DELTA_MILLIS = 0;
  private boolean ignoreDeltaMillis = false;
  @Override
  public void init(Properties props, String topicName) {
    super.init(props, topicName);

    DELTA_MILLIS = Long.valueOf(
            props.getProperty(Configuration.CAMUS_MESSAGE_DELTA_MILLIS, "0"));
    int ignoreMin = Integer.valueOf(
            props.getProperty(Configuration.CAMUS_MESSAGE_DELTA_MILLIS_IGNORE, "60"));
    Calendar calendar = Calendar.getInstance();
    int minute = calendar.get(Calendar.MINUTE);
    if(minute > ignoreMin){
      ignoreDeltaMillis = true;
    }
  }
  @Override
  public CamusWrapper<String> decode(byte[] payload) {

    String payloadString = new String(payload);
    long timestamp = System.currentTimeMillis();
    if(! ignoreDeltaMillis){
      timestamp -= DELTA_MILLIS;
    }
    return new CamusWrapper<String>(payloadString, timestamp);


  }

}
