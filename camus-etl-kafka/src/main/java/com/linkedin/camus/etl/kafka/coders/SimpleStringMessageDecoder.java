package com.linkedin.camus.etl.kafka.coders;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;


/**
 * Created by chenshangan on 14-5-15.
 */
public class SimpleStringMessageDecoder extends MessageDecoder<byte[], String> {
  private final static Logger logger = Logger.getLogger(SimpleStringMessageDecoder.class);

  @Override
  public CamusWrapper<String> decode(byte[] payload) {

    String payloadString = new String(payload);
    long timestamp = System.currentTimeMillis();
    return new CamusWrapper<String>(payloadString, timestamp);


  }

}
