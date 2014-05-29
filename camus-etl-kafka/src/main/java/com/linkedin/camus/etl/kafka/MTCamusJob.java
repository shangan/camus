package com.linkedin.camus.etl.kafka;


import com.meituan.hadoop.SecurityUtils;
import org.apache.hadoop.util.ToolRunner;

import java.security.PrivilegedAction;

/**
 * Created by chenshangan on 14-5-27.
 */
public class MTCamusJob extends CamusJob {

  public MTCamusJob(){
    super();
  }

  public static void main(String[] args) throws Exception{
    final String[] allArgs = args;
    SecurityUtils.doAs("hadoop-data/_HOST@SANKUAI.COM", "/etc/hadoop/keytabs/hadoop-data.keytab",
            null, new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        MTCamusJob job = new MTCamusJob();
        try {
          ToolRunner.run(job, allArgs);
        } catch (Exception e) {
          e.printStackTrace(System.err);
        }
        return null;
      }
    });
  }
}
