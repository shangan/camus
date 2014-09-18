package com.meituan.camus.etl;

import com.meituan.camus.conf.Configuration;
import com.meituan.camus.utils.DateHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by chenshangan on 14-6-23.
 */
public class MeituanExecutionSelector {

  private final static Logger log = Logger.getLogger(MeituanExecutionSelector.class);
  private final static String RELOAD_EXECUTION_SUFFIX = ".Reload";

  public static FileStatus select(Properties props, FileSystem fs, FileStatus[] executions) throws IOException {
    return select(props, fs, executions, false);

  }

  public static FileStatus select(Properties props, FileSystem fs, FileStatus[] executions, boolean renameNewer) throws IOException {
    if (executions == null || executions.length == 0) {
      return null;
    }

    String currentDateStr = props.getProperty(Configuration.ETL_EXECUTION_CURRENT_DATE);
    String deltaHourStr = props.getProperty(Configuration.ETL_EXECUTION_DELTA_HOUR);
    String reloadStr = props.getProperty(Configuration.ETL_RELOAD);
    boolean reload = false;
    if (reloadStr != null && reloadStr.equalsIgnoreCase("true")) {
      reload = true;
    }

    if (currentDateStr == null && deltaHourStr == null && (!reload)) {
      return executions[executions.length - 1];
    }

    long currentTimeMillis = System.currentTimeMillis();
    try {
      if (currentDateStr != null) {
        currentTimeMillis = DateHelper.dayStartTimeMillis(currentDateStr);
      }

      if (deltaHourStr != null) {
        currentTimeMillis -= DateHelper.HOUR_TIME_MILLIS * Integer.valueOf(deltaHourStr);
      }
    } catch (Exception ex) {
      log.error("date format error", ex);
      return null;
    }
    log.info("History execution target time: " + DateHelper.toTimeString(currentTimeMillis));

    String dateFormat = props.getProperty(Configuration.ETL_OUTPUT_FILE_DATETIME_FORMAT, "yyyy-MM-dd-HH-mm-ss");
    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    int idx = -1;
    FileStatus targetExecution = null;
    if (!reload) {
      for (FileStatus f : executions) {
        idx++;
        String pathName = f.getPath().getName();
        if (pathName.endsWith(RELOAD_EXECUTION_SUFFIX)) {
          continue;
        }
        try {
          Date date = sdf.parse(pathName);
          long createTimeMillis = date.getTime();
          if (DateHelper.isSameDay(createTimeMillis, currentTimeMillis)) {
            if (DateHelper.hour(createTimeMillis) == DateHelper.hour(currentTimeMillis)) {
              log.info("find path: " + f.getPath());
              targetExecution = f;
              break;
            }
          }
        } catch (ParseException e) {
          log.warn("Incorrect time format, path: " + f.getPath().toString());
        }

      }
    }

    if (renameNewer) {
      for (int i = idx + 1; i < executions.length; i++) {
        if (executions[i].getPath().getName().endsWith(RELOAD_EXECUTION_SUFFIX)) {
          continue;
        }
        renameHistoryExecution(fs, executions[i]);
      }
    }

    return targetExecution;
  }

  public static boolean renameHistoryExecution(FileSystem fs, FileStatus f) throws IOException {
    Path source = f.getPath();
    Path target = new Path(source.toString() + RELOAD_EXECUTION_SUFFIX);
    if (fs.exists(source)) {
      fs.rename(source, target);
    }
    return true;
  }

}
