package com.meituan.camus.etl;

import com.meituan.camus.conf.Configuration;
import com.meituan.camus.utils.DateHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created by chenshangan on 14-6-23.
 */
public class MeituanExecutionSelector {

	private final static Logger log = Logger.getLogger(MeituanExecutionSelector.class);

	public static FileStatus select(Properties props, FileStatus[] executions){
		if(executions == null || executions.length == 0){
			return null;
		}

		String currentDateStr = props.getProperty(Configuration.ETL_EXECUTION_CURRENT_DATE);
		String deltHourStr = props.getProperty(Configuration.ETL_EXECUTION_DELTA_HOUR);

		if(currentDateStr == null && deltHourStr == null){
			return executions[executions.length - 1];
		}

		long currentTimeMillis = System.currentTimeMillis();
		if(currentDateStr != null){
			currentTimeMillis = DateHelper.dayStartTimeMillis(currentDateStr);
		}

		if(deltHourStr != null){
			currentTimeMillis -= DateHelper.HOUR_TIME_MILLIS * Integer.valueOf(deltHourStr);
		}
		log.info("History execution target time: " + DateHelper.toTimeString(currentTimeMillis));

		String dateFormat = props.getProperty(Configuration.ETL_OUTPUT_FILE_DATETIME_FORMAT, "YYYY-MM-dd-HH-mm-ss");
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);

		for(FileStatus f : executions){
			String pathName = f.getPath().getName();
			try {
				Date date = sdf.parse(pathName);
				long createTimeMillis = date.getTime();
				if(DateHelper.isSameDay(createTimeMillis, currentTimeMillis)){
					if(DateHelper.hour(createTimeMillis) == DateHelper.hour(currentTimeMillis)){
						return f;
					}
				}
			} catch (ParseException e) {
				log.warn("Incorrect time format, path: " + f.getPath().toString());
			}

		}

		return null;

	}

}
