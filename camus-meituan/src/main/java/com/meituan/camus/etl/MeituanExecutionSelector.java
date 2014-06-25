package com.meituan.camus.etl;

import com.meituan.camus.conf.Configuration;
import com.meituan.camus.utils.DateHelper;
import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.Logger;

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

		for(FileStatus f : executions){
			long modifyTime = f.getModificationTime();
			if(DateHelper.isSameDay(modifyTime, currentTimeMillis)){
				if(DateHelper.hour(modifyTime) == DateHelper.hour(currentTimeMillis)){
					return f;
				}
			}
		}

		return null;

	}

}
