package com.meituan.camus.etl;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import org.apache.log4j.Logger;
import java.util.Properties;
import com.meituan.camus.conf.Configuration;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.meituan.data.binlog.BinlogEntry;
import com.meituan.data.binlog.BinlogEntryUtil;
import com.meituan.data.binlog.BinlogColumn;
import com.meituan.data.binlog.BinlogRow;


/**
 * Created by chenshangan on 14-5-15.
 */
public class BinlogMessageDecoder extends MeituanMessageDecoder {
    
    private final static Logger logger = Logger.getLogger(BinlogMessageDecoder.class);
	public final static String FIELDS_SEPARATOR = "\001";
	public final static String PRIMARY_KEYS_SEPARATOR = "\002";
	public final static String COLUMNS_SEPARATOR = "\002";
	//public final static String COLUMNS_BEFORE_AFTER_SEPARATOR = "\003";
	public final static String COLUMN_NAME_VALUE_SEPARATOR = "\003";
	public final static String COLUMN_VALUE_INTERNAL_SEPARATOR = "\004";
	public final static String LINE_SEPARATOR = "\n";

    public void init(Properties props, String topicName) {
        super.init(props, topicName);
    }

    @Override
    public CamusWrapper<String> decode(byte[] payload) {

        BinlogEntry binlogEntry =BinlogEntryUtil.serializeToBean(payload);
        String payloadString = binlogToHDFS(binlogEntry);
        long timestamp = beginTimeMillis;
        if (!ignoreDeltaMillis) {
            timestamp -= DELTA_MILLIS;
        }
        return new CamusWrapper<String>(payloadString, timestamp);

    }

    public String binlogToHDFS(BinlogEntry binlogEntry){
		StringBuilder tmpsb = new StringBuilder();
		StringBuilder finalsb = new StringBuilder();
        String binlogFileName = binlogEntry.getBinlogFileName();
        long binlogOffset = binlogEntry.getBinlogOffset();
        long executeTime = binlogEntry.getExecuteTime();
        String eventType = binlogEntry.getEventType();
        String tableName = binlogEntry.getTableName();

        String dt = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
        String eventID = dt + "-" + binlogFileName + "-" + binlogOffset; 

		tmpsb.append(FIELDS_SEPARATOR)
			.append(executeTime).append(FIELDS_SEPARATOR)
			.append(eventType).append(FIELDS_SEPARATOR)
			.append(tableName).append(FIELDS_SEPARATOR);

		for (int i = 0; i < binlogEntry.getPrimaryKeys().size(); ++i) {
			if (i > 0) {
				tmpsb.append(PRIMARY_KEYS_SEPARATOR);
			}
			tmpsb.append(binlogEntry.getPrimaryKeys().get(i));
		}
		tmpsb.append(FIELDS_SEPARATOR);

		int rowCnt = 0;
		for (BinlogRow row : binlogEntry.getRowDatas()) {
            //eventID += "-" + rowCnt;
			//if (rowCnt > 0) {
		//		sb.append(FIELDS_SEPARATOR);
		//	}
            StringBuilder sb = new StringBuilder(eventID+"-"+rowCnt);
            sb.append(tmpsb.toString());
			Map<String, BinlogColumn> columns = row.getCurColumns();
			int count = 0;
			for (String columnName : columns.keySet()) {
				if (count > 0) {
					sb.append(COLUMNS_SEPARATOR);
				}
		//		BinlogColumn bColumn = row.getBeforeColumn(columnName);
				BinlogColumn aColumn = row.getAfterColumn(columnName);
				/*			
				if (bColumn != null) {
					sb.append(columnName).append(COLUMN_NAME_VALUE_SEPARATOR).append(
							bColumn.toString(COLUMN_VALUE_INTERNAL_SEPARATOR));				
				}
				*/
				//sb.append(COLUMNS_BEFORE_AFTER_SEPARATOR);
				if (aColumn != null) {
					sb.append(columnName).append(COLUMN_NAME_VALUE_SEPARATOR).append(
							aColumn.toString(COLUMN_VALUE_INTERNAL_SEPARATOR));				
				}
				count++;
			}
			sb.append(LINE_SEPARATOR);
			finalsb.append(sb.toString());
			rowCnt++;
		}
		//sb.append(LINE_SEPARATOR);

		return finalsb.toString();
    }

}

