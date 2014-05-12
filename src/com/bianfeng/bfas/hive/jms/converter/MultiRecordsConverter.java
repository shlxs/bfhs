package com.bianfeng.bfas.hive.jms.converter;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import com.bianfeng.bfas.bfac.dtd.MultiRecords;
import com.bianfeng.bfas.hive.jms.JMSSerDe;
import com.bianfeng.bfas.hive.jms.MessageConverter;

public class MultiRecordsConverter implements MessageConverter {

	private String identifier;
	
	private long baseTime;
	
	private String tableName;

	private DateFormat dateFormat;
	
	private int bufferSize = 100000;

	@Override
	public void configure(JobConf jobConf, Properties tableProperties) {
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		identifier = jobConf.get(JMSSerDe.JOB_IDENTIFIER);
		
		//channel name
		tableName = tableProperties.getProperty(JMSSerDe.TABLE_NAME);
		if(tableName.startsWith(JMSSerDe.TABLE_NAME_DEFAULT_SCHEMA)){
			tableName = tableName.substring(JMSSerDe.TABLE_NAME_DEFAULT_SCHEMA_LENGTH);
		}
		
		//buffer size
		String settingBufferSize = tableProperties.getProperty(JMSSerDe.MESSAGE_BUFFER_SIZE);
		if(settingBufferSize != null){
			try {
				bufferSize = Integer.valueOf(settingBufferSize);
			} catch (Exception e) {
				System.err.println("unknown settting buffer size " + settingBufferSize +", use default " + bufferSize);
			}
		}
	}
	
	@Override
	public Serializable process(List<List<String>> rows) {
		if(baseTime == 0){
			if(rows.size() > 0 && rows.get(0).size() > 0){
				String firstColValue = rows.get(0).get(0);
				try {
					baseTime = dateFormat.parse(firstColValue).getTime();
				} catch (ParseException e) {
					throw new RuntimeException("first column data is not a date", e);
				}
			}else{
				throw new RuntimeException("when i want to flush to jms, has no data.");
			}
		}
		
		MultiRecords multiRecords = new MultiRecords();
		multiRecords.setRows(rows);
		multiRecords.setIdentifier(identifier);
		multiRecords.setBaseTime(baseTime);
		multiRecords.setSchemaName(tableName);

		return multiRecords;
	}

	@Override
	public String getChannelName() {
		return tableName;
	}

	@Override
	public int getBufferSize() {
		return bufferSize;
	}

}
