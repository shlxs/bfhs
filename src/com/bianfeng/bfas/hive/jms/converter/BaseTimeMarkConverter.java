package com.bianfeng.bfas.hive.jms.converter;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import com.bianfeng.bfas.bfac.dtd.TimeMarkMultiRecords;
import com.bianfeng.bfas.hive.jms.JMSSerDe;
import com.bianfeng.bfas.hive.jms.MessageConverter;

public class BaseTimeMarkConverter implements MessageConverter {

	private String identifier;
	
	private String channelName;
	
	private DateFormat dateFormat;

	@Override
	public void configure(JobConf jobConf, Properties tableProperties) {
		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		identifier = jobConf.get(JMSSerDe.JOB_IDENTIFIER);
	}

	@Override
	public Serializable process(List<List<String>> rows) {
		if(rows.size() != 1){
			throw new RuntimeException("BaseTimeMark must process one record once, not" + rows.size());
		}
		
		List<String> row = (List<String>)rows.get(0);
		if(row.size() != 2){
			throw new RuntimeException("insert data must has two columns, first is tableName, second is baseTime," + row + " dosen't match.");
		}
		
		try {
			channelName = row.get(0);
			long baseTime = dateFormat.parse(row.get(1)).getTime();
			
			TimeMarkMultiRecords baseTimeMark = new TimeMarkMultiRecords(channelName, baseTime, identifier);
			
			return baseTimeMark;
		} catch (ParseException e) {
			throw new RuntimeException("convert date error", e);
		}
	}

	@Override
	public String getChannelName() {
		return channelName;
	}

	@Override
	public int getBufferSize() {
		return 0;
	}

}
