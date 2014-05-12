package com.bianfeng.bfas.hive.jms.converter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import com.bianfeng.bfas.hive.jms.JMSSerDe;
import com.bianfeng.bfas.hive.jms.MessageConverter;

public class DefaultMessageConveter implements MessageConverter {
	
	private String tableName;

	@Override
	public void configure(JobConf jobConf, Properties tableProperties) {
		
		//channel name
		tableName = tableProperties.getProperty(JMSSerDe.TABLE_NAME);
		if(tableName.startsWith(JMSSerDe.TABLE_NAME_DEFAULT_SCHEMA)){
			tableName = tableName.substring(JMSSerDe.TABLE_NAME_DEFAULT_SCHEMA_LENGTH);
		}
	}
	
	@Override
	public Serializable process(List<List<String>> rows) {
		return (ArrayList<List<String>>)rows;
	}

	@Override
	public String getChannelName() {
		return tableName;
	}

	@Override
	public int getBufferSize() {
		return 0;
	}

}
