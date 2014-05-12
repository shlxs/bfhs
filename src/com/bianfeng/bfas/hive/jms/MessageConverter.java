package com.bianfeng.bfas.hive.jms;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

public interface MessageConverter {

	void configure(JobConf jobConf, Properties tableProperties);

	Serializable process(List<List<String>> rows);

	String getChannelName();

	int getBufferSize();

}
