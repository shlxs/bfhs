package com.bianfeng.bfas.hive.jms;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

public class JMSRecordWriter implements FileSinkOperator.RecordWriter {
	private JMSCacheQueue cacheQueue;
	
	public JMSRecordWriter(JobConf jobConf, Properties properties,
			Progressable progressable) {
		cacheQueue = new JMSCacheQueue(jobConf, properties);
	}

	@Override
	public void write(Writable writable) throws IOException {
		cacheQueue.write(writable);
	}

	@Override
	public void close(boolean abort) throws IOException {
		if(abort){
			cacheQueue.rollback();
		}else{
			cacheQueue.commit();
		}
	}

}
