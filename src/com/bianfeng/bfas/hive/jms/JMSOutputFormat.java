package com.bianfeng.bfas.hive.jms;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

public class JMSOutputFormat implements HiveOutputFormat<Text, MapWritable> {

	@Override
	public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
		verifyConfiguration(jobConf);
	}

	private void verifyConfiguration(JobConf jobConf) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public RecordWriter<Text, MapWritable> getRecordWriter(
			FileSystem fileSystem, JobConf jobConf, String name,
			Progressable progressable) throws IOException {
		throw new UnsupportedOperationException("Error: Hive should not invoke this method.");
	}

	@Override
	public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf,
			Path finalOutPath, Class<? extends Writable> valueClass,
			boolean isCompressed, Properties properties,
			Progressable progressable) throws IOException {
		return new JMSRecordWriter(jobConf, properties, progressable);
	}
}
