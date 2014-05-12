package com.bianfeng.bfas.hive.jms;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * JMSStorageHandler provides a HiveStorageHandler implementation for JMS
 * 
 * @author ShaoHongLiang
 * @date 2014-1-8
 */
public class JMSStorageHandler extends DefaultStorageHandler {

	final static public String DEFAULT_PREFIX = "default.";

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return JMSInputFormat.class;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return JMSOutputFormat.class;
	}

	@Override
	public Class<? extends SerDe> getSerDeClass() {
		return JMSSerDe.class;
	}

	@Override
	public void configureInputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		// Input
		configureTableJobProperties(tableDesc, jobProperties);
	}

	@Override
	public void configureOutputJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		// Output
		configureTableJobProperties(tableDesc, jobProperties);
	}

	@Override
	public void configureTableJobProperties(TableDesc tableDesc,
			Map<String, String> jobProperties) {
		Properties tableProperties = tableDesc.getProperties();

		String tableName = tableProperties.getProperty(JMSSerDe.TABLE_NAME);
		if (tableName == null) {
			tableName = tableProperties
					.getProperty(hive_metastoreConstants.META_TABLE_NAME);
			tableName = tableName.toLowerCase();
			if (tableName.startsWith(DEFAULT_PREFIX)) {
				tableName = tableName.substring(DEFAULT_PREFIX.length());
			}
		}
		jobProperties.put(JMSSerDe.TABLE_NAME, tableName);

		// get borker url
		Configuration jobConf = getConf();
		jobConf.set(JMSSerDe.JOB_IDENTIFIER, UUID.randomUUID().toString());
		addJMSResources(jobConf, jobProperties);

	}

	private void addJMSResources(Configuration jobConf,
			Map<String, String> newJobProperties) {
		jobConf.addResource("jms-site.xml");
		for (Entry<String, String> entry : jobConf) {
			if (jobConf.get(entry.getKey()) == null) {
				newJobProperties.put(entry.getKey(), entry.getValue());
			}
		}
	}
}
