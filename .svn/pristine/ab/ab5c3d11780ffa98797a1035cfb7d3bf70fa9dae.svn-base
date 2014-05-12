package com.bianfeng.bfas.hive.jms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import com.bianfeng.bfas.hive.jms.converter.DefaultMessageConveter;
import com.bianfeng.bfas.hive.util.Base64Codec;
import com.bianfeng.bfas.hive.util.SerializeTool;
import com.bianfeng.bfas.hive.util.ZipUtil;

public class JMSCacheQueue{
	private List<List<String>> rows = new ArrayList<List<String>>();

	private ConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;
	private Map<String, MessageProducer> channelProducerMap;
	private String brokerUrl;
	private String tableChannelName;
	//TODO wait hive support partition in storagehandler interface
	//private long baseTime;
	private int bufferSize;
	private MessageConverter converter;

	public JMSCacheQueue(JobConf jobConf, Properties tableProperties) {
		try {
			channelProducerMap = new HashMap<String, MessageProducer>();
			
			brokerUrl = jobConf.get(JMSSerDe.JMS_BORKER_URL);
			
			if(brokerUrl == null){
				throw new RuntimeException("jms.borker.url must be set at jms.xml in hive conf floder");
			}
			
			//channel name
			tableChannelName = tableProperties.getProperty(JMSSerDe.MESSAGE_CHANNEL);
			
			String converterClass = tableProperties.getProperty(JMSSerDe.MESSAGE_CONVETER);
			if(converterClass != null){
				try {
					converter = (MessageConverter)Class.forName(converterClass).newInstance();
				} catch (Exception e) {
					throw new RuntimeException("create " + converterClass + " error", e);
				}
			}else{
				converter = new DefaultMessageConveter();
			}
			converter.configure(jobConf, tableProperties);
			
			bufferSize = converter.getBufferSize();
			
			connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
			connection = connectionFactory.createConnection();
			
			//transaction enable
			String TRANSACTION_ENABLE = tableProperties.getProperty(JMSSerDe.TRANSACTION_ENABLE);
			boolean txEnable = TRANSACTION_ENABLE == null ? false : Boolean.valueOf(TRANSACTION_ENABLE);
			session = connection.createSession(txEnable, Session.AUTO_ACKNOWLEDGE);
		} catch (JMSException e) {
			throw new RuntimeException("create DefaultJMSCacheQueue error", e);
		}
		
	}

	@SuppressWarnings("unchecked")
	public void write(Writable writable) {
		Object obj = ((ObjectWritable) writable).get();
		rows.add((List<String>) obj);
		if (rows.size() >= bufferSize) {
			flush();
			System.out.println("flush because rows size >= " + bufferSize);
		}
	}

	public void commit() {
		System.out.println("flush because table commit");
		flush();
		close(true);
	}
	
	public void rollback() {
		close(false);
	}

	private synchronized void flush() {
		try {
			if(rows.size() > 0){
				Serializable message = converter.process(rows);
				
				sendMessage(converter.getChannelName(), message);
				
				// clear rows
				deepClear(rows);
			}
		} catch (JMSException e) {
			throw new RuntimeException("flush MultiRecords error", e);
		}
	}

	
	private void close(boolean isCommit) {
		
		if (channelProducerMap.size() > 0) {
			try {
				for(String channelName : channelProducerMap.keySet()){
					MessageProducer producer = channelProducerMap.get(channelName);
					producer.close();
				}
			} catch (JMSException e) {
				throw new RuntimeException("close producer error", e);
			}
		}

		if (session != null) {
			try {
				if(session.getTransacted()){
					if(isCommit) {
						session.commit();
					} else {
						session.rollback();
					}
				}
				session.close();
			} catch (JMSException e) {
				throw new RuntimeException("close session error", e);
			}
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (JMSException e) {
				throw new RuntimeException("close connection error", e);
			}
		}
	}
	
	private static final int COMPRESS_FLAG = 1;
	private static final int UNCOMPRESS_FLAG = 0;
	private int dataCompressSize = 1024 * 10; // 10kb 
	private boolean sendMessage(String channelName, Serializable msg) throws JMSException {
		
		boolean result = false;
		StringBuilder text = new StringBuilder();
		byte[] bytes = SerializeTool.serialize(msg);
		
		if(bytes.length > dataCompressSize) {
			bytes = ZipUtil.compress(bytes);
			text.append(COMPRESS_FLAG);
		} else {
			text.append(UNCOMPRESS_FLAG);
		}
		
		bytes = Base64Codec.encode(bytes);
		text.append(new String(bytes));

		// send to jms queue
		TextMessage message = session.createTextMessage(text.toString());
		
		if(tableChannelName != null){
			channelName = tableChannelName;
		}
		
		MessageProducer producer = channelProducerMap.get(channelName);
		if(producer == null){
			producer = session.createProducer(session.createQueue(channelName));
			channelProducerMap.put(channelName, producer);
		}
		producer.send(message);
		
		return result;
	}
	
	@SuppressWarnings("rawtypes")
	public static void deepClear(Object obj) {
		if(obj instanceof Map){
			Map mapObj = (Map)obj;
			for(Object key : mapObj.keySet()) {
				deepClear(key);
				deepClear(mapObj.get(key));
			}
			
			try{
				mapObj.clear();
			}catch(Exception e){
				//skip it
			}
			
			mapObj = null;
		}else if(obj instanceof Collection) {
			Collection colObj = (Collection)obj;
			for(Object val : colObj){
				deepClear(val);
			}
			
			try{
				colObj.clear();
			}catch(Exception e){
				//skip it
			}
			
			colObj = null;
		} else if(obj instanceof Object[]) {
			Object[] arr = (Object[])obj;
			
			for(Object val : arr){
				deepClear(val);
			}
			
			arr = null;
		}
	}

}
