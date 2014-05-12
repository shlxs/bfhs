package com.bianfeng.bfas.hive.jms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

public class JMSSerDe implements SerDe {
	//no setting
	public static final String TABLE_NAME = "name";
	public static final String JOB_IDENTIFIER = "job.identifier";
	//must setting
	public static final String JMS_BORKER_URL = "borker.url";
	//could setting
	public static final String MESSAGE_CHANNEL = "message.channel";
	public static final String MESSAGE_CONVETER = "message.conveter";
	public static final String MESSAGE_BUFFER_SIZE = "message.buffer.size";
	public static final String TRANSACTION_ENABLE = "transaction.enable";
	
	public static final String TABLE_NAME_DEFAULT_SCHEMA = "default.";
	public static final int TABLE_NAME_DEFAULT_SCHEMA_LENGTH = TABLE_NAME_DEFAULT_SCHEMA.length();

	private StructObjectInspector objectInspector;
	private ObjectWritable objectWritable = new ObjectWritable();
	private static final String CONF_COLUMNS = "columns";

	@Override
	public void initialize(Configuration configuration, Properties properties)
			throws SerDeException {
		if (configuration == null) {
			configuration = new Configuration();
		}
		Enumeration<?> propertyNames = properties.propertyNames();
		while (propertyNames.hasMoreElements()) {
			String propertyName = (String) propertyNames.nextElement();
			configuration.set(propertyName,
					properties.getProperty(propertyName));
		}

		List<String> columns = Arrays.asList(configuration
				.get(CONF_COLUMNS, "").split(","));
		List<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>(
				columns.size());
		for (int i = 0; i < columns.size(); i++) {
			objectInspectors
					.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		}

		objectInspector = ObjectInspectorFactory
				.getStandardStructObjectInspector(columns, objectInspectors);
	}

	@Override
	public Object deserialize(Writable blob) throws SerDeException {
		// no support for jms
		return null;
	}

	@Override
	public ObjectInspector getObjectInspector() throws SerDeException {
		return objectInspector;
	}

	@Override
	public SerDeStats getSerDeStats() {
		// no support for jms
		return null;
	}

	@Override
	public Class<? extends Writable> getSerializedClass() {
		return ObjectWritable.class;
	}

	@Override
	public Writable serialize(Object obj, ObjectInspector objInspector)
			throws SerDeException {
		if (objInspector.getCategory() != Category.STRUCT) {
			throw new SerDeException(getClass().toString()
					+ " can only serialize struct types, but we got: "
					+ objInspector.getTypeName());
		}

		// Prepare the field ObjectInspectors
		StructObjectInspector loi = (StructObjectInspector) objInspector;
		List<Object> rawData = (List<Object>) loi.getStructFieldsDataAsList(obj);
		
		List<String> result = new ArrayList<String>();
		for(Object data : rawData){
			if(data == null){
				result.add(null);
			}else if(data instanceof Date){
				result.add(String.valueOf(((Date)data).getTime()));
			}else{
				result.add(String.valueOf(data));
			}
		}
		objectWritable.set(result);
		return objectWritable;
	}
}