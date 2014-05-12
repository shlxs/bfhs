package com.bianfeng.bfas.hive.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Peng Peng
 *
 */
public class SerializeTool {

	private static final Log log = LogFactory.getLog(SerializeTool.class);
	
	public static String defaultEncoding = "iso-8859-1";
	
	/**
	 * deserialize the source with decode|uncompress flag
	 * decode first and then uncompress later 
	 * 
	 * @param source
	 * @param decode
	 * @param uncompress
	 * @return
	 */
	public static String serialize(Object source, boolean encode, boolean compress) {
		byte[] data = serialize(source);
		
		if(compress) {
			data = ZipUtil.compress(data);
		}
		
		if(encode) {
			data = Base64Codec.encode(data);
		}
		
		String result;
		try {
			result = new String(data, defaultEncoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			result = null;
		}
		
		return result;
	}
	
	public static String serializeAsString(Object obj){
		return obj == null ? null : new String(serialize(obj));
	}
	
	public static byte[] serialize(Object obj){
		byte[] result;
		
		if(obj == null) {
			result = null;
		} else {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = null;
			try {
				oos = new ObjectOutputStream(baos);
				oos.writeObject(obj);
				
			} catch (IOException e) {
				log.error("error while open byte array output stream.", e);
			} finally {
				if(oos != null) {
					try {
						oos.close();
					} catch (IOException e) {
						log.error("error while close byte array output stream.", e);
					}
				}
			}
			
			result = baos.toByteArray();
		}
		
		return result;
	}
	
	/**
	 * deserialize the source with decode|uncompress flag
	 * decode first and then uncompress later 
	 * 
	 * @param source
	 * @param decode
	 * @param uncompress
	 * @return
	 */
	public static Object deSerialize(String source, boolean decode, boolean uncompress) {
		byte[] data = null;
		try {
			data = source.getBytes(defaultEncoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			data = source.getBytes();
		}
		if(decode) {
			data = Base64Codec.decode(data);
		}
		
		if(uncompress) {
			data = ZipUtil.decompress(data);
		}
		
		return deSerialize(data);
	}
	
	public static Object deSerializeFromString(String source) {
		return deSerialize(source.getBytes());
	}
	
	public static Object deSerialize(byte[] source) {
		
		Object result;
		
		if(CommonTool.isEmpty(source)) {
			result = null;
		} else {
			ByteArrayInputStream bais = new ByteArrayInputStream(source);
			try {
				ObjectInputStream ois = new ObjectInputStream(bais);
				result = ois.readObject();
				ois.close();
			} catch (Exception e) {
				log.error("error while open byte array input stream.", e);
				result = null;
			}
		}
		
		return result;
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		Map<String, Object> dataMap = new HashMap<String, Object>();
		StringBuilder sb = new StringBuilder();
		String key, val;
		for(int i = 0; i < 400000; i++) {
			key = sb.append("key").append(i).toString();
			sb.setLength(0);
			val = sb.append("val").append(i).toString();
			sb.setLength(0);
			dataMap.put(key, val);
		}
		long t1 = System.nanoTime();
		String src = serialize(dataMap, true, true);
		System.err.println("serialize time(nano): " + (System.nanoTime() - t1));
		
		System.out.println(src.length() * 16 / 1024 / 1024 + " kb");
		
		long t2 = System.nanoTime();
		dataMap = CommonTool.uncheckedMapCast(deSerialize(src, true, true));
		
		System.err.println("deserialize time(nano): " + (System.nanoTime() - t2));
		System.out.println(dataMap.size());
	}
}
