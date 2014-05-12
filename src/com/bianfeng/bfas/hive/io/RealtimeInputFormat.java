package com.bianfeng.bfas.hive.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

public class RealtimeInputFormat extends TextInputFormat {

	public static final Log LOG = LogFactory.getLog(RealtimeInputFormat.class);

	@Override
	protected FileStatus[] listStatus(JobConf job) throws IOException {
		FileStatus[] fileStatusArr = super.listStatus(job);

		if (fileStatusArr == null){
			LOG.warn("file status is null");
			return null;
		}

		int length = fileStatusArr.length;
		LOG.warn("file status size:" + length);
		
		FileStatus[] realtimeFileStatusArr = new FileStatus[length];
		for (int i = 0; i < length; i++) {
			FileStatus fileStatus = fileStatusArr[i];
			if (fileStatus.isFile()) {
				Path path = fileStatus.getPath();
				FileSystem fs = path.getFileSystem(job);
				FSDataInputStream in = fs.open(path);

				if (in.getWrappedStream() instanceof DFSInputStream) {

					LOG.warn("transfer file status:" + ((DFSInputStream) in.getWrappedStream()).getFileLength());
					FileStatus realtimeSizeFileStatus = new FileStatus(
							((DFSInputStream) in.getWrappedStream()).getFileLength(),
							fileStatus.isDirectory(), 
							fileStatus.getReplication(), 
							fileStatus.getBlockSize(), 
							fileStatus.getModificationTime(), 
							fileStatus.getAccessTime(), 
							fileStatus.getPermission(), 
							fileStatus.getOwner(),
							fileStatus.getGroup(), 
							fileStatus.getSymlink(),
							fileStatus.getPath());

					realtimeFileStatusArr[i] = realtimeSizeFileStatus;
				} else {
					realtimeFileStatusArr[i] = fileStatus;
				}

			} else {
				LOG.warn("use old file status");
				realtimeFileStatusArr[i] = fileStatus;
			}
		}
		return realtimeFileStatusArr;
	}
}