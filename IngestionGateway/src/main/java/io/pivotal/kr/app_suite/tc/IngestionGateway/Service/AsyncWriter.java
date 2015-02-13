package io.pivotal.kr.app_suite.tc.IngestionGateway.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AsyncWriter implements Runnable {
	private final int HDFS_WRITE_RETRIES = 10;
	private String hdfsErrorMsg = "";
	private long bytesWritten = 0;
	
	private BlockingQueue<byte []> queue = new LinkedBlockingQueue<byte []>(50);
	private OutputStream os;
	private boolean isWriteSuccessful = true;
	private volatile boolean doConsumeRest = false;
	private volatile boolean stopConsuming = false;
	
	public AsyncWriter(OutputStream os) {
		this.os = os;
	}
	
	public void consumeRest() {
		this.doConsumeRest = true;
	}
	
	public boolean isWriteSuccessful() {
		return isWriteSuccessful;
	}
	
	public long getBytesWritten() {
		return bytesWritten;
	}
	
	public String getHdfsErrorMsg() {
		return hdfsErrorMsg;
	}
	
	public void add(byte []buf) {
		boolean retry = false;
		do {
			try {
				queue.add(buf);
				retry = false;
			} catch (IllegalStateException fullAlert) {
				retry = true;
			} catch (Exception e) {
				hdfsErrorMsg = e.getMessage();
				stopConsuming = true;
				retry = false;
			}
		} while (retry);
	}
	
	@Override
	public void run() {
		boolean isRetry = false;
		int trials = HDFS_WRITE_RETRIES;
		
		while (stopConsuming == false) {
			byte buf[] = null;
			
			if (isRetry == false) {
				try {
					buf = queue.poll(100, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {}
				
				if (buf == null && doConsumeRest) {
					stopConsuming = true;
					continue;
				}
			}
				
			if (buf == null) {
				continue;
			}
			
			try {
				os.write(buf);
				
				bytesWritten += buf.length;
				isRetry = false;
				trials = HDFS_WRITE_RETRIES;
			} catch (IOException e) {
				hdfsErrorMsg = e.getMessage();
				
				isRetry = true;
				trials--;
				
				if (trials < 0) {
					stopConsuming = true;
				}
			}
		}
		
		try {
			os.flush();
			os.close();
		} catch (IOException e) {}
		
		if (trials < 0) {
			this.isWriteSuccessful = false;
		}
	}
}