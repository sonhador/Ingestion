package io.pivotal.kr.app_suite.tc.IngestionGateway.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public class AsyncReader implements Runnable {
	private static final int ASYNC_READER_CAPACITY = 20;
	private static final int BUF_SIZE = 1048576;
	
	private boolean isReadSuccessful = true;
	private boolean doConsumeRest = false;
	private String hdfsErrorMsg = "";
	
	public BlockingQueue<byte []> queue = new LinkedBlockingQueue<byte []>(ASYNC_READER_CAPACITY);
	
	private InputStream is;
	
	public AsyncReader(InputStream is) {
		this.is = is;
	}
	
	public String getHdfsErrorMsg() {
		return hdfsErrorMsg;
	}
	
	public boolean isReadSuccessful() {
		return isReadSuccessful;
	}
	
	public boolean doConsumeRest() {
		return doConsumeRest;
	}
	
	private void add(byte buf[]) throws Exception {
		boolean retry = false;
		do {
			try {
				queue.add(buf);
				retry = false;
			} catch (IllegalStateException fullAlert) {
				retry = true;
			} catch (Exception e) {
				hdfsErrorMsg = e.getMessage();
				retry = false;
				
				throw new Exception(hdfsErrorMsg);
			}
		} while (retry);
	}
	
	@Override
	public void run() {
		try {
			byte buf[] = new byte[BUF_SIZE];
			
			int read;
			while ((read = is.read(buf)) != -1) {
				byte []bufExact = new byte[read];
				
				System.arraycopy(buf, 0, bufExact, 0, read);
				
				try {
					add(bufExact);
				} catch (Exception e) {
					break;
				}
			}
		} catch (IOException e) {
			Logger.getLogger(this.getClass()).error(e.getMessage());
			
			isReadSuccessful = false;
		} finally {
			try {
				is.close();
			} catch (IOException e) {}
			doConsumeRest = true;
		}
	}
}
