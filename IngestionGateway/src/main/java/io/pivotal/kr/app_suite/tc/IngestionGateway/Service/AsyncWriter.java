package io.pivotal.kr.app_suite.tc.IngestionGateway.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class AsyncWriter implements Runnable {
	private static final int HDFS_WRITE_RETRIES = 10;
	private static final int ASYNC_WRITER_CAPACITY = 20;
	private String hdfsErrorMsg = "";
	private long totalBytesWritten = 0;
	private long bytesWritten = 0;
	
	private BlockingQueue<byte []> queue = new LinkedBlockingQueue<byte []>(ASYNC_WRITER_CAPACITY);
	private boolean isWriteSuccessful = true;
	private volatile boolean doConsumeRest = false;
	private volatile boolean stopConsuming = false;
	
	private IngestManager mgr;
	private String dir;
	private int idx;
	
	private boolean occupied = false;
	
	public AsyncWriter(IngestManager mgr, String dir, int idx) {
		this.mgr = mgr;
		this.dir = dir;
		this.idx = idx;
	}
	
	public boolean isOccupied() {
		return occupied;
	}
	
	private OutputStream renewFileOutputStream(OutputStream os) throws IOException {
		String date = mgr.getDate();
		
		mgr.createDir(dir, date);
		
		os.flush();
		os.close();
		
		return mgr.createFileOutputStream(dir, date, idx);
	}
	
	public void consumeRest() {
		this.doConsumeRest = true;
	}
	
	public boolean isWriteSuccessful() {
		return isWriteSuccessful;
	}
	
	public long getBytesWritten() {
		return totalBytesWritten;
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
		occupied = true;
		
		OutputStream os = null;
		try {
			os = mgr.createFileOutputStream(dir, mgr.getDate(), idx);
		
			boolean isRetry = false;
			int trials = HDFS_WRITE_RETRIES;
			
			byte bufBeforeNewLine[] = null;
			byte bufAfterNewLine[] = null;
			
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

				if (bytesWritten >= mgr.getHdfsBlockSize()) {
					int untilNextLine = buf.length;
					for (untilNextLine -= 1; untilNextLine >= 0; untilNextLine--) {
						if ((char)buf[untilNextLine] == '\n') {
							break;
						}
					}
					
					if (untilNextLine >= 0) {
						untilNextLine += 1; // just after new-line
						
						bufAfterNewLine = new byte[buf.length - untilNextLine];
						bufBeforeNewLine = new byte[untilNextLine];
						
						System.arraycopy(buf, 0, bufBeforeNewLine, 0, untilNextLine);
						System.arraycopy(buf, untilNextLine, bufAfterNewLine, 0, buf.length - untilNextLine);
						
						buf = bufBeforeNewLine;
					}
				}
				
				try {
					if (bufBeforeNewLine == null && bufAfterNewLine != null) {
						os.write(bufAfterNewLine);
						
						totalBytesWritten += bufAfterNewLine.length;
						bytesWritten += bufAfterNewLine.length;

						bufAfterNewLine = null;
					}
					
					os.write(buf);
					
					totalBytesWritten += buf.length;
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
				
				if (bufBeforeNewLine != null) {
					os = renewFileOutputStream(os);
					
					bytesWritten = 0;
					bufBeforeNewLine = null;
				}
			}
			
			try {
				os.flush();
				os.close();
			} catch (IOException e) {}
			
			if (trials < 0) {
				isWriteSuccessful = false;
			}
		} catch (IOException e) {
			Logger.getLogger(this.getClass()).error(e.getMessage());
			
			isWriteSuccessful = false;
		} finally {
			occupied = false;
		}
	}
}