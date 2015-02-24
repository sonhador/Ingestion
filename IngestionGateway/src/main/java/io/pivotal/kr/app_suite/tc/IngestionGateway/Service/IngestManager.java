package io.pivotal.kr.app_suite.tc.IngestionGateway.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

@Service
public class IngestManager {
	private final int ASYNC_WRITERS = 5;
	
	private Map<String, List<AsyncWriter>> queue;
	private Map<String, AtomicInteger> queueCounter;
	
	private Configuration hdfsConf;
	private FileSystem hdfsFs;
	
	private Random rand;
	
	@PostConstruct
	public void init() throws IOException {
		queue = new ConcurrentHashMap<String, List<AsyncWriter>>();
		queueCounter = new ConcurrentHashMap<String, AtomicInteger>();
		
		hdfsConf = new Configuration();
		hdfsConf.addResource("phd/hdfs-site.xml");
		hdfsConf.addResource("phd/core-site.xml");
		
		hdfsFs = FileSystem.get(hdfsConf);
		
		rand = new Random();
	}
	
	public int getHdfsBlockSize() {
		return hdfsConf.getInt("dfs.blocksize", 134217728);
	}
	
	public AsyncWriter getWriter(String dir) {
		List<AsyncWriter> writers = queue.get(dir);
		
		if (writers == null) {
			writers = createWriters(dir);
		}
		
		AsyncWriter writer = null;
		
		for (int i=0; i<ASYNC_WRITERS; i++) {
			writer = writers.get(getNextQueue(dir));
			
			if (writer.isOccupied() == false) {
				break;
			} else {
				writer = null;
			}
		}
		
		return writer;
	}
	
	public OutputStream createFileOutputStream(String dir, String date, int idx) throws IOException {
		String filename = InetAddress.getLocalHost().getHostName() + "_" + idx + "_" + getDateTime() + "_" + rand.nextInt(1000000) % 1000;
		
		Path file = new Path(dir+"/"+date+"/"+filename);
		
		return hdfsFs.create(file);
	}
	
	public void createDir(String dir, String date) throws IOException {
		Path dirPath = new Path(dir+"/"+date);
		hdfsFs.mkdirs(dirPath);
	}
	
	public String getDate() {
		return new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
	}
	
	public String getDateTime() {
		return new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());
	}
	
	private synchronized List<AsyncWriter> createWriters(String dir) {
		if (queue.containsKey(dir)) {
			return queue.get(dir);
		}
		
		queue.put(dir, new ArrayList<AsyncWriter>());
		queueCounter.put(dir, new AtomicInteger(0));
		
		try {
			String date = getDate();
			
			createDir(dir, date);
			
			for (int queueIdx = 0; queueIdx < ASYNC_WRITERS; queueIdx++) {
				queue.get(dir).add(new AsyncWriter(this, dir, queueIdx));
				
			}
		} catch (IOException e) {
			Logger.getLogger(this.getClass()).error(e.getMessage());
			return null;
		}
		
		return queue.get(dir);
	}

	private int getNextQueue(String dir) {
		AtomicInteger counter = queueCounter.get(dir);
		
		int idx = counter.get() >= ASYNC_WRITERS ? 0 : counter.get();
		
		if (idx >= ASYNC_WRITERS - 1) {
			counter.set(0);
		} else {
			counter.incrementAndGet();
		}
		
		return idx;
	}
}
