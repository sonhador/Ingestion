package io.pivotal.kr.app_suite.tc.IngestionGateway.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.springframework.stereotype.Service;

@Service
public class EgestManager {
	private Configuration hdfsConf;
	private FileSystem hdfsFs;
	
	private Pattern yyyyMMdd = Pattern.compile("^(\\d{8})");
	
	@PostConstruct
	public void init() throws IOException {
		hdfsConf = new Configuration();
		hdfsConf.addResource("phd/hdfs-site.xml");
		hdfsConf.addResource("phd/core-site.xml");
		
		hdfsFs = FileSystem.get(hdfsConf);
	}
	
	private long getTotalFileSize(Path dirPath, String dateTimePattern) throws IOException {
		RemoteIterator<LocatedFileStatus> files = hdfsFs.listFiles(dirPath, false);
		
		long size = 0;
		while (files.hasNext()) {
			Path file = files.next().getPath();
			
			if (file.getName().contains(dateTimePattern)) {	
				size += hdfsFs.getFileStatus(file).getLen();
			}
		}
		
		return size;
	}
	
	public void read(HttpServletResponse res, String dir, String dateTimePattern) throws IOException {
		Matcher dateMatcher = yyyyMMdd.matcher(dateTimePattern);
		String date = null;
		if (dateMatcher.find()) {
			date = dateMatcher.group(1);
		}
		
		if (date == null) {
			res.sendError(HttpServletResponse.SC_BAD_REQUEST, "date(yyyyMMdd) not specified !!");
		}
		
		Path dirPath = new Path(dir + "/" + date);
		
		res.setContentType("application/octet-stream");
		res.setHeader("Content-Length", getTotalFileSize(dirPath, dateTimePattern) + "");
		res.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", dir + dateTimePattern));
		
		OutputStream os = res.getOutputStream();
		RemoteIterator<LocatedFileStatus> files = hdfsFs.listFiles(dirPath, false);

		try {
			while (files.hasNext()) {
				LocatedFileStatus file = files.next();
				
				if (file.getPath().getName().contains(dateTimePattern) == false) {	
					continue;
				}
				
				InputStream is = hdfsFs.open(file.getPath());
				AsyncReader reader = new AsyncReader(is);
				
				Thread thread = new Thread(reader);
				thread.start();
	
				boolean stopConsuming = false;
				
				while (stopConsuming == false) {
					byte buf[] = null;					

					try {
						buf = reader.queue.poll(100, TimeUnit.MILLISECONDS);
					} catch (InterruptedException e) {}
					
					if (buf == null && reader.isReadSuccessful() == false) {
						break;
					}
					
					if (buf == null && reader.doConsumeRest()) {
						stopConsuming = true;
						continue;
					}
					
					if (buf == null) {
						continue;
					}
					
					os.write(buf);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			os.flush();
			os.close();
		}
	}
}
