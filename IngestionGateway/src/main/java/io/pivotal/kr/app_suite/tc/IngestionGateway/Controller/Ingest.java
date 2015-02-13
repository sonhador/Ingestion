package io.pivotal.kr.app_suite.tc.IngestionGateway.Controller;

import io.pivotal.kr.app_suite.tc.IngestionGateway.Service.AsyncWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/ingest")
public class Ingest {
	@PostConstruct
	private void init() {
		System.setProperty("HADOOP_USER_NAME", "gpadmin");
	}
	
	@RequestMapping(value="/hdfs/{dir}", method=RequestMethod.POST)
	public String ingestToHDFS(final HttpServletRequest req, 
										 final HttpServletResponse res,
										 final @PathVariable String dir,
										 final Model model) {
		FileSystem fs = null;
		
		try {
			Configuration conf = new Configuration();
			conf.addResource("phd/hdfs-site.xml");
			conf.addResource("phd/core-site.xml");
			
			fs = FileSystem.get(conf);
			
			String date = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
			
			Path dirPath = new Path(dir+"/"+date);
			
			String errDir = "";
			try {
				fs.mkdirs(dirPath);
			} catch (IOException e) {
				errDir = e.getMessage();
			}
			
			if (fs.exists(dirPath) == false) {
				res.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
				
				model.addAttribute("status", "Failed!! : Could not create hdfs: " + dirPath.getName() + ": " + errDir);
				model.addAttribute("bytes", "0");
				
				return "ingestResult";
			}
			
			String filename = InetAddress.getLocalHost().getHostName() + "_" + 
							  Thread.currentThread().getId() + "_" +
							  UUID.randomUUID().toString();
			
			Path file = new Path(dir+"/"+date+"/"+filename);
			
			OutputStream os = null;
			String errFile = "";
			try {
				os = fs.create(file);
			} catch (IOException e) {
				errFile = e.getMessage();
			}
			
			if (os == null || fs.exists(file) == false) {
				res.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
				
				model.addAttribute("status", "Failed!! : Could not create hdfs: " + file.getName() + ": " + errFile);
				model.addAttribute("bytes", "0");
				
				return "ingestResult";
			}
			
			InputStream is = req.getInputStream();
			
			byte buf[] = new byte[1048576];
			
			AsyncWriter writer = new AsyncWriter(os);
			
			Thread writerThread = new Thread(writer);
			writerThread.start();
			
			int read;
			long totalReads = 0;
			while ((read = is.read(buf)) != -1) {
				totalReads += read;
				
				byte bufExact[] = new byte[read];
				System.arraycopy(buf, 0, bufExact, 0, read);
				writer.add(bufExact);
			}
			
			writer.consumeRest();
			try {
				writerThread.join();
			} catch (InterruptedException e) {}
			
			if (writer.isWriteSuccessful()) {
				res.setStatus(HttpServletResponse.SC_OK);
				model.addAttribute("status", "Complete!! " + writer.getHdfsErrorMsg());
				model.addAttribute("received", totalReads);
				model.addAttribute("written", writer.getBytesWritten());
			} else {
				res.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
				model.addAttribute("status", "Incomplete!! " + writer.getHdfsErrorMsg());
				model.addAttribute("received", totalReads);
				model.addAttribute("written", writer.getBytesWritten());
			}
			
			return "ingestResult";
		} catch (IOException e) {
			res.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
			
			model.addAttribute("status", "Failed!! " + e.getMessage());
			model.addAttribute("bytes", 0);
			
			return "ingestResult";
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {}
			}
		}
	}
}
