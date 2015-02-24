package io.pivotal.kr.app_suite.tc.IngestionGateway.Controller;

import io.pivotal.kr.app_suite.tc.IngestionGateway.Service.AsyncWriter;
import io.pivotal.kr.app_suite.tc.IngestionGateway.Service.IngestManager;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/ingest")
public class Ingest {
	private static final int BUF_SIZE = 1048576;
	
	@Autowired
	private IngestManager mgr;
	
	@PostConstruct
	private void init() {
		System.setProperty("HADOOP_USER_NAME", "gpadmin");
	}
	
	@RequestMapping(value="/hdfs/{dir}", method=RequestMethod.POST)
	public String ingestToHDFS(final HttpServletRequest req, 
							   final HttpServletResponse res,
							   final @PathVariable String dir,
							   final Model model) {
		try {
			InputStream is = req.getInputStream();
			
			byte buf[] = new byte[BUF_SIZE];
			
			AsyncWriter writer = mgr.getWriter(dir);
			
			if (writer == null) {
				res.setStatus(HttpServletResponse.SC_BAD_GATEWAY);
				model.addAttribute("status", "Server Capacity Full !!");
				model.addAttribute("received", 0);
				model.addAttribute("written", 0);
			}
			
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
		}
	}
}
