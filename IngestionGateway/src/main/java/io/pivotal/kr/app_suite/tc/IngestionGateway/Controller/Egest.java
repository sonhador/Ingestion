package io.pivotal.kr.app_suite.tc.IngestionGateway.Controller;

import io.pivotal.kr.app_suite.tc.IngestionGateway.Service.EgestManager;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
@RequestMapping("/egest")
public class Egest {
	
	@Autowired
	private EgestManager mgr;

	@PostConstruct
	private void init() {
		System.setProperty("HADOOP_USER_NAME", "gpadmin");
	}
	
	@RequestMapping(value="/hdfs/{dir}/{dateTimePattern}", method=RequestMethod.GET)
	public void ingestToHDFS(final HttpServletRequest req, 
							 final HttpServletResponse res,
							 final @PathVariable("dir") String dir,
							 final @PathVariable("dateTimePattern") String dateTimePattern) {
		
		try {
			mgr.read(res, dir, dateTimePattern);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
