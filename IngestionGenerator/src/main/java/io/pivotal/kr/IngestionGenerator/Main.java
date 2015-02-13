package io.pivotal.kr.IngestionGenerator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;

public class Main {
	public static void main(String []args) throws InterruptedException, ExecutionException, IOException {
		AsyncHttpClient client = new AsyncHttpClient();
		
		InputStream is = new FileInputStream(new File("/Users/jungm3/Documents/Dev/Data/data"));
		
		Future<Response> post = client.preparePost("http://localhost:8080/IngestionGateway/ingest/hdfs/memory")
									  .setRequestTimeout(-1)
									  .setBody(is)
									  .execute();
		
		Response r = post.get();
		
		System.out.println(r.getResponseBody());
		
		client.close();
	}
}
