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
		
		File file = new File("/Users/jungm3/Documents/Dev/Data/data");
		
		InputStream is = new FileInputStream(file);
		
		long start = System.currentTimeMillis();
		
		Future<Response> post = client.preparePost("http://localhost:8080/IngestionGateway/ingest/hdfs/memory")
									  .setRequestTimeout(-1)
									  .setBody(is)
									  .execute();
		
		Response r = post.get();
		
		long end = System.currentTimeMillis();
		
		System.out.println(r.getResponseBody());
		System.out.println("Took: " + (end - start)/1000 + " sec, Throughput: " + file.length() / ((end - start) / 1000) /1024/1024 + " MB/sec");
		
		client.close();
		is.close();
	}
}
