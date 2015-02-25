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
		
		if (args.length != 2) {
			System.err.println("Correct Usage: <file_path> <http_path_to_upload_to>");
			
			System.exit(1);
		}
		
		// ex> /Users/jungm3/Documents/Dev/Data/data
		File file = new File(args[0]);
		
		if (file.exists() == false) {
			System.err.println(args[0] + " seems not to exist !!");
			
			System.exit(2);
		}
		
		InputStream is = new FileInputStream(file);
		
		long start = System.currentTimeMillis();
		
		// ex> "http://localhost:8080/IngestionGateway/ingest/hdfs/memory"
		Future<Response> post = client.preparePost(args[1])
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
