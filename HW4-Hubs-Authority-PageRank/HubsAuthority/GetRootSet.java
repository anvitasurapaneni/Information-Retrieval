package HitsCrawl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class GetRootSet {

	public static void main(String a[]) throws FileNotFoundException, UnknownHostException, InterruptedException, ExecutionException, UnsupportedEncodingException
	{
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","anv").build();

	    Client client = TransportClient.builder().settings(settings).build()
	    		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
	    
	    PrintWriter writer = new PrintWriter("A4_RootSet.txt", "UTF-8");
		HashSet<String> BaseSet = new 	HashSet<String>();
		
		
		 File f11 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/A4_BasicSetFromOkapi.txt");
		 BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(f11)));
		 String line1;
		 
		
			try {
				while( (line1 = br1.readLine()) != null){
					BaseSet.add(line1.trim().toLowerCase());
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			System.out.println(BaseSet.size());
	 File f1 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/A4_BasicSetFromOkapi.txt");
	 BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f1)));
	 String line;
	 
	 

	try {
		while( (line = br.readLine()) != null){
			
			GetResponse response = client.prepareGet("four", "document",(String) line.trim())
					.setFields("in_links","out_links","author","depth").execute().actionGet(); 
			
			if(response.isExists()){
				//System.out.println("response exists");
			 				//System.out.println(line);
			 				String inlinks2 = (String)response.getField("in_links").getValue();
			 				String outlinks2 = (String)response.getField("out_links").getValue();
			 				String[] inlinks = inlinks2.split("\t");
			 				String[] outlinks = outlinks2.split("\t");
			 				for(int i=0; i< outlinks.length; i++ ){
			 					BaseSet.add(outlinks[i].toLowerCase());
			 				}
			 				for(int i=0; i< inlinks.length && i< 5000; i++ ){
			 					BaseSet.add(inlinks[i].toLowerCase());
			 				}
			}
		}
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
System.out.println(BaseSet.size());

for(String str: BaseSet){
	writer.println(str);
}

writer.close();
	}
}
