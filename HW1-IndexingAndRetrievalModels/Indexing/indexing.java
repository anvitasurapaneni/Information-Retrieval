package Anvita.CS6200;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.File;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;


import org.apache.commons.io.FileUtils;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class indexing {
	public static void main(String[] args) throws IOException
	
	
	{
		

		File IPfolder = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/ap89_collection");
		File[] listOfFiles = IPfolder.listFiles();
		Map<String,String> hm=new HashMap<String,String>(); 
		
		
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","elasticsearch").build();
	
        Client client = TransportClient.builder().settings(settings).build()
        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
        
        BulkRequestBuilder br=client.prepareBulk(); 
        int count =0;
        
		for (int i = 0; i < listOfFiles.length; i++) {
			
			
		
			
			File mFile = new File(listOfFiles[i].getPath());
			 String str = FileUtils.readFileToString(mFile);
			 //  Extract DOC
			  Pattern pattern = Pattern.compile("<DOC>\\s(.+?)</DOC>", Pattern.DOTALL);
			  Matcher matcher = pattern.matcher(str);
			
			 while (matcher.find())
	
			 {
				 count++; 
				 // Save DOC
			 String docTemp = matcher.group(1);
			 
			 // Extract DOCNO
			 
			 final Pattern pattern1 = Pattern.compile("<DOCNO>(.+?)</DOCNO>");
			 final Matcher matcher1 = pattern1.matcher(docTemp);
			 matcher1.find();
			 String docNoTemp1 = matcher1.group(1);
			 String docNoTemp = docNoTemp1.trim();
			// System.out.println(docNoTemp);
			 
			 // Extract TEXT
			 
		
			  Pattern pattern2 = Pattern.compile("<TEXT>\\s(.+?)</TEXT>", Pattern.DOTALL);
			  Matcher matcher2 = pattern2.matcher(docTemp);
			  
			  String textTemp ="";
			
			 while (matcher2.find())
			 {
				// System.out.println("TEXT");
				 textTemp = textTemp.concat(matcher2.group(1));
				// textTemp = matcher2.group(1);
				// textTemp.concat(matcher2.group(1));
			 }
			// System.out.println(textTemp);
			 
			 // Create Hash Entry
			textTemp = textTemp.replaceAll("\n", " ");
			 
			 hm.put(docNoTemp,textTemp);
			
			 }
			 
	
			     }
		
		

		int i=0;

		for(Map.Entry m:hm.entrySet())
		{  i+=1;
		//	System.out.println(m.getKey()+" "+m.getValue());
		
		  br.add(client.prepareIndex("ap_dataset", "document",(String) m.getKey())
                  .setSource(jsonBuilder()
                          .startObject()
                          .field("text", m.getValue())
                          .field("docno", (String) m.getKey())
                      .endObject()
                          ));
               
                  
                		  
                		  
                		  
                		
			

		
		}
			
		
		 System.out.println(count);
		 System.out.println(i);
		 BulkResponse bres=br.get();
		
		}
	
}
