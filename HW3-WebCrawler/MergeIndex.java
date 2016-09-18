package Anvita.A3;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class MergeIndex {
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException
	
	
	{
		

	
		
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","souanvmohsan").build();
	
        Client client = TransportClient.builder().settings(settings).build()
        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
        
        BulkRequestBuilder br=client.prepareBulk(); 
       
			
        File catIn = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/InLinkFcat.txt");
		HashMap<String,ArrayList<Integer>> CatalogIn = createCatalog(catIn);
	
		File catOut = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/OutLinkFCat.txt");
		HashMap<String,ArrayList<Integer>> CatalogOut = createCatalog(catOut);
		
		 File catWave = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/LinkWavesFCat.txt");
			HashMap<String,ArrayList<Integer>> CatalogWave = createCatalog(catWave);
			
			File mFile1 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/HTMLSouceCatF.txt");
			HashMap<String, ArrayList<Long>> hmsrc = createCatalog1(mFile1);
			
			File mFile2 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/HTTPHeaderCatF.txt");
			HashMap<String, ArrayList<Long>> hmhdr = createCatalog1(mFile2);
		
			ArrayList<String> AllDocs = new ArrayList<String>();
			int dc = 0;
			File mFile = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/CorpusF.txt");
			// String str = FileUtils.readFileToString(mFile);
			 BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(mFile)));
			 
			 String line;
			 String doc ="";
			 //&& dc < 15 add to while to restrict no odf docs
			 while( (line = br1.readLine()) != null ){
				 doc = doc + line;
				 if(doc.contains("</DOC>")){
					 dc = dc+1;
					 doc = doc + line;
					 
					 
					 final Pattern pattern1 = Pattern.compile("<DOCNO>(.+?)</DOCNO>");
					 final Matcher matcher1 = pattern1.matcher(doc);
					 matcher1.find();
					 String docNoTemp1 = matcher1.group(1);
					 String docNoTemp = docNoTemp1.trim();
					 
					 final Pattern pattern3 = Pattern.compile("<URL>(.+?)</URL>");
					 final Matcher matcher3 = pattern3.matcher(doc);
					 matcher3.find();
					 String url3 = matcher3.group(1);
					 String url = url3.trim();
					 
					 final Pattern pattern4 = Pattern.compile("<HEAD>(.+?)</HEAD>");
					 final Matcher matcher4 = pattern4.matcher(doc);
					 matcher4.find();
					 String head4 = matcher4.group(1);
					 String head = head4.trim();
					 
					 
					 final Pattern pattern5 = Pattern.compile("<AUTHOR>(.+?)</AUTHOR>");
					 final Matcher matcher5 = pattern5.matcher(doc);
					 matcher5.find();
					 String head5 = matcher5.group(1);
					 String auth = head5.trim();
					 
					 Pattern pattern2 = Pattern.compile("<TEXT>(.+?)</TEXT>", Pattern.DOTALL);
					  Matcher matcher2 = pattern2.matcher(doc);
					  
					  String textTemp ="";
					
					 while (matcher2.find())
					 {
						 textTemp = textTemp.concat(matcher2.group(1));
					 
					 // Create Hash Entry
					textTemp = textTemp.replaceAll("\n", " ");
					// AllDocs.add(doc);
					 
				 }
					// AllDocs.add(doc);
					 
					 //add to elasti
					 System.out.println("adding to ec");
					 
					 String key = (String) docNoTemp;
					 System.out.println(key);
					 String inLinks1 = getInlinks(CatalogIn.get(key));
						String outlinks1 = getOutlinks(CatalogOut.get(key));
						String Auth1 = (String)auth;
						int wave = Integer.parseInt(getWave(CatalogWave.get(key))) ;
						System.out.println(hmsrc.get(key));
						String src = getSource(hmsrc.get(key));
						String hdr = getHTTPHeader(hmhdr.get(key));
						
						
						GetResponse response = client.prepareGet("finalmergeanvi", "document",(String) key)
								.setFields("inlinks","outlinks","author","depth").execute().actionGet(); 
						
						if(response.isExists()){
							//System.out.println("response exists");
						 				
						 				String inlinks2 = (String)response.getField("inlinks").getValue();
						 				String outlinks2 = (String)response.getField("outlinks").getValue();
						 				String Auth2 = (String)response.getField("author").getValue();
						 				int D2 = (int) response.getField("depth").getValue();
						 				
						 				if(wave > D2){
						 					wave = D2;
						 				}
						 				
						 			
						 			
						 		
						 			 inLinks1 = MergeLinks(inLinks1, inlinks2);
						 			 outlinks1 = MergeLinks(outlinks1, outlinks2);
						 			Auth1 = MergeLinks(Auth2, Auth1);
						 			// Auth1 = Auth2 +"\t" +Auth1;
									
						 	

						 			}

						else{
							//System.out.println("response not exists");
						}
						
						IndexRequest indexRequest = new IndexRequest("finalmergeanvi", "document",(String)key)
								  .source(jsonBuilder()
			 	                                .startObject()
			 	                               .field("docno", (String) key)
			  		                          .field("url", url)
			  		                          .field("depth", wave)
			  		                          .field("author", Auth1)
			  		                          .field("head", (String) head)
			  		                          .field("inlinks", inLinks1)
			  		                          .field("outlinks", outlinks1)
			  		                          .field("text", textTemp)
			  		                          .field("html-src", src)
			  		                          .field("http-header", hdr)
			 	                                .endObject()
			 	                              );
			 				
			 				UpdateRequest updateRequest = new UpdateRequest("finalmergeanvi", "document",(String) key)
			 				        .doc(jsonBuilder()
			 				            .startObject()
			 				           .field("docno", (String) key)
				                          .field("url", (String) url)
				                          .field("depth", wave)
				                          .field("author", Auth1)
				                          .field("head", (String) head)
				                          .field("inlinks", inLinks1)
				                          .field("outlinks", outlinks1)
				                          
			 				            .endObject())
			 				        .upsert(indexRequest);              
			 				client.update(updateRequest).get();
						
						
					 doc ="";
				 }
			 }
			 
			 
	System.out.println(dc);
		
		}
	
	


private static String  getHTTPHeader(ArrayList<Long> StAndEnd) throws FileNotFoundException {
		
		long start1 = StAndEnd.get(0);
    	long end1 = StAndEnd.get(1);
    	byte[] string = null;
    	File file123 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/HTTPHeaderF.txt");
		RandomAccessFile randFile = new RandomAccessFile(file123, "rw");
		 
		
		string = new byte[(int) (end1 - start1)];
		try {
			randFile.seek(start1);
			randFile.readFully(string);
			randFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String InLinks = new String(string);
		String[] ils = InLinks.split("\t",2);
		int s = ils.length;
		if(s > 1){
			return ils[1];	
		}
		else {
			return "";
		}
		
		
	}


private static String  getSource(ArrayList<Long> StAndEnd) throws FileNotFoundException {
		
		long start1 = StAndEnd.get(0);
    	long end1 = StAndEnd.get(1);
    	byte[] string = null;
    	File file123 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/HTMLSourceF.txt");
		RandomAccessFile randFile = new RandomAccessFile(file123, "rw");
		 
		
		string = new byte[(int) (end1 - start1)];
		try {
			randFile.seek((long) start1);
			randFile.readFully(string);
			randFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String InLinks = new String(string);
		String[] ils = InLinks.split("\t",2);
		int s = ils.length;
		if(s > 1){
			return ils[1];	
		}
		else {
			return "";
		}
		
		
	}
	
	
	private static HashMap<String, ArrayList<Long>> createCatalog1(File FileName) {
		
		//File mFile = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/HTMLSouceCatF.txt");
		
		 HashMap<String,ArrayList<Long>> Offset = new HashMap<String,ArrayList<Long>>();
			
		
				BufferedReader br = null;
				try {
					br = new BufferedReader(new InputStreamReader(new FileInputStream(FileName)));
				} catch (FileNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				String line;
			
				try {
					while( (line = br.readLine()) != null){
						//System.out.println("sfvzx");
						
						String[] p =	line.split("\t");
						String key = p[0];
						//int key = Integer.parseInt(p[0]);
						ArrayList<Long> arraylist = new ArrayList<Long>();
						Long start = Long.parseLong(p[1]);
					    arraylist.add(start);
					    Long end = Long.parseLong(p[2]);
					   // int end = Integer.parseInt(p[2]);
					    arraylist.add(end);

					   
					    Offset.put(key, arraylist);
					   
						
					}
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			
				
	return (HashMap<String, ArrayList<Long>>) Offset;
	}


	

	private static String MergeLinks(String il1, String il2){
		String str ="";
		Set<String> s = new HashSet<String>();
		if(il1 != null && il2 != null){
			
		String[] ils1 = il1.split("\t");
		String[] ils2 = il2.split("\t");
		for(int k=0; k< ils1.length; k++){
			s.add(ils1[k]);
			}
		for(int k=0; k< ils2.length; k++){
			s.add(ils2[k]);
		}
		for (String s1 : s) {
			str = str + s1 + "\t";
		}
		return str;
		}
		else return il1;
		
		
	}
	
private static String  getWave(ArrayList<Integer> StAndEnd) throws FileNotFoundException {
		
		int start1 = StAndEnd.get(0);
    	int end1 = StAndEnd.get(1);
    	byte[] string = null;
    	File file123 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/LinkWavesF.txt");
		RandomAccessFile randFile = new RandomAccessFile(file123, "rw");
		 
		
		string = new byte[end1 - start1];
		try {
			randFile.seek(start1);
			randFile.readFully(string);
			randFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String InLinks = new String(string);
		String[] ils = InLinks.split("\t");
		String str = ils[1];
		
		
		return str;
		
	}
	
	private static String  getInlinks(ArrayList<Integer> StAndEnd) throws FileNotFoundException {
		
	//	System.out.println(StAndEnd);
		if(StAndEnd != null){
	
		int start1 = StAndEnd.get(0);
    	int end1 = StAndEnd.get(1);
    	byte[] string = null;
    	File file123 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/InLinkF.txt");
		RandomAccessFile randFile = new RandomAccessFile(file123, "rw");
		 
		
		string = new byte[end1 - start1];
		try {
			randFile.seek(start1);
			randFile.readFully(string);
			randFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String InLinks = new String(string);
		String[] ils = InLinks.split("\t");
		String str = "";
		for(int k=1; k< ils.length; k++){
			str = str+ils[k]+"\t";
			
			}	
		return str;
		}
		
		else return "*";	
		
	}
	
	
private static String  getOutlinks(ArrayList<Integer> StAndEnd) throws FileNotFoundException {
		
		int start1 = StAndEnd.get(0);
    	int end1 = StAndEnd.get(1);
    	byte[] string = null;
    	File file123 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/OutLinkF.txt");
		RandomAccessFile randFile = new RandomAccessFile(file123, "rw");
		 
		
		string = new byte[end1 - start1];
		try {
			randFile.seek(start1);
			randFile.readFully(string);
			randFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		String InLinks = new String(string);
		String[] ils = InLinks.split("\t");
		String str = "";
		for(int k=1; k< ils.length; k++){
			str = str+ils[k]+"\t";
			
		}
		
		return str;
		
	}
	

private static HashMap<String,ArrayList<Integer>>  createCatalog(File FileName) {
	
	 HashMap<String,ArrayList<Integer>> Offset = new HashMap<String,ArrayList<Integer>>();
		
	
			BufferedReader br = null;
			try {
				br = new BufferedReader(new InputStreamReader(new FileInputStream(FileName)));
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String line;
		
			try {
				while( (line = br.readLine()) != null){
					
					String[] p =	line.split(" ");
					String key = p[0];
					//int key = Integer.parseInt(p[0]);
					ArrayList<Integer> arraylist = new ArrayList<Integer>();
					int start = Integer.parseInt(p[1]);
				    arraylist.add(start);
				    int end = Integer.parseInt(p[2]);
				    arraylist.add(end);

				   
				    Offset.put(key, arraylist);
				   
					
				}
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
			
return (HashMap<String, ArrayList<Integer>>) Offset;
}	

	
	
	
private static HashMap<String, Float> sortHM(Map<String, Float> aMap) {
        
        Set<Entry<String,Float>> mapEntries = aMap.entrySet();
       List<Entry<String,Float>> aList = new LinkedList<Entry<String,Float>>(mapEntries);

        Collections.sort(aList, new Comparator<Entry<String,Float>>() {

            
            public int compare(Entry<String, Float> ele1,
                    Entry<String, Float> ele2) {
                
                return ele2.getValue().compareTo(ele1.getValue());
            }
        });
       
        Map<String,Float> aMap2 = new LinkedHashMap<String, Float>();
        for(Entry<String,Float> entry: aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }
      
            return (HashMap<String, Float>) aMap2;
        }
}