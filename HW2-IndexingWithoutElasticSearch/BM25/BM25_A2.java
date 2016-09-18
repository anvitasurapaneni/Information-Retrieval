package Anvita.CS6200;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.scanner.Scanner;

import java.io.BufferedReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import javax.naming.directory.SearchResult;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

import org.apache.commons.io.FileUtils;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class BM25_A2 {
	public static void main(String[] args) throws IOException
	
	
	{
		// doc list
				File f1 = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/doclist_new_0609.txt");
				HashMap<Integer, String>  AllDocs = new HashMap<Integer, String>();
				 BufferedReader br4 = new BufferedReader(new FileReader(f1));
				 String line8 ="";
					while (( line8 = br4.readLine()) != null)  {
						
						String[] docs =	 line8.split(" ");
						String DocNo = docs[0];
						DocNo = DocNo.trim();
						int dno = Integer.parseInt(DocNo);
						
						String DocId = docs[2];
						// DocId = DocId.trim();
						System.out.println(DocId);
						AllDocs.put(dno, DocId);
						
					}
					br4.close();
					
					// TID
					 File file = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/TID.txt");
					 HashMap<String, Integer>  TIDmap = new HashMap<String, Integer>();
					 BufferedReader br3 = new BufferedReader(new FileReader(file));
					 String line ="";
						while (( line = br3.readLine()) != null)  {
							String[] TID =	 line.split(":");
						
								String term = TID[0];
								term = term.trim();
								String Tid = TID[1];
								Tid = Tid.trim();
								int tid = Integer.parseInt(Tid);
								TIDmap.put(term, tid);
								
						}
						br3.close();
						
						// Doc Len
						
						 File file1 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/DOClen.txt");
						 HashMap<String, Integer>  DocLen = new HashMap<String, Integer>();
						 BufferedReader br2 = new BufferedReader(new FileReader(file1));
						 String line1 ="";
							while (( line1 = br2.readLine()) != null)  {
								String[] DLen =	 line1.split(":");
							
									String DocId = DLen[0];
									DocId = DocId.trim();
									String L = DLen[1];
									L= L.trim();
									int len = Integer.parseInt(L);
									DocLen.put(DocId, len);
									
							}
							br2.close();
							
							// avg length
							int noOfDocs = 0;
							int TotalDocLengths = 0;
							for(Map.Entry m:DocLen.entrySet()){
								
								int doclen = (Integer) m.getValue();
								noOfDocs = noOfDocs + 1;
								TotalDocLengths = TotalDocLengths + doclen;

							}
							
							Float avgLength = (float) (TotalDocLengths / noOfDocs);
							
							File cat = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/FinalCatalog.txt");
							HashMap<Integer,ArrayList<Integer>> Catalog = createCatalog(cat);
					
				

		File QueryFile = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/query_desc.51-100.short.txt");
//		String Queries = FileUtils.readFileToString(QueryFile);
//		System.out.println(Queries);
		
		
         BufferedReader br = new BufferedReader(new FileReader(QueryFile));  
		    String FullQueryTemp = null; 
		    

		    
		    Float b = (float) 0.75;
		    Float k_1 = (float) 1.6;

		    PrintWriter writer = new PrintWriter("BM25_A2.txt", "UTF-8");
		    
		    while ((FullQueryTemp = br.readLine()) != null)  

		    {  
//for each query
		    	if (FullQueryTemp.length() <= 3)
		    	{break;}
		    	 HashMap<String, Float> OkapiBM25MAP = new HashMap<String, Float>();
		    	 
			   // Map<String, Float> SortedMap = new HashMap<String, Float>();
			    HashMap<String, Float> SortedMap = new HashMap<String, Float>();
				    
				    String queryNo = FullQueryTemp.substring(0, 3);
			    	queryNo = queryNo.replace(".", "");
			    	queryNo = queryNo.trim();
			    	System.out.println(queryNo);
			    	
			    	Float OkapiTFD = (float) 0;
			    	
			    		
			    		// System.out.println(m.getKey()+" "+m.getValue());
// for each word in query
			    		 int i;
				         int j;
				     //    FullQueryTemp = FullQueryTemp.substring(0, FullQueryTemp.length());
				         for (i = 0 + 5; i <= FullQueryTemp.length() - 1 - 5; i++){
				             if (FullQueryTemp.substring(i).startsWith(" ") || i == 0){

				                 
				                 for (j = i + 1 ; j <= FullQueryTemp.length() - 1 ; j++){

				                     if (FullQueryTemp.substring(j).startsWith(" ") || j == FullQueryTemp.length() - 1) 
				                     {
				                         
				                    	 String WordQueryTemp = FullQueryTemp.substring(i, j);
				                    	// System.out.println(WordQueryTemp);
				                    	 
				                    	 WordQueryTemp = WordQueryTemp.replace(".", " ");
				                    	 
				                    	 WordQueryTemp = WordQueryTemp.trim();
				                 //   	 System.out.print("word:");
				                 //   	 System.out.println(WordQueryTemp);
				                    	 
				                    	 Integer  WordQueryTempId = TIDmap.get(WordQueryTemp);
					                    	// get TF map for that term
					                    	ArrayList<Integer> strAndEnd = Catalog.get(WordQueryTempId);
					                    	
					                    	 HashMap<String, Integer>  TFmap = new HashMap<String, Integer>();
					                    	 Float DFt = (float) 0;
					                    	if(strAndEnd != null){
					                    	
					                    	int start1 = strAndEnd.get(0);
					                    	int end1 = strAndEnd.get(1);
					                    	byte[] string = null;
					                    	File file123 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/final.txt");
					            			RandomAccessFile randFile = new RandomAccessFile(file123, "rw");
					        				 
					        				
					        				string = new byte[end1 - start1];
					        				randFile.seek(start1);
					        				randFile.readFully(string);
					        				randFile.close();
					        				String TFstrTotal = new String(string);
					                    	// end get TF
					        				
					        				
					        				String TFstr = TFstrTotal.split(":")[1];
					        				String TFS[] = TFstr.split(";");
					        				
					        				 DFt =  (float) TFS.length;
					        				
					        				for(int k=0; k< TFS.length; k++){
					        					String[] tfd = TFS[k].split("_");
					        					String DocId =  tfd[0];
					        					String tf1 =  tfd[1];
					        					int tf = Integer.parseInt(tf1);
					        					TFmap.put(DocId, tf);
					        				}
					                    	}
				                    	 
//operation on each word in query for every doc
					                    	
						                	  for(Map.Entry m:AllDocs.entrySet())
						                  {
						                		  Integer TFWD;
						                		  Integer lenD;
						                		  Integer did = (Integer) m.getKey();
						                  	String DocID = Integer.toString(did);
						                  			
						                  //	System.out.println(DocID);
						                  	if(TFmap.get(DocID) == null){
						                  		TFWD = 0;
						                  	}
						                  	else TFWD = TFmap.get(DocID);
						                  	
						                  	if(DocLen.get(DocID) == null){
						                  		lenD = 0;
						                  	}
						                  	else lenD = DocLen.get(DocID);
				                  
				                  	
				                  
				                  	
				                  //	System.out.println( DocNo);
				                  	
				                  	 Float  OkapiBM25 = (float) (Math.log((84678+0.5)/(DFt + 0.5))*((TFWD+(k_1*TFWD))/(TFWD+k_1*((1-b)+b*(lenD/avgLength)))));
				                  	
				            
				             //  OkapiTFD = OkapiTFD + OkapiTFWD;
				                  	OkapiBM25MAP.put(DocID, 
				                  			OkapiBM25MAP.get(DocID) == null? OkapiBM25 : OkapiBM25MAP.get(DocID) + OkapiBM25);	
		                    	 	
				                  	
				                   }
				                   
//end operation on each word in query for every doc

				                    	 
				                    	 
				                    	 i = j;
				                     }}}}
//end for each word in query
//end for each doc

				         SortedMap = sortHM(OkapiBM25MAP);
				         //op sorted map
				        // int count = 0;
				         int rank = 0;
						  
						    for(Map.Entry m1:SortedMap.entrySet())
							{  
							
							if(rank < 1000){
								rank = rank + 1;
								int d = Integer.parseInt((String) m1.getKey());
								writer.println(queryNo+"  Q0  "+AllDocs.get(d)+"  "+rank+"  "+m1.getValue()+"  OkapiTF  ");
								
								
								
							}
							else 
								
								break;
								
							}
						    SortedMap.clear();
						    OkapiBM25MAP.clear();
				         //op sorted map
			    		 }
		   
			    	
			    //sop	OkapiTFD
			    	
		    writer.close();    	
//end for each query		            
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



private static HashMap<Integer,ArrayList<Integer>>  createCatalog(File FileName) {
	
	 HashMap<Integer,ArrayList<Integer>> Offset = new HashMap<Integer,ArrayList<Integer>>();
		
	
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
					
					String[] p =	line.split(":");
					int key = Integer.parseInt(p[0]);
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
			
return (HashMap<Integer,ArrayList<Integer>>) Offset;
}	


}


