package Anvita.CS6200;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;

//import org.json.simple.*;
//import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.lang.Object;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import java.util.stream.Stream;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;


public class Laplace_A2{
	public static void main(String[] args) throws IOException{
		
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
			//	System.out.println(DocId);
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
			


			
			
			PrintWriter writer = new PrintWriter("LapSm_A2.txt", "UTF-8");
			Float Vocabulary=(float) 198196;
			
			
			
	        int count =0;
	        

			
File QueryFile = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/query_desc.51-100.short.txt");
			
BufferedReader br1 = new BufferedReader(new FileReader(QueryFile));   

HashMap<String,Float> LaplaceSmoothing = new HashMap<String,Float>();
 HashMap<String, Float> SortedMap = new HashMap<String, Float>();
 String FullQueryTemp = "";
while ((FullQueryTemp = br1.readLine()) != null)  
{
	if (FullQueryTemp.length() <= 3)
		break;
	 HashMap<Integer,ArrayList<String>> wordAndListOfDocs = new HashMap<Integer,ArrayList<String>>();

System.out.println(FullQueryTemp);
	 
	 String queryNo = FullQueryTemp.substring(0, 3);
 	queryNo = queryNo.replace(".", "");
 	queryNo = queryNo.trim();
 	
    
 	
 	
	
 	String[] queryparams= FullQueryTemp.substring(6, FullQueryTemp.length()).split("\\s+");

// get list of documents that contain these terms
 	
 	 for(String WordQueryTemp: queryparams)
	    {
 		WordQueryTemp = WordQueryTemp.replace(".", "");
 		WordQueryTemp = WordQueryTemp.trim();
 		 System.out.println(WordQueryTemp);
 		Integer  WordQueryTempId = TIDmap.get(WordQueryTemp);
 		//System.out.println(WordQueryTempId);
 		ArrayList<Integer> strAndEnd = Catalog.get(WordQueryTempId);
    	
 		ArrayList<String> listOfDocsforWord  = new ArrayList<String>();
   	 
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
		String strTotal = new String(string);
		String TFstr = strTotal.split(":")[1];
		String TFS[] = TFstr.split(";");
		
		for(int k=0; k< TFS.length; k++){
			String[] tfd = TFS[k].split("_");
			String DocId =  tfd[0];
			listOfDocsforWord.add(DocId);
		}
		
	    }
   	wordAndListOfDocs.put(WordQueryTempId, listOfDocsforWord);
   	
 	

 		}
// 	System.out.println("db,sb,...........");
// 	for(Map.Entry m:wordAndListOfDocs.entrySet()){
//
//
// 		System.out.println(m.getKey());
//
//	    }
                	
                	  for(Map.Entry m:AllDocs.entrySet())

                	     { 
                		  
                		  String DocID ="";
                		  Double LM=0.0;
                	    Integer key =  (Integer) m.getKey();
                	    String KEY = Integer.toString(key);
                	    
                	    for(String WordQueryTemp: queryparams)
                	    {
                	    	 WordQueryTemp = WordQueryTemp.replace(".", " ");
                	    	 WordQueryTemp = WordQueryTemp.trim();
                	    	 Integer  WordQueryTempId = TIDmap.get(WordQueryTemp);
                	    	 
                	    	 ArrayList<String> lod = wordAndListOfDocs.get(WordQueryTempId);
                	    	 
                	    	 if(lod != null && lod.contains(KEY) ){
                	    		// System.out.println("this");
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
		        					 Integer TFWD;
		                      		  Integer lenD;
		                      		  Integer did = key;
		                        	 DocID = Integer.toString(did);
		                        	 
		                        	 if(TFmap.get(DocID) == null){
		                          		TFWD = 0;
		                          	}
		                          	else TFWD = TFmap.get(DocID);
		                          	
		                          	if(DocLen.get(DocID) == null){
		                          		lenD = 0;
		                          	}
		                          	else lenD = DocLen.get(DocID);
		                          	
		                          	 Float term1 = (float) ((TFWD+1)/(lenD+Vocabulary));  

		                             float LMTemp = (float) Math.log(term1);
		                             LM= LM + LMTemp;
		        				
		                    	}
		                    	 else{
		                	    		int lenD = DocLen.get(KEY);
		                	    		//System.out.println(lenD);
		                	    		 Float term1 = (float) ((0 + 1)/(lenD+Vocabulary));  

		                	    		 float LMTemp = (float) Math.log(term1);
		                	    		 LM= LM + LMTemp;
		                	    	 }
                	    	 }
                	    	
                	    	 
		       
                  

           	    	 LaplaceSmoothing.put(KEY, 
                                (float) (LaplaceSmoothing.get(KEY) == null? LM :LaplaceSmoothing.get(KEY) + LM));   
            	
        
          
                	     }
                	  SortedMap = sortHM(LaplaceSmoothing);
             	     LaplaceSmoothing.clear();
             	     int rank = 0;
             		  
             		    for(Map.Entry sm:SortedMap.entrySet())
             			{  
             			
             			if((rank < 1000 ))
             			
             			{
             				rank = rank + 1;
             				int d = Integer.parseInt((String) sm.getKey());
             				writer.println(queryNo+"  Q0  "+AllDocs.get(d)+"  "+rank+"  "+sm.getValue()+"  LAPLACE  ");
             			}
             			else break;
             				
             			}
             			
             	  
             		SortedMap.clear();
                	  
                	 
		}
              
 	
   


writer.close();
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





