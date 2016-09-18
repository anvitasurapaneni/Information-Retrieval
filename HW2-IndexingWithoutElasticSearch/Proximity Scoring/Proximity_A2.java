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


public class Proximity_A2{
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
			


			
			
			PrintWriter writer = new PrintWriter("ProximityScoring_A2.txt", "UTF-8");
			Float Vocabulary=(float) 198196;
			
			
			
	        int count =0;
	        

			
File QueryFile = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/query_desc.51-100.short.txt");
			
BufferedReader br1 = new BufferedReader(new FileReader(QueryFile));   

HashMap<String,Float> ProximityScoring = new HashMap<String,Float>();
 HashMap<String, Float> SortedMap = new HashMap<String, Float>();
 String FullQueryTemp = "";
while ((FullQueryTemp = br1.readLine()) != null)  
{
	if (FullQueryTemp.length() <= 3)
		break;

System.out.println(FullQueryTemp);
	 
	 String queryNo = FullQueryTemp.substring(0, 3);
 	queryNo = queryNo.replace(".", "");
 	queryNo = queryNo.trim();
 	int QuerySize = 0;
    
 	 HashMap<String, ArrayList<String>> DocIdAndListOfLopForQuery =
				 new HashMap<String, ArrayList<String>>();
 	
	
 	String[] queryparams= FullQueryTemp.substring(6, FullQueryTemp.length()).split("\\s+");

// get list of documents that contain these terms
 	
 	 for(String WordQueryTemp: queryparams)
	    {
 		QuerySize = QuerySize + 1;
 		WordQueryTemp = WordQueryTemp.replace(".", "");
 		WordQueryTemp = WordQueryTemp.toLowerCase();
 		WordQueryTemp = WordQueryTemp.trim();
 		 System.out.println(WordQueryTemp);
 		Integer  WordQueryTempId = TIDmap.get(WordQueryTemp);
 		//System.out.println(WordQueryTempId);
 		ArrayList<Integer> strAndEnd = Catalog.get(WordQueryTempId);
    	
 		
 		
   	 
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
			String LOP =  tfd[2];
			ArrayList<String> lop = new ArrayList<String>();
			if(DocIdAndListOfLopForQuery.get(DocId) == null){
				lop.add(LOP);	
			}
			else{
				lop = DocIdAndListOfLopForQuery.get(DocId);
				lop.add(LOP);
			}
			
			DocIdAndListOfLopForQuery.put(DocId, lop);	
          	 	
		}
		
	    }
   
 		}
  	
                	  for(Map.Entry m:DocIdAndListOfLopForQuery.entrySet())

                	     { 
                		  
                		  
                	    
                	    String KEY = (String) m.getKey();
                	    ArrayList<String> ListofPositions = (ArrayList<String>) m.getValue();
                	   
                	    int s = GetMinSpanForListofPositionsForTerms(ListofPositions);
    			    	if(ListofPositions.size() >= QuerySize/2 && s != 0){
    			    		
    			    		// System.out.println("this");
    			    		int k = ListofPositions.size();
    			    		// int k  = QuerySize;
    			    		// System.out.println(k);
    			    		String DocID = (String) m.getKey();
    			    		int lenD = DocLen.get(DocID);
    				    	int x = (s -k)/k;
    				    	// float score = (int) Math.pow(0.8, x);
    				    	//System.out.println(score);
    				    	float score =	 (1500 - s) * k / (lenD + Vocabulary);
    				    	 Integer did = Integer.parseInt(KEY);
    		                  	
                	    	
                	    	 
		       
                  

           	    	 ProximityScoring.put(KEY, score);   
            	
    			    	}
          
                	     }
                	  SortedMap = sortHM(ProximityScoring);
             	     ProximityScoring.clear();
             	     int rank = 0;
             		  
             		    for(Map.Entry sm:SortedMap.entrySet())
             			{  
             			
             			if((rank < DocIdAndListOfLopForQuery.size() ))
             			
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
	
	
	private static Integer GetMinSpanForListofPositionsForTerms
	(ArrayList<String> ListofPositionsForTerms){
	
HashMap<Integer,HashMap<Integer,Integer>> listofLists = new HashMap<Integer,HashMap<Integer,Integer>>();
		
		int NoOfTerms = ListofPositionsForTerms.size();
		int no0fIts = 0;
		// list no followed by positon to compare
		HashMap<Integer, Integer> WIndowPositions = new HashMap<Integer, Integer>();
		// creating window positions
		for (int i=1; i<= NoOfTerms; i++){
			WIndowPositions.put(i, 1);
		}
		
		// creating list out of string
		int listCount = 0;
		// adding each list of positions into the map for list no, list of positions
		for( int j = 0; j < ListofPositionsForTerms.size(); j++){
		String[] poss = ListofPositionsForTerms.get(j).split(",");
		// count followed by actual position in the doc
		HashMap<Integer,Integer> list = new HashMap<Integer,Integer>();
		int count = 0;
		for(int i = 0; i < poss.length; i++ ){
			int val = Integer.parseInt(poss[i]);
			count = count + 1;
			list.put(count, val);
		}
		listCount = listCount + 1;
		listofLists.put(listCount, list);
		no0fIts = no0fIts + list.size() - 1;
		}
		
		
		// Initialize working window
		HashMap<Integer,Integer> WorkingWindow =
				createWorkingWindow( listofLists,  WIndowPositions, NoOfTerms);
		
		
		
		// print working window
		 for(Map.Entry m:WorkingWindow.entrySet()){
		//	 System.out.println(m.getKey()+":"+m.getValue());
		 	}
		 int minSpan = getSpan(WorkingWindow, NoOfTerms);
		
		// System.out.println("Minspan:"+minSpan);
		
		 HashMap<Integer,Integer> SortedWorkingWindow = new HashMap<Integer,Integer>();
		 
		 //loop
		for(int i = 1; i <= no0fIts; i++){
			 //Iterator
			 SortedWorkingWindow = sortHM1(WorkingWindow);

			//sliding
			for(Map.Entry m:sortHM1(WorkingWindow).entrySet()){

				int lestvalueList = (Integer) m.getKey();
				if( WIndowPositions.get(lestvalueList) + 1 <= listofLists.get(lestvalueList).size()){
					WIndowPositions.put(lestvalueList, WIndowPositions.get(lestvalueList) + 1);
					break;
				}
				}
			//end sliding
			
			// update working window
					 WorkingWindow = 
							createWorkingWindow( listofLists,  WIndowPositions, NoOfTerms);
					 
		// print working window
//					 for(Map.Entry m:WorkingWindow.entrySet()){
//						 System.out.println(m.getKey()+":"+m.getValue());
//					 	}
				int span = getSpan(WorkingWindow, NoOfTerms);
			//	System.out.println("span:"+span);
				if(span < minSpan){
					minSpan = span;
				}
			//	System.out.println("Minspan:"+minSpan);
					 
		}
		
		
	return minSpan;
	}
	
	
	private static HashMap<Integer,Integer> createWorkingWindow
	(HashMap<Integer,HashMap<Integer,Integer>> listofLists, 
			HashMap<Integer, Integer> WIndowPositions,
			int NoOfTerms){
		
		HashMap<Integer,Integer> WorkingWindow = new  HashMap<Integer,Integer>();
		for(int i =1; i<= NoOfTerms; i++){
			WorkingWindow.put(i, listofLists.get(i).get(WIndowPositions.get(i)));
		}
		return WorkingWindow;
	}
	
private static HashMap<Integer,Integer> sortHM1(HashMap<Integer,Integer> aMap) {
        
        Set<Entry<Integer,Integer>> mapEntries = aMap.entrySet();
       List<Entry<Integer,Integer>> aList = new LinkedList<Entry<Integer,Integer>>(mapEntries);

        Collections.sort(aList, new Comparator<Entry<Integer,Integer>>() {

            
            public int compare(Entry<Integer,Integer> ele1,
                    Entry<Integer,Integer> ele2) {
                
                return ele1.getValue().compareTo(ele2.getValue());
            }
        });
       
        Map<Integer,Integer> aMap2 = new LinkedHashMap<Integer,Integer>();
        for(Entry<Integer,Integer> entry: aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }
      
            return (HashMap<Integer,Integer>) aMap2;
        }
	
	
	private static Integer  getMin (HashMap<Integer,Integer> list) 
	{
		int min = list.get(1);
		for (int i =2 ; i<= 2; i++){
			if(list.get(i) < min){
				min = list.get(i);
			}
		}
		return min;
	}
	
	private static Integer  getSpan (HashMap<Integer,Integer> list, int NoOfTerms) 
	{
		
			
		if(list.get(1) != null){
			
		
	//	System.out.println(list);
		int min = list.get(1);
		int max = list.get(1);
		for (int i =2 ; i<= NoOfTerms; i++){
			if(list.get(i) < min){
				min = list.get(i);
			}
			if(list.get(i) > max){
				max = list.get(i);
			}
		}
		
		return (max -min);
		}
		else return 100000;
		
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





