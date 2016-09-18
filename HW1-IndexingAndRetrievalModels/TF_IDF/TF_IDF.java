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
import java.io.File;
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
import java.io.PrintWriter;

import org.apache.commons.io.FileUtils;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class TF_IDF {
	public static void main(String[] args) throws IOException
	
	
	{
		

		File QueryFile = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/query_desc.51-100.short.txt");
//		String Queries = FileUtils.readFileToString(QueryFile);
//		System.out.println(Queries);
		
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
 				.put("cluster.name","elasticsearch").build();
 	
         Client client = TransportClient.builder().settings(settings).build()
         		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
         
		
	
		
		    
		    
		    BufferedReader br = new BufferedReader(new FileReader(QueryFile));  
		    String FullQueryTemp = null; 
		    

		   
		    Float D = (float) 84678;
		    Float avgLength = (float) (20976545 / 84612);

		    PrintWriter writer = new PrintWriter("TFIDF.txt", "UTF-8");
		    
		    while ((FullQueryTemp = br.readLine()) != null)  

		    {  
//for each query
		    	if (FullQueryTemp.length() <= 3)
		    		break;
		    	 HashMap<String, Float> TF_IDF = new HashMap<String, Float>();
		    	 
			   // Map<String, Float> SortedMap = new HashMap<String, Float>();
			    HashMap<String, Float> SortedMap = new HashMap<String, Float>();
				    
				    String queryNo = FullQueryTemp.substring(0, 3);
			    	queryNo = queryNo.replace(".", "");
			    	queryNo = queryNo.trim();
			    	//System.out.println(queryNo);
			    	
			    	Float OkapiTFD = (float) 0;
			    	
			    		
			    		// System.out.println(m.getKey()+" "+m.getValue());
// for each word in query
			    		 int i;
				         int j;
				         for (i = 0 + 5; i <= FullQueryTemp.length() - 1 - 5; i++){
				             if (FullQueryTemp.substring(i).startsWith(" ") || i == 0){

				                 
				                 for (j = i + 1 ; j <= FullQueryTemp.length() - 1 ; j++){

				                     if (FullQueryTemp.substring(j).startsWith(" ") || j == FullQueryTemp.length() - 1) 
				                     {
				                         
				                    	 String WordQueryTemp = FullQueryTemp.substring(i, j);
				                    	 WordQueryTemp = WordQueryTemp.replace(".", " ");
				                    	 WordQueryTemp = WordQueryTemp.trim();
				                    	// System.out.println(WordQueryTemp);
//operation on each word in query for every doc
				                  		final Map<String, Object> params = new HashMap<String, Object>();
				                         params.put("term", WordQueryTemp);
				                         params.put("field", "text");

				                  SearchResponse response = client.prepareSearch("ap_dataset")
				                                 .setTypes("document")
				                                 .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				                                 .setQuery(QueryBuilders.functionScoreQuery
				                                           (QueryBuilders.termQuery("text", WordQueryTemp), 
				                                             new ScriptScoreFunctionBuilder(new Script("getTF", 
				                                                     ScriptType.INDEXED, "groovy", params)))
				                                           
				                                           .boostMode("replace"))
				                                 .setFrom(0)
				                                 .setSize(10000)
				                                 .addScriptField("getDOCLENGTH", (new Script("doc['text'].values.size()")))
				                                 .get();
				                  
				                  Float DFt =  (float) response.getHits().getTotalHits();
				                //  System.out.println(WordQueryTemp);
				                //  System.out.println(DFt);
				                  
				                  
				                  for ( SearchHit hit : response.getHits().hits())
				                  {
				                  
				                  	String DocID = hit.getId();
				                  	Float TFWD = hit.getScore();
				                  	Integer lenD = (hit.getFields().get("getDOCLENGTH").getValue());
				                  //	System.out.println( DocNo);
				               Float   	OkapiTFWD = (float) (TFWD/(TFWD + 0.5 + (1.5*(lenD/avgLength))));
				               Float   	TF_IDFWD = (float) (OkapiTFWD * (Math.log(D/DFt)));
				             //  OkapiTFD = OkapiTFD + OkapiTFWD;
				               TF_IDF.put(DocID, 
				            		   TF_IDF.get(DocID) == null? 0.0F : TF_IDF.get(DocID) + TF_IDFWD);	
		                    	 	
				                  
				                   }
				                   
//end operation on each word in query for every doc

				                    	 
				                    	 
				                    	 i = j;
				                     }}}}
//end for each word in query
//end for each doc

				         SortedMap = sortMapByValues(TF_IDF);
				         //op sorted map
				         int count = 1001;
				         int rank = 0;
						  
						    for(Map.Entry m1:SortedMap.entrySet())
							{  count--;
							rank = rank + 1;
							
							if(count> 0){
								writer.println(queryNo+"\tQ0\t"+m1.getKey()+"\t"+rank+"\t"+m1.getValue()+"\tOkapiTF\t");
								
								
								
							}
							else 
								
								break;
								
							}
						    SortedMap.clear();
						    TF_IDF.clear();
				         //op sorted map
			    		 }
		   
			    	
			    //sop	OkapiTFD
			    	
		    writer.close();    	
//end for each query		            
		    }
		    

		
	private static HashMap<String, Float> sortMapByValues(Map<String, Float> aMap) {
        
        Set<Entry<String,Float>> mapEntries = aMap.entrySet();
        
        // used linked list to sort, because insertion of elements in linked list is faster than an array list. 
        List<Entry<String,Float>> aList = new LinkedList<Entry<String,Float>>(mapEntries);

        // sorting the List
        Collections.sort(aList, new Comparator<Entry<String,Float>>() {

            
            public int compare(Entry<String, Float> ele1,
                    Entry<String, Float> ele2) {
                
                return ele2.getValue().compareTo(ele1.getValue());
            }
        });
        
        // Storing the list into Linked HashMap to preserve the order of insertion. 
        Map<String,Float> aMap2 = new LinkedHashMap<String, Float>();
        for(Entry<String,Float> entry: aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }
      
            return (HashMap<String, Float>) aMap2;
        }
}
