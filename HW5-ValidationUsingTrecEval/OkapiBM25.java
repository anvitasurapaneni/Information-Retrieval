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

public class OkapiBM25 {
	public static void main(String[] args) throws IOException
	
	
	{
		

		File QueryFile = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/query_desc.A5.short.txt");
//		String Queries = FileUtils.readFileToString(QueryFile);
//		System.out.println(Queries);
		
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
 				.put("cluster.name","anv").build();
 	
         Client client = TransportClient.builder().settings(settings).build()
         		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
     
         BufferedReader br = new BufferedReader(new FileReader(QueryFile));  
		    String FullQueryTemp = null; 
		    

		    Float avgLength = (float) (20976545 / 84612);
		    Float b = (float) 0.75;
		    Float k_1 = (float) 1.6;

		    PrintWriter writer = new PrintWriter("okapiBM25A4.txt", "UTF-8");
		    
		    while ((FullQueryTemp = br.readLine()) != null)  

		    {  
//for each query
		    	if (FullQueryTemp.length() <= 6)
		    	{break;}
		    	 HashMap<String, Float> OkapiBM25MAP = new HashMap<String, Float>();
		    	 
			   // Map<String, Float> SortedMap = new HashMap<String, Float>();
			    HashMap<String, Float> SortedMap = new HashMap<String, Float>();
				    
				    String queryNo = FullQueryTemp.substring(0, 6);
			    	queryNo = queryNo.replace(".", "");
			    	queryNo = queryNo.trim();
			    	System.out.println(queryNo);
			    	
			    	Float OkapiTFD = (float) 0;
			    	
			    		
			    		// System.out.println(m.getKey()+" "+m.getValue());
// for each word in query
			    		 int i;
				         int j;
				     //    FullQueryTemp = FullQueryTemp.substring(0, FullQueryTemp.length());
				         for (i = 0 + 8; i <= FullQueryTemp.length() - 1 - 8; i++){
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
//operation on each word in query for every doc
				                  		final Map<String, Object> params = new HashMap<String, Object>();
				                         params.put("term", WordQueryTemp);
				                         params.put("field", "text");

				                  SearchResponse responseTFLength = client.prepareSearch("four")
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
				                  
				                  Float DFt =  (float) responseTFLength.getHits().getTotalHits();
				                  
				                  for ( SearchHit hit : responseTFLength.getHits().hits())
				                  {
				                  	String DocID = hit.getId();
				                  	Float TFWD = hit.getScore();
				                  	Integer lenD = (hit.getFields().get("getDOCLENGTH").getValue());
				                  	
				                  //	System.out.println( DocNo);
				                  	
				                  	 Float  OkapiBM25 = (float) (Math.log((85678+0.5)/(DFt + 0.5))*((TFWD+(k_1*TFWD))/(TFWD+k_1*((1-b)+b*(lenD/avgLength)))));
				                  	
				            
				             //  OkapiTFD = OkapiTFD + OkapiTFWD;
				                  	OkapiBM25MAP.put(DocID, 
				                  			OkapiBM25MAP.get(DocID) == null? 0.0F : OkapiBM25MAP.get(DocID) + OkapiBM25);	
		                    	 	
				                  	
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
								
								writer.println(queryNo+"  Q0  "+m1.getKey()+"  "+rank+"  "+m1.getValue()+"  OkapiTF  ");
								
								
								
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
}


