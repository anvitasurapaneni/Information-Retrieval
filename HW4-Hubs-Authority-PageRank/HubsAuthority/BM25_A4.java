package HitsCrawl;

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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

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

public class BM25_A4 {
	public static void main(String[] args) throws IOException
	
	
	{
		


		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
 				.put("cluster.name","anv").build();
 	
         Client client = TransportClient.builder().settings(settings).build()
         		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
    

		    Float avgLength = (float) (68807287 / 48170);
		    Float b = (float) 0.75;
		    Float k_1 = (float) 1.6;

		    PrintWriter writer = new PrintWriter("A4_BasicSetFromOkapi.txt", "UTF-8");
		    
		 

		    
			    	
			    	Float OkapiTFD = (float) 0;
			    	
			    		
			    		// System.out.println(m.getKey()+" "+m.getValue());
// for each word in query
			    	 HashMap<String, Float> OkapiBM25MAP = new HashMap<String, Float>();
			    	 
					   // Map<String, Float> SortedMap = new HashMap<String, Float>();
					    HashMap<String, Float> SortedMap = new HashMap<String, Float>();
				         String FullQueryTemp = "terrorism 21 century fear";
				        
				                         
				                    	 String WordQueryTemp1[] = FullQueryTemp.split(" ");
				                    	 for(String WordQueryTemp: WordQueryTemp1){
				                    	
				                    	 
				                    	 WordQueryTemp = WordQueryTemp.replace(".", " ");
				                    	 
				                    	 WordQueryTemp = WordQueryTemp.trim();
				                 //   	 System.out.print("word:");
				                 //   	 System.out.println(WordQueryTemp);
//operation on each word in query for every doc
				                    	 final Map<String, Object> params = new HashMap<>();
				                         params.put("term", WordQueryTemp);
				                         params.put("field", "text");

				                         
				                         
				                     	SearchResponse scrollResp = client.prepareSearch("four")
				                     	            .setScroll(new TimeValue(60000))
				                     	            .setTypes("document")
				                     	           .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				                                   .setQuery(QueryBuilders.functionScoreQuery
				                                             (QueryBuilders.termQuery("text", WordQueryTemp), 
				                                               new ScriptScoreFunctionBuilder(new Script("getTF", 
				                                                       ScriptType.INDEXED, "groovy", params)))
				                                             .boostMode("replace"))
				                     	            .setFrom(0)
				                     	           .setSize(1000)
				                     	          .addScriptField("getDOCLENGTH", (new Script("doc['text'].values.size()")))
				                     	       //  .addScriptField("getTEXT", (new Script("doc['text']")))
				                     	          .execute()
				                                  .actionGet();
				                         
				                         
//				                  
				                  Float DFt =  (float) scrollResp.getHits().getTotalHits();
				                  System.out.println("DFT"+DFt);
				                  
				                  
				                  
				              	while (true) {

					    		    for (SearchHit hit : scrollResp.getHits().getHits())    {
					                  	String DocID = hit.getId().toLowerCase();
					                 // 	String text1 =  (hit.getFields().get("getTEXT").getValue());
					                //  	SearchHitField str = hit.field("getAUTHR");
					                 //	System.out.println(text1);
					                	Float TFWD = hit.getScore();
					                  	//= getTF(WordQueryTemp, text1);
					                  //	System.out.println("TFWD:"+TFWD);
					                  	Integer lenD = 	(hit.getFields().get("getDOCLENGTH").getValue());
					                //	System.out.println("lenD:"+lenD);
					                  //	System.out.println( DocNo);
					                  	
					                  	 Float  OkapiBM25 = (float) (Math.log((48170+0.5)/(DFt + 0.5))*((TFWD+(k_1*TFWD))/(TFWD+k_1*((1-b)+b*(lenD/avgLength)))));
					                  	
					            
					             //  OkapiTFD = OkapiTFD + OkapiTFWD;
					                  	OkapiBM25MAP.put(DocID, 
					                  			OkapiBM25MAP.get(DocID) == null? OkapiBM25 : OkapiBM25MAP.get(DocID) + OkapiBM25);	
			                    	 	
					                  	
					                   }
					    		    System.out.println("1000 dpone");
					    		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
					    		    //Break condition: No hits are returned
					    		    if (scrollResp.getHits().getHits().length == 0) {
					    		        break;
					    		    }
					    		}
					    		
				                    	 }
				                    	 
				                         SortedMap = sortHM(OkapiBM25MAP);
								         //op sorted map
								        // int count = 0;
								         int rank = 0;
										  
										    for(Map.Entry m1:SortedMap.entrySet())
											{  
											
											if(rank < 1000){
												rank = rank + 1;
												
												writer.println(m1.getKey());
												System.out.println(m1.getKey()+"\t"+m1.getValue());
												
												
												
											}
											else 
												
												break;
												
											}
										    SortedMap.clear();
										    OkapiBM25MAP.clear();
			    	
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