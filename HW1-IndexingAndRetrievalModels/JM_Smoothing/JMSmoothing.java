package Anvita.CS6200;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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


public class JMSmoothing{
	public static void main(String[] args) throws IOException{

			File IPfolder = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/ap89_collection");
			File[] listOfFiles = IPfolder.listFiles();
			Map<String,String> hmDocs=new HashMap<String,String>(); 
			
			PrintWriter writer = new PrintWriter("JMSmoothing.txt", "UTF-8");
			Float Vocabulary=(float) 177992;
			Float totallen=(float) 247.0;

			long sumttf=0;
			
			
			Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
					.put("cluster.name","elasticsearch").build();
		
	        Client client = TransportClient.builder().settings(settings).build()
	        		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
	        
	        BulkRequestBuilder br=client.prepareBulk(); 
	        int count =0;
	        
			for (int i = 0; i < listOfFiles.length; i++) {
//begin creating hash map	
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
				 
				hmDocs.put(docNoTemp,textTemp);
				
				 }
//end creating hash map		
			}
			
File QueryFile = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/query_desc.51-100.short.txt");
			
BufferedReader br1 = new BufferedReader(new FileReader(QueryFile));   

HashMap<String,Float> LaplaceSmoothing = new HashMap<String,Float>();
 HashMap<String, Float> SortedMap = new HashMap<String, Float>();
 String FullQueryTemp = "";
while ((FullQueryTemp = br1.readLine()) != null)  
{
	HashMap<String,Float> JMSmoothing = new HashMap<String,Float>();
	int toatlFreq_flag = 0;
	
	if (FullQueryTemp.length() <= 3)
		break;


	 
	 String queryNo = FullQueryTemp.substring(0, 3);
 	queryNo = queryNo.replace(".", "");
 	queryNo = queryNo.trim();
 	
 
 	
 	
	
 	String[] queryWords= FullQueryTemp.substring(5, FullQueryTemp.length()).split("\\s+");
//	 for(String WordQueryTemp: queryWords)
// 	 {
// 		 System.out.println(WordQueryTemp);
// 	 }
//                	 
                	
                	
                	  for(Map.Entry m:hmDocs.entrySet())

                	     { 
                		  Float JM=(float) 0.0;
                		  
                		  if(m.getValue().toString().length()==1||m.getValue().toString().length()==0)
      					{
      						continue;
      					}
                		  sumttf=13944737;
                	    String key = (String) m.getKey();
                	    
                	    for(String WordQueryTemp: queryWords)
                	    {
                	    	Integer tfscore=0;
                	    	 WordQueryTemp = WordQueryTemp.replace(".", " ");
                	    	 WordQueryTemp = WordQueryTemp.trim();
                	//   System.out.println(WordQueryTemp);
                	//   System.out.println(key);
                	    	final Map<String, Object> params = new HashMap<String, Object>();
                        params.put("term", WordQueryTemp);
                        params.put("field", "text");

                 SearchResponse response = client.prepareSearch("ap_dataset")
                                .setTypes("document")
                                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                                .setQuery(QueryBuilders.termQuery("docno", key))
                                .setFrom(0)
                                .setSize(3)
                                .addScriptField("getDOCLENGTH", (new Script("doc['text'].values.size()")))
                                .addScriptField("getTF", (new Script("getTF", 
                                        ScriptType.INDEXED, "groovy", params)))
                                .get();
         
               //  System.out.println(response);
               //  Float LMTemp = (float) 0;
                 String DocID ="";
                 
                 for ( SearchHit hit : response.getHits().hits())
                 { 
                 	 DocID = hit.getId();
                 //	System.out.println(DocID);
                 	tfscore =  (hit.getFields().get("getTF").getValue());
                 	Integer lenD = (hit.getFields().get("getDOCLENGTH").getValue());
                 //	System.out.println(TFWD);
                 	Float TF;
                 	if((response.getHits().totalHits()==0))
					{
						TF = 0.00001F;
					}
					else
					{
						TF = response.getHits().maxScore();
					}
                     
	Float term1 = (float) ((0.6*tfscore/lenD)+(0.4*(TF-tfscore)/(sumttf-lenD)));
	JM=(float) (JM+(Math.log(term1)));
 }
               
                 JMSmoothing.put(DocID, 
                		 (float) (JMSmoothing.get(DocID) == null? JM :  JMSmoothing.get(DocID)+ JM) );	
                 
               
                	     }
                	    
                	  
                	     }
                	 
              
 	
     SortedMap = sortHM(JMSmoothing);
     JMSmoothing.clear();
     int rank = 0;
	  
	    for(Map.Entry sm:SortedMap.entrySet())
		{  
		
		if(rank < 1000)
		{
			rank = rank + 1;
			writer.println(queryNo+"  Q0  "+sm.getKey()+"  "+rank+"  "+sm.getValue()+"  JMSmoothing  ");
		}
		else break;
			
		}
     
 	LaplaceSmoothing.clear();
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
}


