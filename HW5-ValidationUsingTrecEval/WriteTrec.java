package anvitaA5;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import javax.swing.SortOrder;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortParseElement;

public class WriteTrec {
public static void main(String[] args) throws IOException, InterruptedException, ExecutionException{
	Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
			.put("cluster.name","anv").build();

    Client client = TransportClient.builder().settings(settings).build()
    		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
    
    PrintWriter writer = new PrintWriter("AnvitaQrel.txt", "UTF-8");
    HashSet<String> AllUrls = new HashSet<String>();
    int j = 0;
    int strt = 0;

	 
	  //  SearchResponse response = client.prepareSearch("four")
	    		
	    		SearchResponse scrollResp = client.prepareSearch("trec")
	       //     .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
	            .setScroll(new TimeValue(60000))
	            .setTypes("document")
	            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
	            //.setQuery(QueryBuilders.termQuery("author","ANVITA"))
	            .setQuery(QueryBuilders.matchAllQuery())
	            .setFrom(0)
	           .setSize(1000)
	           
	            .get();

	    		
	    		
	    		while (true) {

	    		    for (SearchHit hit : scrollResp.getHits().getHits()) {
	    		    	
	    		    	AllUrls.add(hit.getId());
	    		    	writer.println(hit.getSource().get("queryid").toString()+" "+hit.getSource().get("assessor").toString()+" "+
	    		    			CanonicalizeURL(hit.getSource().get("docid").toString())+" "+hit.getSource().get("grade").toString());
	    		    	}
	    		    
	    		    System.out.println("1000 dpone");
	    		    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
	    		    //Break condition: No hits are returned
	    		    if (scrollResp.getHits().getHits().length == 0) {
	    		        break;
	    		    }
	    		}
	    		
	  
	   

    
  writer.close();
  System.out.println(AllUrls.size());
	}
	
private static String  CanonicalizeURL(String url) {

String MyUrl = url;



if (MyUrl.startsWith("//"))

MyUrl = "http:" + MyUrl;


if (MyUrl.startsWith("www"))

MyUrl = "http://" + MyUrl;



int i = MyUrl.indexOf('/', 1 + MyUrl.indexOf('/', 1 + MyUrl.indexOf('/')));

if(i > 0){

String firstPart = MyUrl.substring(0, i);

String secondPart = MyUrl.substring(i);

firstPart = firstPart.toLowerCase();

secondPart = secondPart.replaceAll("/+", "/");

MyUrl = firstPart + secondPart;

}

if(MyUrl.contains("http")){

MyUrl = MyUrl.replace(":80", "");

}

if(MyUrl.contains("https")){

MyUrl = MyUrl.replace(":443", "");

}


int i1 = MyUrl.indexOf('#');

if(i1 >= 0){

MyUrl = MyUrl.substring(0, i1);

}


int i2 = MyUrl.indexOf('?');

if(i2 >= 0){

MyUrl = MyUrl.substring(0, i2);

}

MyUrl = MyUrl.trim();

return MyUrl;

}  
    

}
