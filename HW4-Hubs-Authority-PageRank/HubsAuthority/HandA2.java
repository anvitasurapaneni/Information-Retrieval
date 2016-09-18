package HitsCrawl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class HandA2 {
	public static void main(String a[]) throws UnknownHostException, FileNotFoundException, UnsupportedEncodingException {
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","anv").build();

	    Client client = TransportClient.builder().settings(settings).build()
	    		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
	    
	    int con = 0;
	    
	    HashMap<String, Double> Hub = new HashMap<String,Double>();
	    HashMap<String, Double> Authority = new HashMap<String,Double>();
	    
	    PrintWriter writerH = new PrintWriter("A4_Hubs.txt", "UTF-8");
	    PrintWriter writerA = new PrintWriter("A4_Authority.txt", "UTF-8");
	    
	    File f11 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/A4_RootSet.txt");
		 BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(f11)));
		 String line1;
		 boolean converge = false;
		//Initialization
			try {
				while( (line1 = br1.readLine()) != null){
					Hub.put(line1.trim().toLowerCase(), (double) 1);
					Authority.put(line1.trim().toLowerCase(), (double) 1);
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			//System.out.println("initialized");
			
			while(con < 31){
				  HashMap<String, Double> oauthmap = Authority;
				  HashMap<String, Double> ohubmap = Hub;
			//	System.out.println("creating old auth and hub");
				
			
				for(Map.Entry m:Hub.entrySet()){
					// get out links and add its authority score
					double hub = 0;
					GetResponse response = client.prepareGet("four", "document",(String) m.getKey())
							.setFields("in_links","out_links").execute().actionGet(); 
					
					if(response.isExists()){
					 				String outlinks2 = (String)response.getField("out_links").getValue();
					 				String outlinks[] = 	outlinks2.split("\t");
					 				for(String out: outlinks){
					 					if(oauthmap.get(out) != null){
					 						//System.out.println("updating hub");
					 						hub = hub + oauthmap.get(out) ;
					 					}
					 				}
					 				
					}
					if(hub != 0){
					//	System.out.println(hub);
						Hub.put((String) m.getKey(), hub);
					}
				} 
				
				System.out.println("Hub UPdated");
		
				  for(Map.Entry m:Authority.entrySet()){
						// get in links and add its hub score
						double auth = 0;
						GetResponse response = client.prepareGet("four", "document",(String) m.getKey())
								.setFields("in_links","out_links").execute().actionGet(); 
						
						if(response.isExists()){
							//System.out.println("response exists");
						 			//	System.out.println(key);
						 				String inlinks2 = (String)response.getField("in_links").getValue();
						 				String inlinks[] = 	inlinks2.split("\t");
						 				for(String in: inlinks){
						 					if(ohubmap.get(in) != null){
						 					//	System.out.println("updating auth");
						 						auth = auth + ohubmap.get(in) ;
						 					}
						 				}
						 				
						}
						if(auth != 0){
						//	System.out.println(auth);
							Authority.put((String) m.getKey(), auth);
						}
					} 
				
				  System.out.println("Authority UPdated");	
//				
			
//			for(Map.Entry m:oauthmap.entrySet()){
//				System.out.println(m.getValue());
//			} 
//		
			//normailze authority
			double nauth = 0;
			for(Map.Entry m:Authority.entrySet()){
				nauth = (double) (nauth + Math.pow((Double)m.getValue(), 2)) ;
			}
			
			nauth =  Math.sqrt(nauth);
			
			for(Map.Entry m1:Authority.entrySet()){
				Authority.put((String) m1.getKey(),(double)m1.getValue()/ nauth);
			}
			
			//normailze Hub
			double nhub = 0;
			for(Map.Entry m2:Hub.entrySet()){
				nhub =  (nhub + Math.pow((Double)m2.getValue(), 2)) ;
			}
			nhub = (double) Math.sqrt(nhub);
			
			for(Map.Entry m3:Hub.entrySet()){
				Hub.put((String) m3.getKey(),(double)m3.getValue()/ nhub);

			}
			
			

			if(convergeCheck(oauthmap, Hub, ohubmap, Authority) == true ){
				con = con + 1;
				converge = true;
			}
			else {con = 0;
			converge = false;}
			
			System.out.println("nauth:"+nauth+"  nhub:"+nhub+"  Convergence:"+converge);
			
			if(con == 30){
				System.out.println("printing to files");
				double hubsum = 0;
				for(Map.Entry m1:Hub.entrySet())
				{  
					hubsum = (double)hubsum + (double)m1.getValue();
				}
				//System.out.println("hub sum: "+hubsum);
				
				double authsum = 0;
				for(Map.Entry m1:Authority.entrySet())
				{  
					authsum = (double)authsum + (double)m1.getValue();
				}
				//System.out.println("authority sum: "+authsum);
				
				 HashMap<String, Double> SortedMapH = sortHM(Hub);
		         int rank1 = 0;
				  
				    for(Map.Entry m1:SortedMapH.entrySet())
					{  
					
					if(rank1 < 500){
						rank1 = rank1 + 1;
						
						writerH.println(m1.getKey()+"\t"+m1.getValue());
						
					}
					else 
						
						break;
						
					}
				    
				
					 HashMap<String, Double> SortedMapA = sortHM(Authority);
			         int rank = 0;
					  
					    for(Map.Entry m2:SortedMapA.entrySet())
						{  
						
						if(rank < 500){
							rank = rank + 1;
							
							writerA.println(m2.getKey()+"\t"+m2.getValue());
							
						}
						else 
							
							break;
							
						}
			}
			
			}
			
			
			
			
		
				    

writerA.close();    	
writerH.close();   
			
	}
	
	
	private static HashMap<String, Double> updateAuth (HashMap<String, Double> Authority1, HashMap<String, Double> Hub1) throws UnknownHostException
	{
		
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","anv").build();

	    Client client = TransportClient.builder().settings(settings).build()
	    		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
		//update authority
		HashMap<String, Double> ohubmap = Hub1;
		HashMap<String, Double> Authority = Authority1;
		
    
		//System.out.println("Authority UPdated");
		return Authority;
	}
	
	
	private static HashMap<String, Double> updateHub (HashMap<String, Double> Authority1, HashMap<String, Double> Hub1) throws UnknownHostException
	{
		
		Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
				.put("cluster.name","anv").build();

	    Client client = TransportClient.builder().settings(settings).build()
	    		.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
		//update authority
		HashMap<String, Double> Hub = Hub1;
		HashMap<String, Double> oauthmap = Authority1;
		
    
		return Hub;
	}

	private static boolean convergeCheck(HashMap<String, Double> oauth, HashMap<String, Double> nauth,
			HashMap<String, Double> ohub, HashMap<String, Double> nhub) {
		
		for(Map.Entry m:oauth.entrySet()){
	//	System.out.println(m.getValue());
		} 

		
		double authscore = 0;
		double hubscore = 0;
		double authc = 0;
		double hubc = 0;
		for(Map.Entry m:oauth.entrySet()){
			double diff = (double)m.getValue() - (double)nauth.get(m.getKey());
			//diff = Math.abs(diff);
			authc = authc +1;
			authscore = authscore + diff;
		} 

		for(Map.Entry m:ohub.entrySet()){
			double diff = (double)m.getValue() - (double)nhub.get(m.getKey());
			//diff = Math.abs(diff);
			hubc = hubc + 1;
			hubscore = hubscore + diff;
		} 
		
		double h =  Math.abs(hubscore/ hubc);
		double a =  Math.abs(authscore/ authc);
		
		double r = Math.abs(h - a);
System.out.printf("hubbscore:%.15f authscore:%.15f DIFF=%.15f%n",h,a, Math.abs(h - a));
System.out.println("hub"+h+"auth"+a);
		//if((h  <= 0.0001) && (a <= 0.0001))
//DecimalFormat df = new DecimalFormat("#.####");
//String res = df.format(r);
//System.out.println(res);
if(Math.abs(h - a) <= 0.000000000000001)
		{
			return true;
		}
		else	return false;
	}
	
private static HashMap<String, Double> sortHM(Map<String, Double> aMap) {
        
        Set<Entry<String,Double>> mapEntries = aMap.entrySet();
       List<Entry<String,Double>> aList = new LinkedList<Entry<String,Double>>(mapEntries);

        Collections.sort(aList, new Comparator<Entry<String,Double>>() {

            
            public int compare(Entry<String, Double> ele1,
                    Entry<String, Double> ele2) {
                
                return ele2.getValue().compareTo(ele1.getValue());
            }
        });
       
        Map<String,Double> aMap2 = new LinkedHashMap<String, Double>();
        for(Entry<String,Double> entry: aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }
      
            return (HashMap<String, Double>) aMap2;
        }
	
}
