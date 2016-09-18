package Anvita.A4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class A4_2 {
	public static int convCnt = 0;
	public static void main(String a[]) throws FileNotFoundException, UnsupportedEncodingException
	{
		PrintWriter writer = new PrintWriter("A4_PR.txt", "UTF-8");
		
		double PrevPerp = 0;
		double CurrPerp = 0;
		ReturnData data = extract.getData();
		
		
		HashSet<String> P = data.P;

		
		Set<String> S = data.S;
	//	S.add("E");
		HashMap<String,HashSet<String>> M = data.MP;
	
		HashMap<String,HashSet<String>> L = data.LQ;
		
		 double n = ((double)P.size());
		 System.out.println(n);
		 System.out.println(M.size());
			System.out.println(L.size());
			System.out.println(S.size());
		
		
		HashMap<String,Double> PR = new  HashMap<String,Double>();
		HashMap<String,Double> NewPR = new  HashMap<String,Double>();
		double d = (double) 0.85;
		int itr = 0;
		
		for(String p : P){
			PR.put(p, ((double)1/n));
		}
	//	System.out.println("perp "+Perplexity(PR));
		CurrPerp = Perplexity(PR);
		while(convCnt <= 4){
			
			itr = itr + 1;
			System.out.println(itr+"   ITERATION  "+convCnt+"   "+CurrPerp);
			
			
			
			PrevPerp = CurrPerp;
			Double sinkPR = (double)0;
			for(String p: S){
				sinkPR += (double)PR.get(p);
			}
			
			for(String p: P){
				NewPR.put(p, ((double)(1.0-d)/n));
				Double spread = (double)(d*sinkPR/n);
				NewPR.put(p,((double) (NewPR.get(p) + spread)));
				
				if(M.get(p) != null){
				for(String q: M.get(p)){
					Double val = ((double)d*PR.get(q)/L.get(q).size());
					NewPR.put(p, ((double)NewPR.get(p)+val));
				//	System.out.println("bj");
				}
				}
//				else{
//					//System.out.println("set is null");
//				}
			}
			
			for(String p:P){
				PR.put(p, ((double)NewPR.get(p)));
			}
			
		
			
			CurrPerp = Perplexity(PR);
			int i = (int)PrevPerp;
			int j = (int)CurrPerp;
			PerpCnt( i , j );
			
		}
		
		
		double sum = 0;
		 for(Map.Entry m1:PR.entrySet()){
		    sum = (double)sum + (double)m1.getValue();	
		   }
		 System.out.println("summation :"+sum);
		
		HashMap<String, Double> SortedMap = sortHM(PR);
		int count = 500;
		  
		    for(Map.Entry m1:SortedMap.entrySet()){
		    	count --;
		    	if(count> 0){
		    	//	System.out.println(m1.getKey()+" "+m1.getValue());
		    		writer.println(m1.getKey()+" "+m1.getValue());
		    	}
		    }
		    
		    writer.close();
	}
	
	private static double Perplexity(HashMap<String,Double> PR) {
		double entropy = 0;
		for(Entry<String, Double> m:PR.entrySet()){
			entropy = (double) (entropy + (m.getValue() * (Math.log(m.getValue())/Math.log(2))));
		//	System.out.println(m.getValue());
		}
	//	System.out.println(perpSum);
		double perp = (double)Math.pow(2, -entropy);
		return perp;
		//return 112.12;
		
	}
	
	private static void PerpCnt(int PrevPerp, int CurrPerp ) {
		if(PrevPerp == CurrPerp){
			convCnt = convCnt + 1;
		}
		else convCnt = 0;
		
		
		
	}
	
	private static HashMap<String,Double> sortHM(Map<String,Double> aMap) {
        
        Set<Entry<String,Double>> mapEntries = aMap.entrySet();
       List<Entry<String,Double>> aList = new LinkedList<Entry<String,Double>>(mapEntries);

        Collections.sort(aList, new Comparator<Entry<String,Double>>() {

            
            public int compare(Entry<String,Double> ele1,
                    Entry<String,Double> ele2) {
                
                return ele2.getValue().compareTo(ele1.getValue());
            }
        });
       
        Map<String,Double> aMap2 = new LinkedHashMap<String,Double>();
        for(Entry<String,Double> entry: aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }
      
            return (HashMap<String,Double>) aMap2;
        }
}
