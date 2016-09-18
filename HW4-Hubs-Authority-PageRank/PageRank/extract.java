package Anvita.A4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

public class extract {
	public static ReturnData  getData() throws FileNotFoundException, UnsupportedEncodingException
	{
		
		PrintWriter writer = new PrintWriter("A4_outcnt.txt", "UTF-8");
		
		HashSet<String> P = new HashSet<String>();

		
		Set<String> S = new HashSet<String>();
	//	S.add("E");
		HashMap<String,HashSet<String>> MP = new  HashMap<String,HashSet<String>>();
		HashMap<String,HashSet<String>> outlinkM = new  HashMap<String,HashSet<String>>();
	
		HashMap<String,Double> LQ = new  HashMap<String,Double>();
		
		HashMap<String,HashSet<String>> LQtemp = new  HashMap<String,HashSet<String>>();
		 
	
	//	 File f1 = new File("/Users/anvitasurapaneni/Downloads/temp.txt");
	//	 File f1 = new File("/Users/anvitasurapaneni/Downloads/wt2g_inlinks-2.txt");
		 File f1 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/MergedInlinksFile.txt");
		 BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f1)));
		 String line;
	try {
		while( (line = br.readLine()) != null){
			//System.out.println(line);
			String[] links = line.split("\t");
			//System.out.println(links.length);
			
			String Key =links[0];
			P.add(Key);
		//	System.out.println(Key);
			HashSet<String> inlinks = new HashSet<String>();
		//	if(links.length > 1){
			
			HashSet<String> outlink = new HashSet<String>();
			for(int k=1; k< links.length; k++){
				//System.out.println(links[k]);
				HashSet<String> ol = new HashSet<String>();
				P.add(links[k]);
					inlinks.add(links[k]);
					if(outlinkM.get(links[k]) == null){
					 ol = new HashSet<String>();
					ol.add(Key);
					}
					else{
						 ol = outlinkM.get(links[k]);
						 ol.add(Key);
					}
					outlinkM.put(links[k], ol);
					LQ.put(links[k], LQ.get(links[k]) == null? 1 : LQ.get(links[k]) + 1);
				
				
				}
			MP.put(Key, inlinks);
		//	}
			
		 }
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
		S.addAll(P);
	for(Entry<String, Double> m:LQ.entrySet()){
		
		String s = m.getKey();
		S.remove(s);
					
}

	
	for(Entry<String, Double> m:LQ.entrySet()){
		
	//	System.out.println(m.getKey()+":"+m.getValue());
		writer.println(m.getKey()+":"+m.getValue());
					
}
	
//	for(Entry<String, HashSet<String>> m:outlinkM.entrySet()){
//		
//		System.out.println(m.getKey()+":"+m.getValue());
//		writer.println(m.getKey()+":"+m.getValue());
//					
//}
	
	
	ReturnData  mydata = new ReturnData(MP, outlinkM, P, S);
	return mydata;
	
	


	}
	
	
}
