package anvitaA5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class CreateQrel {
	public static void main(String a[]) throws  ExecutionException, IOException
	{
		 LinkedHashMap<String, LinkedHashSet<MyTrecData>> qreldataA = new LinkedHashMap<String, LinkedHashSet<MyTrecData>>();
		 
		 File fqrA = new File("/Users/anvitasurapaneni/Downloads/AnvitaQrel.txt");
		 BufferedReader brA= new BufferedReader(new InputStreamReader(new FileInputStream(fqrA)));
		 String line1;
		 //trec eval
		 while( (line1 = brA.readLine()) != null){
			 String[] strSp = line1.split("\\s+");
			 String qno = strSp[0];
			 String url = strSp[2];
			 String auth = strSp[1];
			 
			 float rel = Float.parseFloat(strSp[3]);
			 
			 MyTrecData mt = new MyTrecData(url, auth, rel);
			 
			 
			
			 
			 if(qreldataA.get(qno) == null){
				 LinkedHashSet<MyTrecData> ls = new LinkedHashSet<MyTrecData>();
				 ls.add(mt);
				// System.out.println(mt.url);
				 qreldataA.put(qno, ls);
			 }
			 else{
				 LinkedHashSet<MyTrecData> ls1 = new LinkedHashSet<MyTrecData>();
				 ls1 = qreldataA.get(qno);
				
				 ls1.add(mt);
				 
				// System.out.println(mt.url);
				 qreldataA.put(qno, ls1);
			 }
			 
		 }
		 
		 
	 LinkedHashMap<String, LinkedHashSet<MyTrecData>> qreldataSa = new LinkedHashMap<String, LinkedHashSet<MyTrecData>>();
		 
		 File fqrSa = new File("/Users/anvitasurapaneni/Downloads/SanjanaQrel.txt");
		 BufferedReader brSa= new BufferedReader(new InputStreamReader(new FileInputStream(fqrSa)));
		 
		 //trec eval
		 while( (line1 = brSa.readLine()) != null){
			 String[] strSp = line1.split("\\s+");
			 String qno = strSp[0];
			 String url = strSp[2];
			 String auth = strSp[1];
			 
			 float rel = Float.parseFloat(strSp[3]);
			 
			 MyTrecData mt = new MyTrecData(url, auth, rel);
			 
			 
			
			 
			 if(qreldataSa.get(qno) == null){
				 LinkedHashSet<MyTrecData> ls = new LinkedHashSet<MyTrecData>();
				 ls.add(mt);
				// System.out.println(mt.url);
				 qreldataSa.put(qno, ls);
			 }
			 else{
				 LinkedHashSet<MyTrecData> ls1 = new LinkedHashSet<MyTrecData>();
				 ls1 = qreldataSa.get(qno);
				
				 ls1.add(mt);
				 
				// System.out.println(mt.url);
				 qreldataSa.put(qno, ls1);
			 }
			 
		 }
		 
		 
	 LinkedHashMap<String, LinkedHashSet<MyTrecData>> qreldataSo = new LinkedHashMap<String, LinkedHashSet<MyTrecData>>();
		 
		 File fqrSo = new File("/Users/anvitasurapaneni/Downloads/soumya_qrels-151005.txt");
		 BufferedReader brSo= new BufferedReader(new InputStreamReader(new FileInputStream(fqrSo)));
	
		 //trec eval
		 while( (line1 = brSo.readLine()) != null){
			 String[] strSp = line1.split("\\s+");
			 String qno = strSp[0];
			 String url = strSp[2];
			 String auth = strSp[1];
			 
			 float rel = Float.parseFloat(strSp[3]);
			 
			 MyTrecData mt = new MyTrecData(url, auth, rel);
			 
			 
			
			 
			 if(qreldataSo.get(qno) == null){
				 LinkedHashSet<MyTrecData> ls = new LinkedHashSet<MyTrecData>();
				 ls.add(mt);
				// System.out.println(mt.url);
				 qreldataSo.put(qno, ls);
			 }
			 else{
				 LinkedHashSet<MyTrecData> ls1 = new LinkedHashSet<MyTrecData>();
				 ls1 = qreldataSo.get(qno);
				
				 ls1.add(mt);
				 
				// System.out.println(mt.url);
				 qreldataSo.put(qno, ls1);
			 }
			 
		 }
		 
	 LinkedHashMap<String, LinkedHashSet<MyTrecData>> qreldataMo = new LinkedHashMap<String, LinkedHashSet<MyTrecData>>();
		 
		 File fqrMo = new File("/Users/anvitasurapaneni/Downloads/Mohsen_eval.txt");
		 BufferedReader br1= new BufferedReader(new InputStreamReader(new FileInputStream(fqrMo)));
		
		 //trec eval
		 while( (line1 = br1.readLine()) != null){
			 String[] strSp = line1.split("\\s+");
			 String qno = strSp[0];
			 String url = strSp[2];
			 String auth = strSp[1];
			 
			 float rel = Float.parseFloat(strSp[3]);
			 
			 MyTrecData mt = new MyTrecData(url, auth, rel);
			 
			 
			
			 
			 if(qreldataMo.get(qno) == null){
				 LinkedHashSet<MyTrecData> ls = new LinkedHashSet<MyTrecData>();
				 ls.add(mt);
				// System.out.println(mt.url);
				 qreldataMo.put(qno, ls);
			 }
			 else{
				 LinkedHashSet<MyTrecData> ls1 = new LinkedHashSet<MyTrecData>();
				 ls1 = qreldataMo.get(qno);
				
				 ls1.add(mt);
				 
				// System.out.println(mt.url);
				 qreldataMo.put(qno, ls1);
			 }
			 
		 }
		 
		 
		 //merge code
		 LinkedHashMap<String, LinkedHashSet<MyTrecData>> qreldataFi = new LinkedHashMap<String, LinkedHashSet<MyTrecData>>();
			
		 for(Map.Entry m:qreldataA.entrySet()){
		
		//	 System.out.println(m.getKey());
			 LinkedHashSet<MyTrecData> lh = (LinkedHashSet<MyTrecData>)m.getValue();
			 
				java.util.Iterator<MyTrecData> itrB = lh.iterator();
	 			
	 			while(itrB.hasNext()){
	 				MyTrecData trd = itrB.next();
	 				Float rm = getRelavence(trd.url, qreldataMo.get(m.getKey()));
	 				Float rsa = getRelavence(trd.url, qreldataSa.get(m.getKey()));
	 				Float rso = getRelavence(trd.url, qreldataSo.get(m.getKey()));
	 				Float ran = trd.relevance;
	 				float relT = (rm + rsa + rso + ran)/4;
	 				float rel;
	 				if(relT < 1){
	 					rel = 0;
	 				}
	 				else{
	 					rel = 1;
	 				}
	 				MyTrecData mt = new MyTrecData(trd.url, "ASoSaMo", rel);
	 				
	 				 if(qreldataFi.get(m.getKey()) == null){
	 					 LinkedHashSet<MyTrecData> ls = new LinkedHashSet<MyTrecData>();
	 					 ls.add(mt);
	 					// System.out.println(mt.url);
	 					qreldataFi.put((String)m.getKey(), ls);
	 				 }
	 				 else{
	 					 LinkedHashSet<MyTrecData> ls1 = new LinkedHashSet<MyTrecData>();
	 					 ls1 = qreldataFi.get(m.getKey());
	 					
	 					 ls1.add(mt);
	 					 
	 					// System.out.println(mt.url);
	 					qreldataFi.put((String)m.getKey(), ls1);
	 			}
			


	 			}
		 }
		 //checking
		 for(Map.Entry m:qreldataFi.entrySet()){
				
					 System.out.println(m.getKey());
					 LinkedHashSet<MyTrecData> lh = (LinkedHashSet<MyTrecData>)m.getValue();
					 
						System.out.println(lh.size());
				 }
		 //printing
		 PrintWriter writer = new PrintWriter("AFinalQrel.txt", "UTF-8");
		 for(Map.Entry m:qreldataFi.entrySet()){
				
				//	 System.out.println(m.getKey());
					 LinkedHashSet<MyTrecData> lh = (LinkedHashSet<MyTrecData>)m.getValue();
					 
						java.util.Iterator<MyTrecData> itrB = lh.iterator();
			 			
			 			while(itrB.hasNext()){
			 				MyTrecData trd = itrB.next();
			 				writer.println(m.getKey()+" "+trd.author+" "+trd.url+" "+trd.relevance);
			 			}
				 }
		 writer.close();
	}

	private static Float getRelavence(String url, LinkedHashSet<MyTrecData> TrecForQuery) {
		float res = 0;
		if(TrecForQuery != null){
		for(MyTrecData mt: TrecForQuery){
			
			if (mt.url.equals(url)){
				//System.out.println(url+":matched:"+mt.url);
				res = mt.relevance;
			}
		}
		}
		  return res;
	}
}
