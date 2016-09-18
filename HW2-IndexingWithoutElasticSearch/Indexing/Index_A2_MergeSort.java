		package Anvita.CS6200;
		
		import org.elasticsearch.action.bulk.BulkRequestBuilder;
		import org.elasticsearch.action.bulk.BulkResponse;
		import org.elasticsearch.client.Client;
		import org.elasticsearch.client.transport.TransportClient;
		import org.elasticsearch.common.settings.Settings;
		import org.elasticsearch.common.transport.InetSocketTransportAddress;
		
		import java.io.BufferedReader;
		import java.io.File;
		import java.io.FileInputStream;
		import java.io.FileNotFoundException;
		import java.io.FileReader;
		import java.net.InetAddress;
		import java.util.ArrayList;
		import java.util.Collections;
		import java.util.Comparator;
		import java.util.HashMap;
		import java.util.LinkedHashMap;
		import java.util.LinkedList;
		import java.util.List;
		import java.util.Map;
		import java.util.Set;
		import java.util.Map.Entry;
		import java.util.regex.Matcher;
		import java.util.regex.Pattern;
		import java.io.IOException;
		import java.io.InputStreamReader;
		import java.io.PrintWriter;
		import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.FileUtils;
		
		
public class Index_A2_MergeSort {
		
		public static void main(String[] args) throws IOException
	{
			String line;
			
			File file1 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/InvInd1.txt");
			File file2 = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/InvInd2.txt");
			
			
			File file12 =	 merge2II(file1, file2);
			BufferedReader br1 = new BufferedReader(new FileReader(file12));
			PrintWriter writer1 = new PrintWriter("final.txt", "UTF-8");
			while (( line = br1.readLine()) != null)  {
			
				writer1.println(line);
			}
			br1.close();
			writer1.close();
			
			String filename = "/Users/anvitasurapaneni/Documents/workspace/CS6200/InvInd*.txt";
	for(int i=3; i<= 61; i++){
		
		StringBuilder sb = new StringBuilder();
		sb.append("");
		sb.append(i);
		String fileNo = sb.toString();
		
		String fn = filename.replace("*", fileNo);
		
		
		File fileFinal = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/final.txt");
		BufferedReader br3 = new BufferedReader(new FileReader(fileFinal));
		PrintWriter writer3 = new PrintWriter("finalTemp.txt", "UTF-8");
		while (( line = br3.readLine()) != null)  {
			
			writer3.println(line);
		}
		br3.close();
		writer3.close();
		
		File fileFinalTemp = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/finalTemp.txt");
		File fileFN = new File(fn);
		
		System.out.println("adding file :"+i);
		File fileM =	 merge2II(fileFN, fileFinalTemp);
		
		BufferedReader br2 = new BufferedReader(new FileReader(fileM));
		PrintWriter writer2 = new PrintWriter("final.txt", "UTF-8");
		while (( line = br2.readLine()) != null)  {
			
			writer2.println(line);
		}
		br2.close();
		writer2.close();
		
	}
		
		}
		
private static File merge2II(File II1, File II2) {
	

	 HashMap<Integer,ArrayList<Integer>> Offset1 = createCatalog(II1);
	 HashMap<Integer,ArrayList<Integer>> Offset2 = createCatalog(II2);
	 
	
	 PrintWriter writer = null;
	try {
		writer = new PrintWriter("mergedOptm.txt", "UTF-8");
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (UnsupportedEncodingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	 
	 for(Map.Entry m:Offset1.entrySet()){
		 
		 int termId = (Integer) m.getKey();
		 if(Offset2.get(termId) != null){
			 
			 String str1 = getLineOfFileWithtermIdAndCatalog( Offset1, termId , II1);
			  String str2 = getLineOfFileWithtermIdAndCatalog( Offset2, termId , II2);
			  String str12 = MergeSortOf2Lines(str1, str2);
			  str12 = str12.trim();
			  str12 = str12.replace(" ", "").replace("[", "").replace("]", "");
			  writer.println(str12); 
		}
		 else{
			 
			 String str1 = getLineOfFileWithtermIdAndCatalog( Offset1, termId , II1);
			 str1 = str1.trim();
			 str1 = str1.replace(" ", "").replace("[", "").replace("]", "");
			 writer.println(str1);
				
		 }
		
	}
	 
	 
	 for(Map.Entry m:Offset2.entrySet()){
		 
		 int termId = (Integer) m.getKey();
		 
		 if(Offset1.get(termId) != null){
			 continue;
		 }
		 else{
			 String str2 = getLineOfFileWithtermIdAndCatalog( Offset2, termId , II2);
			 str2 = str2.trim();
			 str2 = str2.replace(" ", "").replace("[", "").replace("]", "");
			 writer.println(str2);
				
		 }
		 
	}
	 
	
			    writer.close();
			    Offset2.clear();
			    Offset1.clear();
			    File file = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/mergedOptm.txt");
			    return(file);  
			
	
	}
		
		
		
		

private static String MergeSortOf2Lines(String str1, String str2) {
	String str12 = "";
	str1 = str1.trim();
	str2 = str2.trim();
	
	String[] IdAndToken1 =	str1.split(":");
	
	String Id1 = (String)(IdAndToken1[0]);
	
	int termId1 = Integer.parseInt(Id1);
	String token1 = IdAndToken1[1];
	
	String[] tuples1 =	 token1.split(";");
	int len1 = tuples1.length;
	
	
	
	
	String[] IdAndToken2 =	str2.split(":");
	
	String Id2 = (String)(IdAndToken2[0]);
	
	int termId2 = Integer.parseInt(Id2);
	String token2 = IdAndToken2[1];

	String[] tuples2 =	 token2.split(";");
	int len2 = tuples2.length;
	
	
	
	
	if(termId1 == termId2){
		str12 = str12 + termId2 + ":";
		
		String tuple1 = "";
		String tuple2 = "";
		
		
		  int i = 0, j = 0;
		    while (i < len1 && j < len2)
		    {
		    	
		    	tuple1 = tuples1[i];
				tuple2 = tuples2[j];
				int Tf1 = GetTfOfTuple(tuple1);
				int Tf2 = GetTfOfTuple(tuple2);
				
		        if (Tf1 < Tf2)
		        {
		        	str12 = str12 + tuple1 + ";";
		            i++;
		        }
		        else
		        {
		        	str12 = str12 + tuple2 + ";";
		            j++;
		        }
		        
		    }

		    while (i < len1)
		    {
		    	tuple1 = tuples1[i];
		    	str12 = str12 + tuple1 + ";";
		        i++;
		        
		    }

		    while (j < len2)
		    {
		    	tuple2 = tuples2[j];
		    	str12 = str12 + tuple2 + ";";
		        j++;
		        
		    }
		
		// str12 = str12 + "\n";
	}
		
else {
		System.out.println("keys of 2 lines do not match");
	}
	

	return str12;
}

		private static Integer GetTfOfTuple(String tuple) {
			
			
			String[] DocIdTfLop =	tuple.split("_");
			String Tf = DocIdTfLop[1];
			int tfTupple = Integer.parseInt(Tf);
			return tfTupple;
			
		}
		
		
		private static HashMap<String, Integer> sortHM(Map<String, Integer> aMap) {
			
	        Set<Entry<String,Integer>> mapEntries = aMap.entrySet();
	
	       List<Entry<String,Integer>> aList = new LinkedList<Entry<String,Integer>>(mapEntries);
	
	 Collections.sort(aList, new Comparator<Entry<String,Integer>>() {
	
	 public int compare(Entry<String, Integer> ele1,
	
	                    Entry<String, Integer> ele2) {
	
	                return ele2.getValue().compareTo(ele1.getValue());
	
	            }
	
	        });
	
	       Map<String,Integer> aMap2 = new LinkedHashMap<String, Integer>();
	
	        for(Entry<String,Integer> entry: aList) {
	
	            aMap2.put(entry.getKey(), entry.getValue());
	 }
	  return (HashMap<String, Integer>) aMap2;
	}
		
		
		private static HashMap<Integer,ArrayList<Integer>>  createCatalog(File FileName) {
			System.out.println("creating cat");
			 HashMap<Integer,ArrayList<Integer>> Offset = new HashMap<Integer,ArrayList<Integer>>();
				
			
					BufferedReader br = null;
					try {
						br = new BufferedReader(new InputStreamReader(new FileInputStream(FileName)));
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					String line;
					int start = 0;
					int end = 0;
					try {
						while( (line = br.readLine()) != null){
							int lengthOfLine = line.length();
							end = start + lengthOfLine;
							String[] IdAndIl =	line.split(":");
							
							String key1 = (String)(IdAndIl[0]);
							int key = Integer.parseInt(key1);
							String mapping = IdAndIl[1];
							ArrayList<Integer> arraylist = new ArrayList<Integer>();
						    arraylist.add(start);
						    arraylist.add(end);

						   
						    Offset.put(key, arraylist);
						    start = end + 1;
							
						}
					} catch (NumberFormatException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("creating cat done");
					
	  return (HashMap<Integer,ArrayList<Integer>>) Offset;
	}	
		
		
		
		
		
		private static String  getLineOfFileWithtermIdAndCatalog
		( HashMap<Integer,ArrayList<Integer>> Offset, Integer termId, File iI2) 
		{
			ArrayList<Integer> arraylisttemp = Offset.get(termId);
			Integer start1 = arraylisttemp.get(0);
			Integer end1 = arraylisttemp.get(1);
			//File fileTemp = new File(FileName);
			
			RandomAccessFile randFile = null;
			int len = 0;
			byte[] string = null;
			
			
			try {
				
				randFile = new RandomAccessFile(iI2, "rw");
				 len = (int) randFile.length();
				 if(end1 >= len){
					 end1 = len;
				 }
				string = new byte[end1 - start1];
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				randFile.seek(start1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				//randFile.setLength(end1);
				randFile.readFully(string);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

			
			try {
				randFile.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
	return (new String(string));
	}	
		
		
		}
		
