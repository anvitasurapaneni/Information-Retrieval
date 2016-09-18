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

		import org.apache.commons.io.FileUtils;
		
		
public class Index_A2 {
		
		public static void main(String[] args) throws IOException
	{
			
			// doc list
						File f1 = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/doclist_new_0609.txt");
						HashMap<String, Integer>  AllDocs = new HashMap<String, Integer>();
						 BufferedReader br4 = new BufferedReader(new FileReader(f1));
						 String line8 ="";
							while (( line8 = br4.readLine()) != null)  {
								
								String[] docs =	 line8.split(" ");
								String DocNo = docs[0];
								DocNo = DocNo.trim();
								int dno = Integer.parseInt(DocNo);
								
								String DocId = docs[2];
								// DocId = DocId.trim();
								System.out.println(DocId);
								AllDocs.put(DocId, dno);
								
							}
							br4.close();
			
			
			Settings settings = Settings.settingsBuilder().put("client.transport.sniff", true)
			.put("cluster.name","elasticsearch").build();
			 Client client = TransportClient.builder().settings(settings).build()
			.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300));
			
			        
			 PrintWriter writerTID = new PrintWriter("TID.txt", "UTF-8");
			 
			 File IPfolder = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/ap89_collection");
			 File[] listOfFiles = IPfolder.listFiles();
			 int ListOfFilesLength = listOfFiles.length;
			// int ListOfFilesLength = 1;
			 
			 // My REgexp
			 
			 Pattern MY_PATTERN =
					 Pattern.compile("[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*");
			 // end My Regexp
		
		// for Stop words
		
		File StopWordsFile = new File("/Users/anvitasurapaneni/Downloads/AP_DATA/s.txt");
		BufferedReader br1 = new BufferedReader(new FileReader(StopWordsFile));  
		
		int i1 = 0 ;
		
		String StopWord = "";
		
		List<String> StopWordsList = new ArrayList<String>();
		
		StopWordsList.add("a");
		
		
		while ((StopWord = br1.readLine()) != null)  {
		
		i1 = i1 + 1;
		
		StopWord = StopWord.replace("\\", " ");
		StopWord = StopWord.trim();
		
		if(i1>=8){
		StopWordsList.add(StopWord);
		}
		
		}
	
		// end for stop words
		
	
		
		        int TermId = 0;
		
		        HashMap<String,Integer> TermIdMap = new HashMap<String,Integer>();
		        HashMap<String,Integer> DocLength = new HashMap<String,Integer>();
		
		        int i = 0;
		        int batch = 0;
		        int s = 0;
		        int e = 6;
		        int flag = 0;
		
		       while ( s < ListOfFilesLength) {
		
		    	  batch = batch + 1;
		
		    	  String filename = "InvInd"+batch+".txt";
		
		    	  System.out.println(filename);
		
		    	  HashMap<Integer,HashMap<String,Integer>> InvertedIndexTemp = new HashMap<Integer,HashMap<String,Integer>>();
		    	  HashMap<Integer,HashMap<String,ArrayList<Integer>>> InvertedIndexLstOfPos = new HashMap<Integer,HashMap<String,ArrayList<Integer>>>();
		  		
		
		    	  PrintWriter writer = new PrintWriter(filename, "UTF-8");
		
		for (i = s; i < e ; i++) {
		
		System.out.println(i);
		
		File mFile = new File(listOfFiles[i].getPath());
		
		String str = FileUtils.readFileToString(mFile);
		
		//  Extract DOC
		
		  Pattern pattern = Pattern.compile("<DOC>\\s(.+?)</DOC>", Pattern.DOTALL);
		
		  Matcher matcher = pattern.matcher(str);
		
		  HashMap<String,Integer> TermFreqWD = new HashMap<String,Integer>();
		  HashMap<String,ArrayList<Integer>> TermPosD = new HashMap<String,ArrayList<Integer>>();
		
		
		  String docNo ;
		
		while (matcher.find())
		{
		String docTemp = matcher.group(1);
		final Pattern pattern1 = Pattern.compile("<DOCNO>(.+?)</DOCNO>");
		final Matcher matcher1 = pattern1.matcher(docTemp);
		matcher1.find();
		String docNo1 = matcher1.group(1);
		
		docNo1 = docNo1.trim();
		
		int  docNo11 = AllDocs.get(docNo1);
		docNo = Integer.toString(docNo11);

		// Extract TEXT
		
		Pattern pattern2 = Pattern.compile("<TEXT>(.+?)</TEXT>", Pattern.DOTALL);
		Matcher matcher2 = pattern2.matcher(docTemp);
		
		String textForDoc ="";
		
		while (matcher2.find())
		{
		textForDoc = textForDoc.concat(matcher2.group(1));
		}
		
		 
		textForDoc = textForDoc.toLowerCase();
		
		java.util.regex.Matcher m = MY_PATTERN.matcher(textForDoc);
		
		
		
		int wordCount = 0;
		
		//caliculate term freq and term pos
		String word1;
		
		Integer pos = 0;
		while (m.find()){
			word1 = m.group(0);
			word1 = word1.trim();
		
		if(StopWordsList.contains(word1)){
		continue;
		}
		
		// for TF
		else{
			pos = pos + 1;
			
		TermFreqWD.put(word1, 
		(TermFreqWD.get(word1) == null? 1 :  TermFreqWD.get(word1)+ 1) );
		
		// for list of pos
		ArrayList<Integer> arraylist;
		if(TermPosD.get(word1) == null){
		 arraylist = new ArrayList<Integer>();
		 arraylist.add(pos); 
		}
		else{
		 arraylist = TermPosD.get(word1);
		 
		  arraylist.add(pos); 
		}
		
		TermPosD.put(word1, arraylist);
		
		}
		
		}


		
		
		
		// end caliculate term freq
		
		java.util.regex.Matcher m2 = MY_PATTERN.matcher(textForDoc);
		
		String word;
		
		while (m2.find()){
			word = m2.group(0);
		//	System.out.println(word);
		
		word = word.trim();
		
		if(StopWordsList.contains(word)){
		continue;
		}
		
		
		else{
		wordCount = wordCount + 1;
		
		if(TermIdMap.get(word) == null){
		TermId = TermId + 1;
		TermIdMap.put(word, TermId);
		}
		
		int Tid = TermIdMap.get(word);
		
		if(InvertedIndexTemp.get(Tid) == null){
			
		HashMap<String,Integer> docIDFreq = new HashMap<String,Integer>();
		HashMap<String,ArrayList<Integer>> docIDLop = new HashMap<String,ArrayList<Integer>>();
		
		docIDFreq.put(docNo, TermFreqWD.get(word));
		
		InvertedIndexTemp.put(Tid, docIDFreq);
		InvertedIndexLstOfPos.put(Tid, docIDLop);
		
		}
		
		 	if(InvertedIndexTemp.get(Tid) != null){
		
		HashMap<String,Integer> docIDFreq = InvertedIndexTemp.get(Tid);
		HashMap<String,ArrayList<Integer>> docIDLop = InvertedIndexLstOfPos.get(Tid);
		
		docIDFreq.put(docNo, TermFreqWD.get(word));
		docIDLop.put(docNo, TermPosD.get(word));
		
		InvertedIndexTemp.put(Tid,docIDFreq);
		InvertedIndexLstOfPos.put(Tid,docIDLop);
		}
		
		}
		
		}
		
		wordCount = 0;
		
		DocLength.put(docNo, pos);
		
		TermFreqWD.clear();
		TermPosD.clear();
		
		
		
		}
		
		 }
		
		
		for(Map.Entry m:InvertedIndexTemp.entrySet()){
			
			Integer keyTerm =  (Integer) m.getKey();
		
		HashMap<String,Integer> docIDFreq = InvertedIndexTemp.get(m.getKey()); 
		
		writer.print(m.getKey()+":");
		
		HashMap<String,Integer> SortedMap = new HashMap<String,Integer>();
		
		SortedMap = sortHM(docIDFreq);
		
			for(Map.Entry m1:SortedMap.entrySet()){
			
			String keyDocId = (String) m1.getKey();
			
			writer.print(m1.getKey()); 
			
			writer.print("_"); 
			
			writer.print(m1.getValue()); 
			
			writer.print("_"); 
			
			
			writer.print( InvertedIndexLstOfPos.get(keyTerm).get(keyDocId));
			
			writer.print(";"); 
			
			}
		
		writer.print("\n"); 
		
		}
		
		InvertedIndexTemp.clear();
		InvertedIndexLstOfPos.clear();
		
		    writer.close();
		
		     
		
		
		
		s = s + 6;
		
		e = e + 6;
		
		if(e >= ListOfFilesLength){
		
		e = ListOfFilesLength;
		
		}

		// for all docs ends here
	 }
		
		
		for(Map.Entry TID:TermIdMap.entrySet()){
		
		writerTID.println(TID.getKey()+":"+TID.getValue());
		
		}
		
		
		 PrintWriter writerDocLen = new PrintWriter("DOClen.txt", "UTF-8");
		for(Map.Entry doclen:DocLength.entrySet()){
			
			writerDocLen.println(doclen.getKey()+":"+doclen.getValue());
			
			}
		
		DocLength.clear();
		
		
		writerTID.close();
		writerDocLen.close();
		
System.out.println(TermId);
		
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
		
		
		
		private static HashMap<Integer,ArrayList<Integer>>  createCatalog(String FileName) {
			
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
					
	  return (HashMap<Integer,ArrayList<Integer>>) Offset;
	}
		
		}
		
