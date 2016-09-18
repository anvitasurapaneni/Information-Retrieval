package Anvita.CS6200;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FinalCatalogue {
public static void main(String[] args) throws IOException
	{
	File cat = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/final.txt");
	HashMap<Integer,ArrayList<Integer>> finalCat = createCatalog(cat);
	
	 PrintWriter writer = new PrintWriter("FinalCatalog.txt", "UTF-8");
	 
	 for(Map.Entry m:finalCat.entrySet()){
		 ArrayList<Integer> al = (ArrayList<Integer>) m.getValue();
		 writer.println(m.getKey()+":"+al.get(0)+":"+al.get(1));
		 }
	
	 writer.close();
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
}
