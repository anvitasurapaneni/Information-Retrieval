package Anvita.A3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.Document;

import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

public class URLwithheader {
public static void main(String[] args) throws IOException{
	PrintWriter header = new PrintWriter("HTTPHeaderF.txt", "UTF-8");
	PrintWriter headerCat = new PrintWriter("HTTPHeaderCatF.txt", "UTF-8");
	//PrintWriter source = new PrintWriter("HTMLSourceF.txt", "UTF-8");
	//PrintWriter SourceCat = new PrintWriter("HTMLSouceCatF.txt", "UTF-8");
	
	File mFile = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/CorpusF.txt");
	// String str = FileUtils.readFileToString(mFile);
	 BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(mFile)));
	 int dc = 0;
	 String line;
	 String doc ="";
	 Long start = (long) 0;
	 Long end = (long) 0;
	 while( (line = br1.readLine()) != null){
		 doc = doc + line;
		 if(doc.contains("</DOC>")){
			 dc = dc+1;
			 doc = doc + line;
			 
			 
			 final Pattern pattern1 = Pattern.compile("<DOCNO>(.+?)</DOCNO>");
			 final Matcher matcher1 = pattern1.matcher(doc);
			 matcher1.find();
			 String docNoTemp1 = matcher1.group(1);
			 String docNoTemp = docNoTemp1.trim();
			 
			 final Pattern pattern3 = Pattern.compile("<URL>(.+?)</URL>");
			 final Matcher matcher3 = pattern3.matcher(doc);
			 matcher3.find();
			 String url3 = matcher3.group(1);
			 String url = url3.trim();
			 org.jsoup.nodes.Document doc1;
			 
			  doc1 = Jsoup.connect(url).timeout(10000).header("Accept-Language", "en").get();
			
			 
			 URL obj = new URL(url);
				URLConnection conn = obj.openConnection();
				
			// System.out.println("http source");
			// System.out.println(response.parse().toString());
				Element s = doc1.head();
			 String source1 = url+"\t"+s;
					 //.getHeaderField("Server");
			 Long size = (long) source1.length();
			 end = start + size;
			 System.out.println(source1);
			 header.println(source1);
			 
			// System.out.println(start + " " +end);
			 headerCat.println(docNoTemp+"\t"+start+"\t"+end);
			 
			 doc ="";
			 start = end + 1;
		 }
	 
		
	}
	 
	 header.close();
	 headerCat.close();
	 
}
	

}
