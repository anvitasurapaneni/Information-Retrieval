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

import org.jsoup.Connection.Response;
import org.jsoup.Jsoup;

public class UrlWithSource {
public static void main(String[] args) throws IOException{
	//PrintWriter header = new PrintWriter("HTTPHeaderF.txt", "UTF-8");
	//PrintWriter headerCat = new PrintWriter("HTTPHeaderCatF.txt", "UTF-8");
	PrintWriter source = new PrintWriter("HTMLSourceF.txt", "UTF-8");
	PrintWriter SourceCat = new PrintWriter("HTMLSouceCatF.txt", "UTF-8");
	
	File mFile = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/CorpusF.txt");
	// String str = FileUtils.readFileToString(mFile);
	 BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(mFile)));
	 int dc = 0;
	 String line;
	 String doc ="";
	 long start = 0;
	 long end = 0;
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
			 
			 
			 Response	 response = Jsoup.connect(url)
		                .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21")
		                .timeout(10000)
		                .execute();
			
			 
			 URL obj = new URL(url);
				URLConnection conn = obj.openConnection();
				
			// System.out.println("http source");
			// System.out.println(response.parse().toString());
			 String source1 = url+"\t"+response.parse().toString();
					 //.getHeaderField("Server");
			 int size = source1.length();
			 end = start + size;
			// System.out.println(source1);
			 source.println(source1);
			 
			 System.out.println(start + " " +end);
			 SourceCat.println(docNoTemp+"\t"+start+"\t"+end);
			 
			 doc ="";
			 start = end + 1;
		 }
	 
		
	}
	 System.out.println(dc);
	 source.close();
	 SourceCat.close();
	 
}
	

}
