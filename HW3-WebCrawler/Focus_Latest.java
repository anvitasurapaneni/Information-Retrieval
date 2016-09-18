package Anvita.A3;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.swing.text.html.HTMLDocument.Iterator;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;











public class Focus_Latest {
	public static void main(String a[]) throws URISyntaxException, InterruptedException, IOException
	{ 
	System.out.println("welcome");
	
	PrintWriter corpus = new PrintWriter("CorpusF.txt", "UTF-8");

	PrintWriter inlink = new PrintWriter("InLinkF.txt", "UTF-8");
	PrintWriter outlink = new PrintWriter("OutLinkF.txt", "UTF-8");
	PrintWriter waves = new PrintWriter("LinkWavesF.txt", "UTF-8");

	int NoOfURLS = 0;
	int NoOfURLSinQueue = 0;
	
//constants
int size = 20000;
int seedurls = 6;
int sortsize = 100;
//constants

	int time = 1;
	//	LinkedList<MyURL> listOfURLs = new LinkedList<MyURL>();
	LinkedHashMap<String, MyURLdata1234> listOfURLs = new LinkedHashMap<String, MyURLdata1234>();
	
	//HashMap<String,Integer> URLInDegree = new HashMap<String,Integer>();
	HashMap<String,Integer> URLTyre = new HashMap<String,Integer>();
	HashMap<String,HashSet<String>> URLInLinks = new HashMap<String,HashSet<String>>();

	//	HashMap<String,Set<String>> URLInLinks = new HashMap<String,Set<String>>();

	HashMap<String,Integer> URLStartTime = new HashMap<String,Integer>();
	
	Set<String> URLsVisited = new HashSet<String>();

	URLsVisited.add("http://georgewbush-whitehouse.archives.gov/news/releases/2001/09/20010920-8.html");
	URLsVisited.add("http://voyage.lefigaro.fr/");
	URLsVisited.add("https://en.wikipedia.org/wiki/Module:Convert/text");
		
			
	
	HashMap<String,Date> DomainEntryTime = new HashMap<String,Date>();
//	
//	 String SeedURL7 = "https://en.m.wikipedia.org/wiki/History_of_terrorism#21st_century";
//		URLInDegree.put(SeedURL7, 0);
//		  listOfURLs.add(new MyURLdata1234(SeedURL7,1, time++));
//		  URLsVisited.add(SeedURL7);
//		  DomainEntryTime.put( getDomainName(SeedURL7), new Date());
//		  URLTyre.put(SeedURL7, 0);
		  
	

//	  String SeedURL8 = "http://www.brookings.edu/research/articles/2011/12/terrorism-riedel";
//		URLInDegree.put(SeedURL8, 0);
//		  listOfURLs.add(new MyURLdata1234(SeedURL8,1, time++));
//		  URLsVisited.add(SeedURL8);
//		  DomainEntryTime.put( getDomainName(SeedURL8), new Date());
//		  URLTyre.put(SeedURL8, 0);
//	
		String SeedURL6 = "https://en.wikipedia.org/wiki/History_of_terrorism";
	//URLInDegree.put(SeedURL6, 0);
	  listOfURLs.put(CanonicalizeURL(SeedURL6), new MyURLdata1234(SeedURL6,1, time++, 0));
	  URLsVisited.add(SeedURL6);
	  DomainEntryTime.put( getDomainName(SeedURL6), new Date());
	  URLTyre.put(SeedURL6, 0);
	  
	 
		  
		
//				  
//				  String SeedURL9 = "http://www.mapreport.com/subtopics/c/9.html";
//					URLInDegree.put(SeedURL9, 0);
//					  listOfURLs.add(new MyURLdata1234(SeedURL9,1, time++));
//					  URLsVisited.add(SeedURL10);
//					  DomainEntryTime.put( getDomainName(SeedURL9), new Date());
//					  URLTyre.put(SeedURL9, 0);
			  
				  
			  
	
	String SeedURL1 = "http://en.wikipedia.org/wiki/List_of_terrorist_incidents";
	//URLInDegree.put(SeedURL1, 0);
	  listOfURLs.put(CanonicalizeURL(SeedURL1),(new MyURLdata1234(SeedURL1,1, time++, 0)));
	  URLsVisited.add(SeedURL1);
	  DomainEntryTime.put( getDomainName(SeedURL1), new Date());
	  URLTyre.put(SeedURL1, 0);
	  
	 String SeedURL2 = "http://en.wikipedia.org/wiki/World_Trade_Center";
	//  URLInDegree.put(SeedURL2, 0);  
	 listOfURLs.put(CanonicalizeURL(SeedURL2), new MyURLdata1234(SeedURL2,1, time++, 0));
	  URLsVisited.add(SeedURL2);
	  DomainEntryTime.put( getDomainName(SeedURL2), new Date());
	  URLTyre.put(SeedURL2, 0);
	
	  String SeedURL3 = "http://en.wikipedia.org/wiki/Collapse_of_the_World_Trade_Center";
	 // URLInDegree.put(SeedURL3, 0);
	  listOfURLs.put(CanonicalizeURL(SeedURL3), new MyURLdata1234(SeedURL3,1, time++, 0));
	  URLsVisited.add(SeedURL3);
	  DomainEntryTime.put( getDomainName(SeedURL3), new Date());
	  URLTyre.put(SeedURL3, 0);

	  String SeedURL4 = "http://en.wikipedia.org/wiki/September_11_attacks";
	//  URLInDegree.put(SeedURL4, 0);
	  listOfURLs.put(CanonicalizeURL(SeedURL4), new MyURLdata1234(SeedURL4,1, time++, 0));
	  URLsVisited.add(SeedURL4);
	  DomainEntryTime.put( getDomainName(SeedURL4), new Date());
	  URLTyre.put(SeedURL4, 0);

	  String SeedURL5 = "http://en.wikipedia.org/wiki/Patriot_Act";
	 // URLInDegree.put(SeedURL5, 0);
	  listOfURLs.put(CanonicalizeURL(SeedURL5), new MyURLdata1234(SeedURL5,1, time++, 0));
	  URLsVisited.add(SeedURL5);
	  DomainEntryTime.put( getDomainName(SeedURL5), new Date());
	  URLTyre.put(SeedURL5, 0);
	
	  
	  NoOfURLSinQueue = seedurls;
	  
	 
	  
	  while (!listOfURLs.isEmpty()) {
	// System.out.println("reaching here");
		  Map.Entry<String,MyURLdata1234> entry=listOfURLs.entrySet().iterator().next();
	  MyURLdata1234 urll =  (MyURLdata1234) entry.getValue();
	  String key = (String) entry.getKey();
	  listOfURLs.remove(key);
	 
	  String str =  (String) urll.geturl();
	  URL u1 = new URL(str);
	String strCan = CanonicalizeURL(str);
	

	
try{

	  Document doc;
	  //pause
	  Date d2 = new Date();
	  long mseconds =  d2.getTime() -  DomainEntryTime.get(getDomainName(str)).getTime() ;
	if(mseconds <1000){
	Thread.sleep(1000 - mseconds);
	}
	// end pause
	  doc = Jsoup.connect(str).timeout(1000).header("Accept-Language", "en").get();
	  DomainEntryTime.put( getDomainName(str), new Date());
	//  String txtDoc = getText(str); 
	  String txtDoc = doc.text(); 
	  String title = doc.title();
//	  Element taglang = doc.select("html").first();
//	  String lang = taglang.attr("lang");
	//  System.out.println(lang);


	  
	 
	  NoOfURLS = NoOfURLS + 1;
	  //writing to file
	  corpus.println("<DOC>\n<DOCNO>"+strCan+"</DOCNO>\n<HEAD>"+title+"</HEAD>\n"
	  		+ "<AUTHOR>ANVITA</AUTHOR>\n<URL>"+str+"</URL>\n<TEXT>"+txtDoc+"\n</TEXT>\n</DOC>");

	  System.out.println(NoOfURLS+" "+str+" popping");
	  // writing to file

	  

	outlink.print(strCan+"\t");
	  Elements links = doc.select("a[href]");

	  for (Element link : links) {

	  String str1 = link.attr("abs:href");
	  outlink.print(str1+"\t");
	  if(NoOfURLSinQueue < size){
	  try{
		//  String txt = getText(str1);
		  URL u11 = new URL(str1);
		  Date d21 = new Date();
		  long mseconds1 =  d21.getTime() -  DomainEntryTime.get(getDomainName(str1)).getTime() ;
		if(mseconds1 <1000){
		Thread.sleep(1000 - mseconds1);
		}
		  Document doc1 = Jsoup.connect(str1).header("Accept-Language", "en").timeout(1000).get();
		  DomainEntryTime.put( getDomainName(str1), new Date());
		  String strCan1 = CanonicalizeURL(str1);
		  boolean validity1 = isRobotAllowed(u11);
		  
		  
		  String txt = doc1.text(); 
		 
//		  Element taglang1 = doc1.select("html").first();
//		  String lang1 = taglang1.attr("lang");
////		  if(lang1 != lang){
//			  System.out.println(lang1);
//			  System.out.println(lang);  
//		  }
			
		  if (!(URLsVisited.contains(strCan1))){
			  URLsVisited.add(strCan1);
		
		// (lang1 == "en") &&
		  if( isURLRequired(str1) && (validity1 == true)
				  && 
			//	  || !(URLsVisited.contains(strCan1.replace("http:", "https:")))  
			//			  ||   !(URLsVisited.contains(strCan1.replace("https:", "http:"))) 
				//  )&&  
				  isTextValied(txt) && (NoOfURLSinQueue < size)){
			//  System.out.println(lang1);
		//	  URLInDegree.put(str1, URLInDegree.get(str1) == null? 1 : URLInDegree.get(str1) + 1);
			  HashSet<String> al = new HashSet<String>();
			  al.add(str);
			  URLInLinks.put(strCan1, (HashSet<String>) (URLInLinks.get(strCan1) == null? al : URLInLinks.get(strCan1).add(strCan)));
			  MyURLdata1234 mu = listOfURLs.get(CanonicalizeURL(str1));
			  int indegree = 1;
			  if(mu == null){
				  indegree = 1;  
			  }
			  else{
				  indegree = mu.getindegree()+ 1; 
			  }
			 

		//	listOfURLs.add(new MyURLdata1234(str1,URLInDegree.get(str1),time++));
			listOfURLs.put(CanonicalizeURL(str1), new MyURLdata1234(str1,indegree, time++, GetScore(txt)));
			
			URLTyre.put(strCan1, URLTyre.get(strCan)+1);
			 DomainEntryTime.put( getDomainName(str1), new Date());
			 NoOfURLSinQueue = NoOfURLSinQueue + 1;
			 System.out.println(NoOfURLSinQueue+" "+str1+" adding");
			 if (NoOfURLSinQueue == size) break;
			  }
		  }
		  
	  }
	  
	  catch (java.net.UnknownHostException e) {

	  //    System.out.println("Unknown Host Ex");

	  }
	  catch (org.jsoup.HttpStatusException e) {

	 //     System.out.println("Jsoup Ex");

	  }

	  catch (java.net.ConnectException e) {

	 //     System.out.println("connection exception");

	  }

	  catch (MalformedURLException e) {

	//	    System.out.println("The URL is not valid.");

	  }
	  catch (java.net.SocketTimeoutException e) {

			//	    System.out.println("The URL is not valid.");

			  }
	  

	  catch (Exception e) {

	//      System.out.println("other exception");

	  }
	  }
	  } 
	  
	  outlink.print("\n"); 
	  if((NoOfURLS) >= seedurls   && NoOfURLS <= sortsize){
			 System.out.println("sorting");
			 System.out.println(NoOfURLS);
		//	 Collections.sort(listOfURLs, new MyindegreeComp1234());
			 
		//	 listOfURLs =	 (LinkedHashMap<java.lang.String, MyURLdata1234>) entriesSortedByValues(listOfURLs);
			 
			 listOfURLs =	 sortByValues(listOfURLs);
			 
			
			 }
	 
}
catch (java.net.UnknownHostException e) {

	  //    System.out.println("Unknown Host Ex");

	  }
	  catch (org.jsoup.HttpStatusException e) {

	 //     System.out.println("Jsoup Ex");

	  }

	  catch (java.net.ConnectException e) {

	 //     System.out.println("connection exception");

	  }

	  catch (MalformedURLException e) {

	//	    System.out.println("The URL is not valid.");

	  }
	  catch (java.net.SocketTimeoutException e) {

			//	    System.out.println("The URL is not valid.");

			  }
	  

	  catch (Exception e) {

	//      System.out.println("other exception");

	  }

  }
	  
	  
	  for(Entry<String, HashSet<String>> m:URLInLinks.entrySet()){

		  HashSet<String> al = m.getValue();
		  inlink.print(m.getKey()+"\t");
		  for (String s : al) {
			  inlink.print(s+"\t");
			}
		  inlink.print("\n");

	  }
	  
	  for(Entry<String, Integer> m:URLTyre.entrySet()){
		  waves.print(m.getKey()+"\t"+m.getValue()+"\n");
	 }
	  
	  
	  
	  inlink.close();
	  outlink.close();
	  corpus.close();
	  waves.close();


	
	}
	private static boolean isURLRequired(String str1) {
		String str = str1.toLowerCase();
	if(str.contains(".pdf") || str.contains(".txt") || str.contains("facebook") 
			|| str.contains("twitter") || str.contains("maps.google") || str.contains("instagram")
			|| str.contains("about") || str.contains("contact") || str.contains("license")
			|| str.contains("policy")){
	return false;
	}
	else
	return true;
	}
	
	
	private static boolean isTextValied(String txt1) {
		String str = txt1.toLowerCase();
	if(str.contains("bomb") || str.contains("shoot") || str.contains("explosion") 
			|| str.contains("terror") || str.contains("assassin") || str.contains("death")
			|| str.contains("massacres") || str.contains("violence") || str.contains("dead")){
	return true;
	}
	else
	return false;
	}
	

	
	
	public static void printList(List l) {
	for (Object o : l) {
	System.out.println(o);
	}
	}
	
	
	public static String getDomainName(String url) throws URISyntaxException {
	    URI uri = new URI(url);
	    String domain = uri.getHost();
	    return  domain;
	}
	
	 private static HashMap disallowListCache = new HashMap();
	
	private URL verifyUrl(String url) {
	        // Only allow HTTP URLs.
	        if (!url.toLowerCase().startsWith("http://"))
	            return null;
	         
	        // Verify format of URL.
	        URL verifiedUrl = null;
	        try {
	            verifiedUrl = new URL(url);
	        } catch (Exception e) {
	            return null;
	        }
	         
	        return verifiedUrl;
	    }
	
	
	private static boolean isRobotAllowed(URL urlToCheck) {
	        String host = urlToCheck.getHost().toLowerCase();
	         
	        // Retrieve host's disallow list from cache.
	        ArrayList disallowList =
	                (ArrayList) disallowListCache.get(host);
	         
	        // If list is not in the cache, download and cache it.
	        if (disallowList == null) {
	            disallowList = new ArrayList();
	             
	            try {
	                URL robotsFileUrl =
	                        new URL("http://" + host + "/robots.txt");
	                 
	                // Open connection to robot file URL for reading.
	                BufferedReader reader =
	                        new BufferedReader(new InputStreamReader(
	                        robotsFileUrl.openStream()));
	                 
	                // Read robot file, creating list of disallowed paths.
	                String line;
	                while ((line = reader.readLine()) != null) {
	                    if (line.indexOf("Disallow:") == 0) {
	                        String disallowPath =
	                                line.substring("Disallow:".length());
	                         
	                        // Check disallow path for comments and 
	                        // remove if present.
	                        int commentIndex = disallowPath.indexOf("#");
	                        if (commentIndex != - 1) {
	                            disallowPath =
	                                    disallowPath.substring(0, commentIndex);
	                        }
	                         
	                        // Remove leading or trailing spaces from 
	                        // disallow path.
	                        disallowPath = disallowPath.trim();
	                         
	                        // Add disallow path to list.
	                        disallowList.add(disallowPath);
	                    }
	                }
	                 
	                // Add new disallow list to cache.
	                disallowListCache.put(host, disallowList);
	            } catch (Exception e) {
	        /* Assume robot is allowed since an exception
	           is thrown if the robot file doesn't exist. */
	                return true;
	            }
	        }
	         
	    /* Loop through disallow list to see if the
	       crawling is allowed for the given URL. */
	        String file = urlToCheck.getFile();
	        for (int i = 0; i < disallowList.size(); i++) {
	            String disallow = (String) disallowList.get(i);
	            if (file.startsWith(disallow)) {
	                return false;
	            }
	        }
	         
	        return true;
	    }
	     
	
	
	
	
	
	private static String  getText(String url) throws IOException {

	Document doc;

	  doc = Jsoup.connect(url).get();

	  Elements paragraphs = doc.select("p");

	  String str = "";

	  for(Element p : paragraphs)

	  str = str + "\n" + p.text();

	  return str;

	}
	
	
	
	private static int GetScore(String txt1) {
		String textForDoc = txt1.toLowerCase();
		int i =0;
		Pattern MY_PATTERN =
				Pattern.compile("massacres|violence|explosion|death|21|shoot|terror|bomb|century");
		
		java.util.regex.Matcher m = MY_PATTERN.matcher(textForDoc);
		while (m.find()){
			i++;
		}
	//	System.out.print(i);
	
	return i;
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
	
	private static LinkedHashMap<String, MyURLdata1234> sortByValues(HashMap<String, MyURLdata1234> map) { 
	       List list = new LinkedList(map.entrySet());
	       // Defined Custom Comparator here
	       Collections.sort(list, new MyindegreeComp1234() );

	       // Here I am copying the sorted list in HashMap
	       // using LinkedHashMap to preserve the insertion order
	       LinkedHashMap<String, MyURLdata1234> sortedHashMap = new LinkedHashMap<String, MyURLdata1234>();
	       for (Iterator it = (Iterator) list.iterator(); ((java.util.Iterator<Entry<String, MyURLdata1234>>) it).hasNext();) {
	              
	    	   Map.Entry<String, MyURLdata1234> entry =  ((java.util.Iterator<Entry<String, MyURLdata1234>>) it).next();
	              sortedHashMap.put(entry.getKey(), entry.getValue());
	       } 
	       return sortedHashMap;
	  }
	
	} 

class MyindegreeComp1234 implements Comparator <Entry<String, MyURLdata1234>>
{
	@Override
	public int compare(Entry<String, MyURLdata1234> e11, Entry<String, MyURLdata1234> e21) 
{
	MyURLdata1234 e1 = e11.getValue();
	MyURLdata1234 e2 = e21.getValue();
	if(e1.getindegree() < e2.getindegree())
	{ return 1; } 	
	if(e1.getindegree() == e2.getindegree())
	{ 
		if(e1.getscore() < e2.getscore())
		{ return 1; } 
		if(e1.getscore() == e2.getscore()){
			if(e1.getentryTime() < e2.getentryTime()){
				return -1;
				}
				else{
				return 1;
				}
		}
		else
		{ return -1; 	}
	}
	else
	{ return -1; }
	
}
}


class MyURLdata1234
{
	private String url;
	private int indegree;
	private int entryTime;
	private int score;
	
	public MyURLdata1234(String n, int s, int e, int sc)
	{ 
	this.url = n;
	this.indegree = s; 
	this.entryTime = e;
	this.score = sc;
	
	} 
	
	public int getentryTime() 
	{ return entryTime; } 
	public void setentryTime(int t) 
	{ this.entryTime = entryTime; } 
	
	public String geturl() 
	{ return url; } 
	public void seturl(String url) 
	{ this.url = url; } 
	
	public int getscore() 
	{ return score; } 
	public void setscore(int sc) 
	{ this.score = sc; } 
	
	
	public int getindegree() 
	{ return indegree; } 
	public void setindegree(int indegree) 
	{ this.indegree = indegree; } 
	public String toString()
	{ return "url: "+this.url+"-- indegree: "+this.indegree; }
	} 
