package anvitaA5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class TrecEvalFinal {
	public static void main(String a[]) throws  ExecutionException, IOException
	{

		LinkedHashMap<String, LinkedHashSet<MyTrecData>> qreldata = new LinkedHashMap<String, LinkedHashSet<MyTrecData>>();

		LinkedHashMap<String, LinkedHashSet<MyRankListData>> ranklistdata = new LinkedHashMap<String, LinkedHashSet<MyRankListData>>();
		LinkedHashMap<String, LinkedHashSet<MyRankListData>> RELranklistdata = new LinkedHashMap<String, LinkedHashSet<MyRankListData>>();

		LinkedHashMap<String, LinkedHashSet<PRFatK>> prk = new LinkedHashMap<String, LinkedHashSet<PRFatK>>();
		
		LinkedHashMap<String, Float> TotalRel = new LinkedHashMap<String, Float>();

		LinkedHashMap<String, Float> avgprecision = new LinkedHashMap<String, Float>();

		LinkedHashMap<String, Float>  Rprecision = new LinkedHashMap<String, Float>();

		LinkedHashMap<String, Float>  ndcgM = new LinkedHashMap<String, Float>();

		LinkedHashMap<String, Float>  preAt5 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  preAt10 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  preAt20 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  preAt50 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  preAt100 = new LinkedHashMap<String, Float>();

		LinkedHashMap<String, Float>  reAt5 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  reAt10 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  reAt20 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  reAt50 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  reAt100 = new LinkedHashMap<String, Float>();

		LinkedHashMap<String, Float>  f1At5 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  f1At10 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  f1At20 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  f1At50 = new LinkedHashMap<String, Float>();
		LinkedHashMap<String, Float>  f1At100 = new LinkedHashMap<String, Float>();


		File fqr = new File("/Users/anvitasurapaneni/Downloads/TrecEval/qrels.adhoc.51-100.AP89.txt");
		//File fqr = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/AFinalQrel.txt");
		BufferedReader br1= new BufferedReader(new InputStreamReader(new FileInputStream(fqr)));

		File frl = new File("/Users/anvitasurapaneni/Downloads/TrecEval/Trec-100.txt");
		//File frl = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/okapiBM25A4.txt");
		BufferedReader br2= new BufferedReader(new InputStreamReader(new FileInputStream(frl)));
		String line1;


		//trec eval
		while( (line1 = br1.readLine()) != null){
			String[] strSp = line1.split("\\s+");
			String qno = strSp[0];
			String url = strSp[2];
			String auth = strSp[1];

			float rel = Float.parseFloat(strSp[3]);

			MyTrecData mt = new MyTrecData(url, auth, rel);
			if(rel == 1){
			if(TotalRel.get(qno) == null){
				TotalRel.put((String)qno, (float) 1);
			}
			else{
				float x = TotalRel.get(qno);
				x = x + 1;
				TotalRel.put((String)qno, x);
			}
			}
			




			if(qreldata.get(qno) == null){
				LinkedHashSet<MyTrecData> ls = new LinkedHashSet<MyTrecData>();
				ls.add(mt);
				
				qreldata.put(qno, ls);
			}
			else{
				LinkedHashSet<MyTrecData> ls1 = new LinkedHashSet<MyTrecData>();
				ls1 = qreldata.get(qno);

				ls1.add(mt);

				
				qreldata.put(qno, ls1);
			}

		}
		
		


		// rank list
		
		
		while( (line1 = br2.readLine()) != null){
			String[] strSp = line1.split("\\s+");
			String qno = strSp[0];
			String url = strSp[2];
			String auth = strSp[1];

			float rank = Float.parseFloat(strSp[3]);
			float rel = getRelavenceFronQrel(url,qreldata.get(qno));
			
if(rank <= 1000){
			MyRankListData mt = new MyRankListData(url, auth, rank, rel);




			if(ranklistdata.get(qno) == null){
				LinkedHashSet<MyRankListData> ls = new LinkedHashSet<MyRankListData>();
				ls.add(mt);
				ranklistdata.put(qno, ls);
				if(rel == 1){
					RELranklistdata.put(qno, ls);
				}
			}
			else{
				LinkedHashSet<MyRankListData> ls1 = new LinkedHashSet<MyRankListData>();
				ls1 = ranklistdata.get(qno);

				ls1.add(mt);

				ranklistdata.put(qno, ls1);
				if(rel == 1){
					RELranklistdata.put(qno, ls1);
				}
			}
		}
		}


		for(Map.Entry m:RELranklistdata.entrySet()){

			LinkedHashSet<MyRankListData> lh = (LinkedHashSet<MyRankListData>)m.getValue();

		}




		for(Map.Entry m:ranklistdata.entrySet()){

			LinkedHashSet<MyRankListData> l = (LinkedHashSet<MyRankListData>) m.getValue();
			TreeSet<MyRankListData> l1 = new TreeSet<MyRankListData>(new MyRankComp()); 
			l1.addAll(l);
			LinkedHashSet<MyRankListData> l2 = new LinkedHashSet<MyRankListData>();
			l2.addAll(l1);
			ranklistdata.put((String) m.getKey(), l2);


		}



		// caliculating pre and rel and f1
		for(Map.Entry m:ranklistdata.entrySet()){
			
			LinkedHashSet<PRFatK> prklist = new LinkedHashSet<PRFatK>();


			// for each query
			LinkedHashSet<MyRankListData> rldata = (LinkedHashSet<MyRankListData>) m.getValue();
			LinkedHashSet<MyRankListData> rldata1 = (LinkedHashSet<MyRankListData>) m.getValue();
			float k = rldata.size();
			


			java.util.Iterator<MyRankListData> itr1 = rldata1.iterator();


			
			float totalRel = TotalRel.get(m.getKey());


			java.util.Iterator<MyRankListData> itrB = rldata.iterator();
			float i = 1;

			while(itrB.hasNext()){
				MyRankListData tdB = itrB.next();
				java.util.Iterator<MyRankListData> itr = rldata.iterator();
				java.util.Iterator<MyRankListData> itr2 = rldata.iterator();
				java.util.Iterator<MyRankListData> itr3 = rldata.iterator();
				

				 
				// getting total no of relevant 
				float totalRelk = 0;
				float c = 0;
				while(itr.hasNext()){
					if (c > i-1) break; 
					MyRankListData td = itr.next();
					if(td.relevance == 1){
						totalRelk = totalRelk + 1;
					}
					c++;
				}
				


				float p = totalRelk/i;

				float r =0;
				if(totalRel != 0)
				{	r = totalRelk/totalRel;}

				float f = 0;
				if(p+r != 0){
					f = (2 * p * r)/(p + r);
				}
				if(totalRel != 0){
					if(p == r && Rprecision.get(m.getKey()) == null){
						Rprecision.put((String) m.getKey(), p);
					}
				}

				PRFatK prkone = new PRFatK(tdB.rank, p, r, f, tdB.relevance);
				if(i == 5){
					preAt5.put((String) m.getKey(), p);
					reAt5.put((String) m.getKey(), r);
					f1At5.put((String) m.getKey(), f);
				}

				if(i == 10){
					preAt10.put((String) m.getKey(), p);
					reAt10.put((String) m.getKey(), r);
					f1At10.put((String) m.getKey(), f);
				}

				if(i == 20){
					preAt20.put((String) m.getKey(), p);
					reAt20.put((String) m.getKey(), r);
					f1At20.put((String) m.getKey(), f);
				}

				if(i == 50){
					preAt50.put((String) m.getKey(), p);
					reAt50.put((String) m.getKey(), r);
					f1At50.put((String) m.getKey(), f);
				}
				if(i == 100){
					preAt100.put((String) m.getKey(), p);
					reAt100.put((String) m.getKey(), r);
					f1At100.put((String) m.getKey(), f);
				}
				prklist.add(prkone);

				i = i + 1;

			}

			prk.put((String) m.getKey(), prklist);

		}



		//caliculate avg precision


		for(Map.Entry m:prk.entrySet()){
			//


			// for each query
			LinkedHashSet<PRFatK> rldata = (LinkedHashSet<PRFatK>) m.getValue();
			
			float count = TotalRel.get(m.getKey());
			float val = (float) 0;


			java.util.Iterator<PRFatK> itr1 = rldata.iterator();

			while(itr1.hasNext()){

				PRFatK td = itr1.next();
				if(td.relavence != 0 ){
					val = val + td.precision;
				}
			}

			avgprecision.put((String) m.getKey(), val/count);
		}


		// caliculating ndcg


		for(Map.Entry m:ranklistdata.entrySet()){


			LinkedList<Float> iANDr = new LinkedList<Float>();
			LinkedHashSet<MyRankListData> rldata = (LinkedHashSet<MyRankListData>) m.getValue();



			java.util.Iterator<MyRankListData> itr1 = rldata.iterator();
			float i = 0;
			while(itr1.hasNext()){

				MyRankListData td = itr1.next();
				Float val  = (float)0;
					val = td.rank;
				

				iANDr.add(val);
			}

			LinkedList<Float>iANDrD = new LinkedList<Float>();
			iANDrD.addAll(iANDr);
			Collections.sort(iANDrD, Collections.reverseOrder());

			float Dcg = 0;
			float dcgDesc = 0;
			float ndcg = 0;
			float i1 = 0;
			float i2 = 0;
			for(float r:iANDr){
				i1 = i1+1;
				float lg =0;
				if(i1 == 1){
					lg=1;
				}
				else{
					lg = (float) Math.log(i1);
				}
				Dcg = Dcg + (r/lg);
			}

			for(float r:iANDrD){
				i2 = i2+1;
				float lg =0;
				if(i2 == 1){
					lg=1;
				}
				else{
					lg = (float) Math.log(i2);
				}
				dcgDesc = dcgDesc + (r/lg);
			}
			if(dcgDesc != 0){
				ndcg = Dcg/dcgDesc;
			}
			ndcgM.put((String)m.getKey(), ndcg);

		}





		float c = 0;
		float v = 0;		 		

		System.out.println("\nr precision\n");
		for(Map.Entry m:Rprecision.entrySet()){
			System.out.println(m.getKey()+"  "+m.getValue());
			c = c + 1;
			v = v + (float)m.getValue();


		}
		float rp = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\naverage precision\n");
		for(Map.Entry m:avgprecision.entrySet()){
			System.out.println(m.getKey()+"  "+m.getValue());
			float val = (float)m.getValue();
			c = c + 1;
			if(!Float.isNaN(val)){



				v = v + (float)m.getValue();
			}


		}
		float ap = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nndcg\n");
		for(Map.Entry m:ndcgM.entrySet()){
			System.out.println(m.getKey()+"  "+m.getValue());
			v = v + (float)m.getValue();
			c = c + 1;

		}
		float nd = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nprecision\n");
		System.out.println("\nprecision at 5\n");
		// at 5
		for(Map.Entry m:preAt5.entrySet()){
			System.out.println(m.getKey()+"  "+m.getValue()); 
			c = c + 1;
			v = v + (float)m.getValue();


		}
		float p5 = v/c;
	
		c = 0;
		v = 0;
		System.out.println("\nprecision at 10\n");
		// at 10
		for(Map.Entry m:preAt10.entrySet()){
			System.out.println(m.getKey()+" "+m.getValue());
			c = c + 1;
			v = v + (float)m.getValue();


		}
		float p10 = v/c;
		
		c = 0;
		v = 0;
		// at 20
		System.out.println("\nprecion at 20\n");
		for(Map.Entry m:preAt20.entrySet()){
			System.out.println(m.getKey()+" "+m.getValue()); 
			c = c + 1;
			v = v + (float)m.getValue();


		}
		float p20 = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nprecision at 50\n");
		// at 50
		for(Map.Entry m:preAt50.entrySet()){
			System.out.println(m.getKey()+" "+m.getValue()); 
			c = c + 1;
			v = v + (float)m.getValue();


		}
		float p50 = v/c;
		
		c = 0;
		v = 0;
		//at 100
		System.out.println("\nprecision at 100\n");
		for(Map.Entry m:preAt100.entrySet()){
			System.out.println(m.getKey()+" "+m.getValue());
			c = c + 1;
			v = v + (float)m.getValue();


		}
		float p100 = v/c;
		
		c = 0;
		v = 0;

		System.out.println("\nRecall\n");
		System.out.println("\nrecall at 5\n");
		for(Map.Entry m:reAt5.entrySet()){
			System.out.println(m.getKey()+" "+m.getValue());
			c = c + 1;
			v = v + (float)m.getValue();
			

		}
		float r5 = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nrecall at 10\n");
		for(Map.Entry m:reAt10.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float r10 = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nrecall at 20\n");
		for(Map.Entry m:reAt20.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float r20 = v/c;
	
		c = 0;
		v = 0;
		System.out.println("\nrecall at 50\n");
		for(Map.Entry m:reAt50.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float r50 = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nrecall at 100\n");
		for(Map.Entry m:reAt100.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float r100 = v/c;
		
		c = 0;
		v = 0;

		System.out.println("\nf1\n");
		System.out.println("\nf1 at 5\n");
		for(Map.Entry m:f1At5.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float f5 = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nf1 at 10\n");
		for(Map.Entry m:f1At10.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float f10 = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nf1 at 20\n");
		for(Map.Entry m:f1At20.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float f20 = v/c;
		
		c = 0;
		v = 0;
		System.out.println("\nf1 at 50\n");
		for(Map.Entry m:f1At50.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float f50 = v/c;
	
		c = 0;
		v = 0;
		System.out.println("\nf1 at 100\n");
		for(Map.Entry m:f1At100.entrySet()){

			c = c + 1;
			v = v + (float)m.getValue();
			System.out.println(m.getKey()+" "+m.getValue());

		}
		float f100 = v/c;
		
		c = 0;
		v = 0;
		System.out.println("----------------------------------------------------------------");
		System.out.println("----------------------------------------------------------------");
		

System.out.println("r precision		"+rp);

System.out.println("average precision	"+ap);

System.out.println("ndcg			"+nd);

System.out.println("\n precision");

System.out.println("at 5	"+p5);

System.out.println("at 10	"+p10);

System.out.println("at 20	"+p20);

System.out.println("at 50	"+p50);

System.out.println("at 100	"+p100);

System.out.println("\nRecall");

System.out.println("at 5	"+r5);

System.out.println("at 10	"+r10);

System.out.println("at 20	"+r20);

System.out.println("at 50	"+r50);

System.out.println("at 100	"+r100);

System.out.println("\nf1");

System.out.println("at 5	"+f5);

System.out.println("at 10	"+f10);

System.out.println("at 20	"+f20);

System.out.println("at 50	"+f50);

System.out.println("at 100	"+f100);

	}

	private static float getPrecision(LinkedHashSet<PRFatK> prkfor1query, float rank) {

		float res = (float)0;
		for(PRFatK pr: prkfor1query){
			if(pr.k == rank){
				res = pr.precision;
			}
		}
		return res;
	}

	private static float  getRelavenceFronQrel(String url,LinkedHashSet<MyTrecData> listOfTrec) throws IOException {
		float res = 0;
		if(listOfTrec != null){
			for(MyTrecData mt: listOfTrec){

				if (mt.url.equals(url)){
					//System.out.println(url+":matched:"+mt.url);
					res = mt.relevance;
				}

			}
		}
		return res;
	}


	private static LinkedHashMap<Float, Float> sortHMRev(LinkedHashMap<Float, Float> aMap) {

		Set<Entry<Float, Float>> mapEntries = aMap.entrySet();
		List<Entry<Float, Float>> aList = new LinkedList<Entry<Float, Float>>(mapEntries);

		Collections.sort(aList, new Comparator<Entry<Float, Float>>() {


			public int compare(Entry<Float, Float> ele1,
					Entry<Float, Float> ele2) {

				return ele1.getValue().compareTo(ele2.getValue());
			}
		});

		Map<Float, Float> aMap2 = new LinkedHashMap<Float, Float>();
		for(Entry<Float, Float> entry: aList) {
			aMap2.put(entry.getKey(), entry.getValue());
		}

		return (LinkedHashMap<Float, Float>) aMap2;
	}

}
