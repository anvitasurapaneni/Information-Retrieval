package anvitaA6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonFormat.Feature;
import com.google.common.reflect.Parameter;

import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;

public class getMatrix {
	public static void main(String a[]) throws NumberFormatException, IOException{
	File fqr = new File("/Users/anvitasurapaneni/Downloads/TrecEval/qrels.adhoc.51-100.AP89.txt");
	BufferedReader br1= new BufferedReader(new InputStreamReader(new FileInputStream(fqr)));
	HashMap<String ,HashSet<docnoRel>> Docs = new HashMap<String ,HashSet<docnoRel>>();
	String line1;
	while( (line1 = br1.readLine()) != null){
		String[] strSp = line1.split("\\s+");
		String qno = strSp[0];
		String DocNo = strSp[2];
		DocNo = DocNo.trim();
		String rel = strSp[3];
		float relf = Float.parseFloat(rel);
		
		docnoRel dr = new docnoRel(DocNo, relf);
		
		  if(Docs.get(qno) == null){
			  HashSet<docnoRel> h = new HashSet<docnoRel>();
			  h.add(dr);
			  Docs.put(qno, h);
		  }
		  else{
			  HashSet<docnoRel> h = Docs.get(qno);
			  h.add(dr);
			  Docs.put(qno, h);
		  }
}
	System.out.println(Docs.size());
	
int y = 0;
	
	for(Map.Entry m:Docs.entrySet()){

		HashSet<String> h = (HashSet<String>) m.getValue();
		System.out.println(m.getKey()+" "+h.size());
		y = y + h.size();
		} 
	System.out.println("DSF"+y);
	// from BM 25 data that contains all docs
	File fBM = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/BM25_A6.txt");
	BufferedReader br2= new BufferedReader(new InputStreamReader(new FileInputStream(fBM)));
	HashMap<String ,HashSet<DnoScore>> BM25DocsWithScore = new HashMap<String ,HashSet<DnoScore>>();
	
	while( (line1 = br2.readLine()) != null){
		String[] strSp = line1.split("\\s+");
		
		if(strSp.length >= 4){
		String qno = strSp[0];
		String DocNo = strSp[2];
		DocNo = DocNo.trim();
		String score = strSp[4];
		float s = Float.parseFloat(score);
		//System.out.println(qno);
		HashSet<docnoRel> htemp = Docs.get(qno);
		DnoScore ds = new DnoScore(DocNo, s);
		int boo = 0;
		for(docnoRel dr: htemp){
			if(dr.docno.equals(DocNo)){
				boo = 1;
			}
		}
		if(boo == 1){
			
		  if(BM25DocsWithScore.get(qno) == null){
			  HashSet<DnoScore> h = new HashSet<DnoScore>();
			  h.add(ds);
			  BM25DocsWithScore.put(qno, h);
		  }
		  else{
			  HashSet<DnoScore> h = BM25DocsWithScore.get(qno);
			  h.add(ds);
			  BM25DocsWithScore.put(qno, h);
		  }
		}
}
	}
	
//from laplace smoothing
	File fLS = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/LapSm_A6.txt");
	BufferedReader br3= new BufferedReader(new InputStreamReader(new FileInputStream(fLS)));
	HashMap<String ,HashSet<DnoScore>> LSDocsWithScore = new HashMap<String ,HashSet<DnoScore>>();
	
	while( (line1 = br3.readLine()) != null){
		String[] strSp = line1.split("\\s+");
		
		if(strSp.length >= 4){
		String qno = strSp[0];
		String DocNo = strSp[2];
		DocNo = DocNo.trim();
		String score = strSp[4];
		float s = Float.parseFloat(score);
		//System.out.println(qno);
		HashSet<docnoRel> htemp = Docs.get(qno);
		DnoScore ds = new DnoScore(DocNo, s);
		int boo = 0;
		for(docnoRel dr: htemp){
			if(dr.docno.equals(DocNo)){
				boo = 1;
			}
		}
		if(boo == 1){
			
		  if(LSDocsWithScore.get(qno) == null){
			  HashSet<DnoScore> h = new HashSet<DnoScore>();
			  h.add(ds);
			  LSDocsWithScore.put(qno, h);
		  }
		  else{
			  HashSet<DnoScore> h = LSDocsWithScore.get(qno);
			  h.add(ds);
			  LSDocsWithScore.put(qno, h);
		  }
		}
}
	}
	
//	ProximityScoring_A6.txt
	
	File fPS = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/ProximityScoringA6.txt");
	BufferedReader br4= new BufferedReader(new InputStreamReader(new FileInputStream(fPS)));
	HashMap<String ,HashSet<DnoScore>> PSDocsWithScore = new HashMap<String ,HashSet<DnoScore>>();
	
	while( (line1 = br4.readLine()) != null){
		String[] strSp = line1.split("\\s+");
		
		if(strSp.length >= 4){
		String qno = strSp[0];
		String DocNo = strSp[2];
		DocNo = DocNo.trim();
		String score = strSp[4];
		float s = Float.parseFloat(score);
		//System.out.println(qno);
		HashSet<docnoRel> htemp = Docs.get(qno);
		DnoScore ds = new DnoScore(DocNo, s);
		int boo = 0;
		for(docnoRel dr: htemp){
			if(dr.docno.equals(DocNo)){
				boo = 1;
			}
		}
		if(boo == 1){
			
		  if(PSDocsWithScore.get(qno) == null){
			  HashSet<DnoScore> h = new HashSet<DnoScore>();
			  h.add(ds);
			  PSDocsWithScore.put(qno, h);
		  }
		  else{
			  HashSet<DnoScore> h = PSDocsWithScore.get(qno);
			  h.add(ds);
			  PSDocsWithScore.put(qno, h);
		  }
		}
}
	}
	
	//okapi_TF_A6.txt

	
	File fo = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/okapi_TF_A6.txt");
	BufferedReader br5= new BufferedReader(new InputStreamReader(new FileInputStream(fo)));
	HashMap<String ,HashSet<DnoScore>> OKDocsWithScore = new HashMap<String ,HashSet<DnoScore>>();
	
	while( (line1 = br5.readLine()) != null){
		String[] strSp = line1.split("\\s+");
		
		if(strSp.length >= 4){
		String qno = strSp[0];
		String DocNo = strSp[2];
		DocNo = DocNo.trim();
		String score = strSp[4];
		float s = Float.parseFloat(score);
		//System.out.println(qno);
		HashSet<docnoRel> htemp = Docs.get(qno);
		DnoScore ds = new DnoScore(DocNo, s);
		int boo = 0;
		for(docnoRel dr: htemp){
			if(dr.docno.equals(DocNo)){
				boo = 1;
			}
		}
		if(boo == 1){
			
		  if(OKDocsWithScore.get(qno) == null){
			  HashSet<DnoScore> h = new HashSet<DnoScore>();
			  h.add(ds);
			  OKDocsWithScore.put(qno, h);
		  }
		  else{
			  HashSet<DnoScore> h = OKDocsWithScore.get(qno);
			  h.add(ds);
			  OKDocsWithScore.put(qno, h);
		  }
		}
}
	}
	
	//A6_TFIDF
	
	
	File ft = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/A6_TFIDF.txt");
	BufferedReader br6= new BufferedReader(new InputStreamReader(new FileInputStream(ft)));
	HashMap<String ,HashSet<DnoScore>> TFDocsWithScore = new HashMap<String ,HashSet<DnoScore>>();
	
	while( (line1 = br6.readLine()) != null){
		String[] strSp = line1.split("\\s+");
		
		if(strSp.length >= 4){
		String qno = strSp[0];
		String DocNo = strSp[2];
		DocNo = DocNo.trim();
		String score = strSp[4];
		float s = Float.parseFloat(score);
		//System.out.println(qno);
		HashSet<docnoRel> htemp = Docs.get(qno);
		DnoScore ds = new DnoScore(DocNo, s);
		int boo = 0;
		for(docnoRel dr: htemp){
			if(dr.docno.equals(DocNo)){
				boo = 1;
			}
		}
		if(boo == 1){
			
		  if(TFDocsWithScore.get(qno) == null){
			  HashSet<DnoScore> h = new HashSet<DnoScore>();
			  h.add(ds);
			  TFDocsWithScore.put(qno, h);
		  }
		  else{
			  HashSet<DnoScore> h = TFDocsWithScore.get(qno);
			  h.add(ds);
			  TFDocsWithScore.put(qno, h);
		  }
		}
}
	}
	
	System.out.println(BM25DocsWithScore.size());
	System.out.println(LSDocsWithScore.size());
	System.out.println(PSDocsWithScore.size());
	System.out.println(OKDocsWithScore.size());
	
	int cntbm = 0;
	int cntls = 0;
	int cntps = 0;
	int cntok = 0;
	int cnttf = 0;
	int cnttot = 0;
	 PrintWriter writer = new PrintWriter("HW6MyMatrix.txt", "UTF-8");
	for(Map.Entry m:Docs.entrySet()){
		HashSet<docnoRel> drh = (HashSet<docnoRel>) m.getValue();
		String qno = (String)m.getKey();
		for(docnoRel dr : drh){
			float rel = dr.relavence;
			String dno = dr.docno;
			float scoreBM = getScore( qno,  dno, BM25DocsWithScore);
			if(scoreBM != 10000){
				cntbm = cntbm + 1;
				float scoreLS = getScore( qno,  dno, LSDocsWithScore);
				if(scoreLS != 10000){
					cntls = cntls + 1;
				}
				//PSDocsWithScore
				float scorePS = getScore( qno,  dno, PSDocsWithScore);
				if(scorePS != 10000){
					cntps = cntps + 1;
				}
				
				float scoreOK = getScore( qno,  dno, OKDocsWithScore);
				if(scoreOK != 10000){
					cntok = cntok + 1;
				}
				//TFDocsWithScore
				float scoreTF = getScore( qno,  dno, TFDocsWithScore);
				if(scoreOK != 10000){
					cnttf = cnttf + 1;
				}
				if(scorePS == 10000){
					scorePS = 0;
				}
				
				writer.println(qno+"\t"+dno+"\t"+scoreBM+"\t"+scoreLS+"\t"+scoreOK+"\t"+scorePS+"\t"+scoreTF+"\t"+rel);
				cnttot = cnttot + 1;
			}
		}
	}
	System.out.println(cntbm);
	System.out.println(cntls);

	System.out.println(cntok);
	System.out.println(cntps);
	System.out.println(cnttf);
	System.out.println(cnttot);
	writer.close();
	
	//---------LIB LINERAR -------------------
	
	
	
	}

	private static float getScore(String qno, String dno, HashMap<String, HashSet<DnoScore>> ModelDocsWithScore) {
		HashSet<DnoScore> hsdnos = ModelDocsWithScore.get(qno);
		if(hsdnos != null){
		for(DnoScore dnos : hsdnos){
			if(dnos.docno.equals(dno)){
				return dnos.score;
			}
		}
		}
		return 10000;
	}
	
	
		
	
}
