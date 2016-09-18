package anvitaA6;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.io.IOException;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;

public class AnviLinear {
	public static void main(String a[]) throws NumberFormatException, IOException{
		File f = new File("/Users/anvitasurapaneni/Documents/workspace/CS6200/HW6MyMatrix.txt");
		BufferedReader br1= new BufferedReader(new InputStreamReader(new FileInputStream(f)));
		HashMap<String ,HashSet<DnoAttr>> Matrix = new HashMap<String ,HashSet<DnoAttr>>();
		
		
		String line1;
		
		while( (line1 = br1.readLine()) != null){
			String[] strSp = line1.split("\\s+");
			String qno = strSp[0];
			
			String DocNo = strSp[1];
			DocNo = DocNo.trim();
			
			String bm = strSp[2];
			float bmf = Float.parseFloat(bm);
			
			String ls = strSp[3];
			float lsf = Float.parseFloat(ls);
			
			String ok = strSp[4];
			float okf = Float.parseFloat(ok);
			
			String ps = strSp[5];
			float psf = Float.parseFloat(ps);
			
			String tf = strSp[6];
			float tff = Float.parseFloat(ps);
			
			String rel = strSp[7];
			float relf = Float.parseFloat(rel);
			
			DnoAttr dr = new DnoAttr(DocNo, bmf, lsf, okf, psf, tff, relf);
			
			  if(Matrix.get(qno) == null){
				  HashSet<DnoAttr> h = new HashSet<DnoAttr>();
				  h.add(dr);
				  Matrix.put(qno, h);
			  }
			  else{
				  HashSet<DnoAttr> h = Matrix.get(qno);
				  h.add(dr);
				  Matrix.put(qno, h);
			  }
	}
		
		System.out.println(Matrix.size());
		HashMap<String ,HashSet<DnoAttr>> TrainingMatrix = new HashMap<String ,HashSet<DnoAttr>>();
		HashMap<String ,HashSet<DnoAttr>> TestingMatrix = new HashMap<String ,HashSet<DnoAttr>>();
		
	int c = 0;	
	int valtot = 0;
		for(Map.Entry m1:Matrix.entrySet()){
			c = c + 1;
			
			String key = (String) m1.getKey();
			//System.out.println(key);
			HashSet<DnoAttr> val = (HashSet<DnoAttr>) m1.getValue();
	
			if(!(key.equals("56")||key.equals("57")||key.equals("64")||key.equals("71")||key.equals("99"))){
				TrainingMatrix.put(key, val);
				valtot = valtot + val.size();
			}
			else{
				TestingMatrix.put(key, val);
			}
		}
		System.out.println(TrainingMatrix.size());
		System.out.println(TestingMatrix.size());
		
		

		double[] GROUPS_ARRAY = new double[5519]  ;
		 FeatureNode[][] trainingSetWithUnknown = new FeatureNode[5519][]; 
		
		Problem problem = new Problem();
		
        
		int j = 0;
		for(Map.Entry m1:TrainingMatrix.entrySet()){
			String qno = (String)m1.getKey();
			HashSet<DnoAttr> valH = (HashSet<DnoAttr>)m1.getValue();
				
for(DnoAttr attr: valH){
	if(j < 5519){
	
	j = j + 1;	
	//System.out.println(attr.scoreBM);
 FeatureNode[] tp5 = {new FeatureNode(1,  attr.scoreBM),  new FeatureNode(2,  attr.scoreLS),new FeatureNode(3,  attr.scoreOK) , new FeatureNode(4,  attr.scorePS), new FeatureNode(5,  attr.scoreTF) };
//System.out.println(j+"  "+tp5.length);
//			 trainingSetWithUnknown[j][1] = new FeatureNode(1,  attr.scoreBM);
//			 trainingSetWithUnknown[j][2] = new FeatureNode(2,  attr.scoreLS);
//			 trainingSetWithUnknown[j][3] = new FeatureNode(3,  attr.scoreOK);
 trainingSetWithUnknown[j-1] = tp5;
double h = 1;
 if(attr.relavence == 0){
	 h = -1;
 }
			 GROUPS_ARRAY[j-1] = attr.relavence;
}
		}
		}
		


		
		problem.l = (5519);
		problem.n = 5;
		 problem.x = trainingSetWithUnknown;
		    problem.y = GROUPS_ARRAY;
		
	  
		    SolverType solver = SolverType.L2R_LR; // -s 0
		    double C = 1.0;    // cost of constraints violation
		    double eps = 0.01; // stopping criteria

		    de.bwaldvogel.liblinear.Parameter parameter = new de.bwaldvogel.liblinear.Parameter(solver, C, eps);
		 //   System.out.println(parameter);
		    Model model = de.bwaldvogel.liblinear.Linear.train(problem, parameter);
		    File modelFile = new File("model");
		    model.save(modelFile);
		    
		    model = Model.load(modelFile);

//		    Feature[] instance = { new FeatureNode(1, 0), new FeatureNode(2, 0),  new FeatureNode(3, 0) };
//		    double prediction = de.bwaldvogel.liblinear.Linear.predict(model, instance);
//		    
//		    System.out.println("prediction : " + prediction);   
		    float total = 0;
		    float match = 0;
		    HashMap<String, Float> hm = new HashMap<String, Float>();

			PrintWriter writer = new PrintWriter("LapSm_testing.txt", "UTF-8");
//		    
			for(Map.Entry m1:TestingMatrix.entrySet()){
				String qno = (String)m1.getKey();
				HashSet<DnoAttr> valH = (HashSet<DnoAttr>)m1.getValue();
					
	for(DnoAttr attr: valH){
	
		//System.out.println(attr.scoreBM);
		Feature[] instance={new FeatureNode(1,  attr.scoreBM),  new FeatureNode(2,  attr.scoreLS),new FeatureNode(3,  attr.scoreOK) ,new FeatureNode(4,  attr.scorePS), new FeatureNode(5,  attr.scoreTF) };
		double prediction = de.bwaldvogel.liblinear.Linear.predict(model, instance);
	//	System.out.println(prediction);
		hm.put(attr.docno, (float)prediction);
//			
	
				

			}
	HashMap<String, Float> hms = sortHM(hm);
	
	 int rank = 0;
	  
	    for(Map.Entry sm:hms.entrySet())
		{  
		

			rank = rank + 1;
			writer.println(qno+"  Q0  "+sm.getKey()+"  "+rank+"  "+sm.getValue()+"  LAPLACE  ");

			
		}
		

	    hms.clear();
	    hm.clear();
	
			}
			
			//training

System.out.println("--------------------------------------------------------------");
		  
		    hm.clear();
			PrintWriter writerT = new PrintWriter("LapSm_training.txt", "UTF-8");
//		    
			for(Map.Entry m1:TrainingMatrix.entrySet()){
				String qno = (String)m1.getKey();
				HashSet<DnoAttr> valH = (HashSet<DnoAttr>)m1.getValue();
					
	for(DnoAttr attr: valH){
	
	
		Feature[] instance={new FeatureNode(1,  attr.scoreBM),  new FeatureNode(2,  attr.scoreLS),new FeatureNode(3,  attr.scoreOK) ,new FeatureNode(4,  attr.scorePS) , new FeatureNode(5,  attr.scoreTF)};
		double prediction = de.bwaldvogel.liblinear.Linear.predict(model, instance);
		//System.out.println("training"+prediction);
		hm.put(attr.docno, (float)prediction);
//			
	
				

			}
	HashMap<String, Float> hms = sortHM(hm);
	
	 int rank = 0;
	  
	    for(Map.Entry sm:hms.entrySet())
		{  
		

			rank = rank + 1;
			writerT.println(qno+"  Q0  "+sm.getKey()+"  "+rank+"  "+sm.getValue()+"  LAPLACE  ");

			
		}
		

	    hms.clear();
	    hm.clear();
	
			}

	writer.close();	
	writerT.close();
	}

	
private static HashMap<String, Float> sortHM(Map<String, Float> aMap) {
        
        Set<Entry<String,Float>> mapEntries = aMap.entrySet();
       List<Entry<String,Float>> aList = new LinkedList<Entry<String,Float>>(mapEntries);

        Collections.sort(aList, new Comparator<Entry<String,Float>>() {

            
            public int compare(Entry<String, Float> ele1,
                    Entry<String, Float> ele2) {
                
                return ele2.getValue().compareTo(ele1.getValue());
            }
        });
       
        Map<String,Float> aMap2 = new LinkedHashMap<String, Float>();
        for(Entry<String,Float> entry: aList) {
            aMap2.put(entry.getKey(), entry.getValue());
        }
      
            return (HashMap<String, Float>) aMap2;
        }
}