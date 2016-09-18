package anvitaA5;


import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
public class MyRankListData {
	
	

	public  String author ;
	public  String url ;
	public Float rank;
	public  Float relevance ;

	public MyRankListData(String u1, String a1,Float rk, Float r1){
		author = a1;
		url = u1;
		rank = rk;
		relevance = r1;
		

	
	}


}



class MyRankComp implements Comparator<MyRankListData>{
	 
    @Override
    public int compare(MyRankListData e1, MyRankListData e2) {
        if(e1.rank > e2.rank){
            return 1;
        } else {
            return -1;
        }
    }
}

