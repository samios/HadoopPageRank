
import java.util.ArrayList;

public class RankRecord {
	public int sourceUrl;
	public double rankValue;
	public ArrayList<Integer> targetUrlsList;
	
	public RankRecord(String strLine){
		String[] strArray = strLine.split(" ");
		sourceUrl = Integer.parseInt(strArray[0]);
		rankValue = Double.parseDouble(strArray[1]);
		targetUrlsList = new ArrayList<Integer>();
		for (int i=2;i<strArray.length;i++){
			if(Integer.parseInt(strArray[i])==1)
			targetUrlsList.add(i);
		}
	}
}