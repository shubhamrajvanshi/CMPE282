package convert;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


/**
 cat u.data | cut -f1,2,3 | tr "\\t" "," --- in Unix systems
*/
public class ConvertMovieLens {

	
	public static void main(String args[]) throws IOException{
		BufferedReader br = new BufferedReader(new FileReader("data/users.dat"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("data/users.csv"));
		
		String line;
		while((line = br.readLine())!= null){
			String[] values = line.split("::", -1);
			bw.write(values[0]+ ","+values[1]+","+values[2]+","+values[3]+","+values[4]+"\n");
			
		}
		br.close();
		bw.close();
	}
}
