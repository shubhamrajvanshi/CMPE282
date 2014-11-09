package problem2;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Date;
import java.util.StringTokenizer;

public class problem2_javacode {
	

		public static void main(String[] args) throws Exception {
			// TODO Auto-generated method stub
			Date start=new Date();
			System.out.println("Start........................... Program start time: "+start);
			BufferedReader br = new BufferedReader(new FileReader("input/Holmes.txt"));
			String line = br.readLine();
	    	
			
			int final_count=0;
			while (line != null) {
				int count1=0;
			    int count2=0;
			    int i=0;
			    line=line.toLowerCase();
			    line=line.replaceAll("([!?.;,'_\"*()$#/-:@])", "");
			    String[] contents = new String[100];
			    StringTokenizer tokenizer = new StringTokenizer(line);
				 while (tokenizer.hasMoreTokens()) {
					 String temp=tokenizer.nextToken();
			    	  
		    		 if(temp.equals("sherlock")){
		    			 contents[i]=temp;
		    			 i++;
		    		 }
		    		 if(temp.equals("holmes")){
		    			 contents[i]=temp;
		    			 i++;
		    		 }
				 } // end of while loop
			       if(contents!=null && i>0){
			       for (String x :contents){
			    	  
			    	 if(x!=null){
			    	  if(x.equalsIgnoreCase("sherlock")){count1++;}
			    	  if(x.equalsIgnoreCase("holmes")){count2++;}
			    	 }
			      }
			       if(count1>=1 && count2>=1){
			    	   
			    	   final_count++;
			       }
			      
			       }
			       line = br.readLine(); 
		       }
			System.out.println("Sherlock and holmes together in the file: "+final_count);
			Date end=new Date();
		     
		     System.out.println("End............................. Program end time: "+end);
		     
		     Long diff=(end.getTime()-start.getTime());
		     
		     System.out.println("The program Word_count started at: "+start+" , and ended at: "+end);
		     System.out.println("Time Difference....................... Program took: "+diff+" milliseconds.");
			}
		}


