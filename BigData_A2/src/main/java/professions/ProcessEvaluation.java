package professions;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class ProcessEvaluation {
	
	File file;
	public ProcessEvaluation(File file){
		//should only be two things in this file: num successes, num fails
		this.file = file;
		
	}
	public int percentage() throws FileNotFoundException{
		Scanner sc = new Scanner(file);
		int successes = 0;
		int total = 0;
		while (sc.hasNextLine()){
			String line = sc.nextLine();
			StringTokenizer lineItr = new StringTokenizer(line);
			while(lineItr.hasMoreTokens()){
				int outcome = Integer.parseInt(lineItr.nextToken());
				int size = Integer.parseInt(lineItr.nextToken());
				if(outcome == 1){
					//Success
					successes = size;
					total = total + size;
				}
				else{
					total = total + size;
				}
			}
		}
		sc.close();
		return successes/total;
	}
	public static void main(String[] args) throws FileNotFoundException{
		String evalPath = args[0];
		ProcessEvaluation pe = new ProcessEvaluation(new File(evalPath));
		System.out.println(pe.percentage());
	}
}
