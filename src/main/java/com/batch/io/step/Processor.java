package com.batch.io.step;



import org.springframework.batch.item.ItemProcessor;

import com.batch.io.model.CustomeFileReader;
/* this class will encrypt the lines */
public class Processor implements ItemProcessor<CustomeFileReader,CustomeFileReader> {



	private String threadName;


	public String getThreadName() {
		return threadName;
	}

	public void setThreadName(String threadName) {
		this.threadName = threadName;
	}


	@Override
	public CustomeFileReader process(CustomeFileReader reader) throws Exception {
		String line=reader.getFileline();
		String encryptedline=encrypt(line);
		CustomeFileReader eline=new CustomeFileReader(encryptedline);

		System.out.println(threadName+" processing :"+"Encrypting " +line+ "to"+ encryptedline);
		return eline;
	}

	private String encrypt(String line){
		String cipherline = "";
		char alphabet;
		int shift=2;


		for(int i=0; i < line.length();i++) 
		{
			alphabet = line.charAt(i);
			if(alphabet >= 'a' && alphabet <= 'z') 
			{

				alphabet = (char) (alphabet + shift);
				if(alphabet > 'z') {
					alphabet = (char) (alphabet+'a'-'z'-1);
				}
				cipherline = cipherline + alphabet;
			}
			else if(alphabet >= 'A' && alphabet <= 'Z') {
				alphabet = (char) (alphabet + shift);    
				if(alphabet > 'Z') {
					alphabet = (char) (alphabet+'A'-'Z'-1);
				}
				cipherline = cipherline + alphabet;
			}
			else {
				cipherline = cipherline + alphabet;   
			}


		}
		return cipherline;

	}

}






