package org.hathitrust.extractedfeatures;


import java.util.Scanner;

public class TestWhitelistDictionaryMain {
	
	public static void main(String[] args) 
	{
		Scanner scan = new Scanner(System.in);
		System.out.println("Press enter to start:");
		String wait = scan.nextLine();
		
		String dictionary_filename = "whitelist-placeholder.txt";
		
			
		TestWhitelistBloomFilter whitelist = new TestWhitelistBloomFilter(dictionary_filename);
		//TestWhitelistHashmap whitelist = new TestWhitelistHashmap(dictionary_filename);
		
		whitelist.storeEntries();
				
		System.out.println("Enter words to test (enter '.quit' to quit program");
		
		boolean quit = false;
		while (!quit) {
			String test_word = scan.next();
			boolean might_contain_test = whitelist.contains(test_word);
			if (test_word.equals(".quit")) {
				quit = true;
			}
			else {
				System.out.println("Whitelist ["+test_word+"] = " + might_contain_test);
			}
		}
		scan.close();
		System.out.println("Program ended.");
	}

}
