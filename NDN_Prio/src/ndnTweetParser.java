/*
 * this class will act as a mail class of the parser module
 * it will receive a root directory name as its argument
 * it will go hierarchically
 * and at the leaves it will traverse through the file and parse it to get tweets
 * and will add tweets in the hashmap which is stored at object level.
 */

import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.ccnx.ccn.CCNHandle;
import org.ccnx.ccn.io.CCNOutputStream;
import org.ccnx.ccn.io.RepositoryOutputStream;
import org.ccnx.ccn.protocol.ContentName;
import org.ccnx.ccn.protocol.MalformedContentNameStringException;

public class ndnTweetParser {
	private HashMap<String, String> tweetMap = null;
	private long sequenceNumber = 0;
	
	public static ndnTweetParser initParser() {
		ndnTweetParser myParser = new ndnTweetParser();
		return myParser;
	}
	
	private void addEntryToMap(String key, String val, CCNHandle handle) {
		//System.out.println("addEntryToMap called for key -"+key+", and value - "+val);
		if(tweetMap == null) {
			tweetMap = new HashMap<String, String>();
		}
		if(tweetMap.containsKey(key)) {
			System.out.println("The tweetMap already contains key - "+key+" hence won't add it again!");
			return;
		} else {
			tweetMap.put(key, val);
			
			try {
				ContentName nodeName = ContentName.fromNative(key);
				CCNOutputStream ostream = new RepositoryOutputStream(nodeName, handle);
				byte[] tweetBuf = new byte[4000];
				Arrays.fill(tweetBuf, (byte)'0');
				
				byte[] tBytes = val.getBytes();
				System.arraycopy(tBytes, 0, tweetBuf, 0, tBytes.length);
				
    			ostream.write(tweetBuf, 0, tweetBuf.length);
    			
    			ostream.close();				
			} catch (MalformedContentNameStringException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	private String lastTwoCharsInToken(String token) {
		if(token.length() < 2) {
			return "??";
		} else {
			return token.subSequence(token.length()-2, token.length()).toString();
		}
	}
	
	private void parseIndividualFile(File finalFile, String ndnPath, String rootDirStr, CCNHandle handle) {
		int perFileCnt = 0;
		String fullPath = finalFile.getAbsolutePath();
		fullPath = fullPath.substring(rootDirStr.length(), fullPath.length());
		fullPath = ndnPath + fullPath;
		System.out.println("parseIndividualFile called for file - "+finalFile.getName());
		try {
			Scanner sc = new Scanner(finalFile);
			sc.findInLine("\"top_claims\": ");
			System.out.println("\"top_claims\" found in file - "+finalFile.getName());
			while(sc.hasNext()) {
				String token = sc.next();
				if(token.equals("\"claim_desc\":")) {
					String fullTweet = "";
					
					while(!lastTwoCharsInToken(token).equals("},")) {		//error condition needs to be handled here
						token = sc.next();
						fullTweet = fullTweet + " " + token;
					}	
					String tweet = fullTweet.substring(1, fullTweet.length()-2);	//error condition needs to be handled here
					this.sequenceNumber++;
					addEntryToMap(fullPath+"/"+String.valueOf(this.sequenceNumber), tweet, handle);
					perFileCnt++;
				}
			}
			sc.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("File " + finalFile.getName() + " contains '" + perFileCnt+ "'tweets");
	}
	
	public void startParsing(File directory, String ndnPath, String rootDirStr, CCNHandle handle) {
		
		
		for(File indiDirOrFile : directory.listFiles()) {
			if(!indiDirOrFile.exists()) {
				System.out.println("File "+indiDirOrFile.toString()+" doesn't exist or problem in accessibility!");
				continue;
			}
			//File/Directory is there and accessible
			if (indiDirOrFile.isFile()) {
					parseIndividualFile(indiDirOrFile, ndnPath, rootDirStr, handle);
				
			} else if (indiDirOrFile.isDirectory()) {
					startParsing(indiDirOrFile, ndnPath, rootDirStr, handle);
				
			}
			
		}
		
	}
	
	void printMap() {
		if(tweetMap == null) {
			System.out.println("tweetMap is null..please debug!!");
			return;
		} else {
			System.out.print(tweetMap.toString());
		}
	}
	
	
	public static void parseAndInsertTweetsInRepo(String tweetDir, CCNHandle handle) {
		/*if(args.length > 1) {
			System.out.println("Wrong usage..corrent usage is - ./<program.java> <tweets_directory_name>");
			return;
		}*/
		File rootDir = new File(tweetDir);
		if(!rootDir.exists()) {
			System.out.println("Some problem with - "+rootDir.toString());
			return;
		}
		String rootDirStr = rootDir.getAbsolutePath();
		
		ndnTweetParser myParser = null;
		File thisDir = new File(".");
		String thisDirStr = thisDir.getAbsolutePath();
		System.out.println("The current directory in which the program is running is - "+thisDirStr);
		
		myParser = initParser();
		
		File virtualFile = new File("/ndn/tweet");	//might seem a little weird, but there is a reason behind doing like this
		
		myParser.startParsing(rootDir, virtualFile.toString(), rootDirStr, handle);
		
		//System.out.println("Parsing is done!! lets see the formed hashmap - ");
		
		//myParser.printMap();
	}
	/*public static void main(String[] args) {
		if(args.length > 1) {
			System.out.println("Wrong usage..corrent usage is - ./<program.java> <tweets_directory_name>");
			return;
		}
		File rootDir = new File(args[0]);
		if(!rootDir.exists()) {
			System.out.println("Some problem with - "+rootDir.toString());
			return;
		}
		String rootDirStr = rootDir.getAbsolutePath();
		
		ndnTweetParser myParser = null;
		File thisDir = new File(".");
		String thisDirStr = thisDir.getAbsolutePath();
		System.out.println("The current directory in which the program is running is - "+thisDirStr);
		
		myParser = initMainClass();
		
		File virtualFile = new File("/ndn/tweet");	//might seem a little weird, but there is a reason behind doing like this
		
		myParser.startParsing(rootDir, virtualFile.toString(), rootDirStr);
		
		//System.out.println("Parsing is done!! lets see the formed hashmap - ");
		
		//myParser.printMap();
	}*/
}
