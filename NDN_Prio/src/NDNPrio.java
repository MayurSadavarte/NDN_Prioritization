import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.ccnx.ccn.CCNContentHandler;
import org.ccnx.ccn.CCNHandle;
import org.ccnx.ccn.CCNInterestHandler;
import org.ccnx.ccn.io.CCNInputStream;
import org.ccnx.ccn.io.CCNOutputStream;
import org.ccnx.ccn.io.RepositoryOutputStream;
import org.ccnx.ccn.io.RepositoryVersionedOutputStream;
import org.ccnx.ccn.profiles.nameenum.BasicNameEnumeratorListener;
import org.ccnx.ccn.profiles.nameenum.CCNNameEnumerator;
import org.ccnx.ccn.protocol.ContentName;
import org.ccnx.ccn.protocol.ContentObject;
import org.ccnx.ccn.protocol.Interest;



public class NDNPrio implements CCNInterestHandler ,BasicNameEnumeratorListener{
	CCNHandle puthandle;
    CCNHandle repohandle;
    CCNNameEnumerator ccnNE;
    
    boolean enumerationDone = false;
    int limit = 10;
    int counter = 0;
    
    String tweetDir = "/media/Data/study/study_sub_wise/cps/NDNTwitter/miniSet";
    
    SortedSet<ContentName> allNames;
    HashMap<String, List<String>> listMap = new HashMap<String, List<String>>();
    
    
    public static void main(String[] args) {
    	System.out.println("In main");
        NDNPrio obj = new NDNPrio();
        
        obj.getHandles();
        
        //ndnTweetParser.parseAndInsertTweetsInRepo(obj.tweetDir, obj.repohandle);
        //obj.insertToRepo();
       
        obj.registerInterest();

        
        while(true) {
        	
        }
        //obj.testInterest();
        
        //obj.readRepo();
        //obj.enumerateNames();
    }
    
    public void getHandles(){
    	try{
    		puthandle = CCNHandle.open();
    		repohandle = CCNHandle.open();
    		
    		ccnNE = new CCNNameEnumerator(repohandle, this);
    	}catch(Exception e){
    		System.out.println("Exception:"+e.getMessage());
    	}    	
    }
    
    public void registerInterest(){
    	System.out.println("Server registering for serving interest - " + "/ndn/tweet");
    	
    	try{    		
    		puthandle.registerFilter(ContentName.fromNative("/ndn/tweet"), this);
    	}catch(Exception e){
    		System.out.println("Exception:"+e.getMessage());
    	}
    }
    
    public void unregisterInterest(){
    	try{    		
    		puthandle.unregisterFilter(ContentName.fromNative("/ndn/tweet"), this);
    	}catch(Exception e){
    		System.out.println("Exception:"+e.getMessage());
    	}
    }
    
    
    /*public void testInterest(){
    	try{	    	
	    	Interest interestSent = new Interest(ContentName.fromNative("/ndn/tweet/simpleTest"));
	       	gethandle.expressInterest(interestSent, this);
	       	counter++;
    	}catch(Exception e){
    		System.out.println("Exception:"+e.getMessage());
    	}    
    }*/
    
	public boolean handleInterest(Interest interest) {
		String interestName = interest.getContentName().toString();
		
		System.out.println("Interest received !!!, contentName received - " + interestName);
		
		String[] tokens = interestName.split("/");
		String CPrefix = "";
		int CIndex = 0;
		String indexStr = tokens[tokens.length - 1];
		CPrefix = interestName.substring(0, interestName.length() - indexStr.length() - 1);
				
		
		try {
			CIndex = Integer.parseInt(indexStr);
		} catch (NumberFormatException e) {
			System.out.println("Wrong sequence number received in the interest - " + CIndex);
			return false;
		}
		
		String priotizedContent = getContentObject(CPrefix, CIndex);
		System.out.println("Priotized Content Received!!! Will build a content object now.");
		
		try{
			//System.out.println("Building content !!!");
		
			ContentObject co = ContentObject.buildContentObject(interest.getContentName(), readRepo(priotizedContent));
			
			puthandle.put(co);		//WHY GETHANDLE??
			
			System.out.println("Interest Satisfied!!");
			
		}catch(Exception e){
			System.err.println("Exception handling interest");
		}
		return true;
	}

	
	private String getContentObject(String cPrefix, int cIndex) {
		String ContentObject = null;
		List<String> priorityList;
		
		if (listMap.containsKey(cPrefix)) {
			System.out.println("Priotized List for the prefix - " + cPrefix + " already exists!! Lucky YOU!!");
			priorityList = listMap.get(cPrefix);

		} else {
			System.out.println("Priotized List for the prefix - " + cPrefix + " doesn't exist! Bad Luck!!, Don't worry, Will create one for you!!");
			priorityList = GeneratePriorityList(cPrefix);
		}

		if(cIndex < priorityList.size()) {
			ContentObject = priorityList.get(cIndex);
		} else {
			ContentObject = "EOD";
		}
		
		return ContentObject;
	}

	
	private List<String> GeneratePriorityList(String CPrefix) {
		List<String> priorityList = new ArrayList<String>();
		
		System.out.println("Initializing Occupancy Counts for subtree under prefix - " + CPrefix);
		HashMap<String, Integer> OccupancyCount = InitOccupancyCount(CPrefix);
		System.out.println("Initializing Occupancy Counts for subtree under prefix - " + CPrefix + " Done");
		
		String nextContentName = CPrefix;
		int count = 0;
		
		while(true) {
			count++;
			System.out.println("****************   Generating prioritized list - " + count + " ******************");			
			nextContentName = getNextContentName(CPrefix, OccupancyCount);
			if(nextContentName == null)
				break;
			
			priorityList.add(nextContentName);
		}
		
		listMap.put(CPrefix, priorityList);
		
		return priorityList;
	}

	
	private String getNextContentName(String CPrefix,
			HashMap<String, Integer> OccupancyCount) {
		String basePrefix = CPrefix;
		
		String curPrefix = CPrefix;
		
		while(true) {

			if (!OccupancyCount.containsKey(curPrefix)) {
				System.err.println("OccupancyCount data structure doesn't have key - " + curPrefix);
				break;
			}
			Integer currentCnt = OccupancyCount.get(curPrefix);
			OccupancyCount.put(curPrefix, currentCnt+1);				
			
			enumerateNames(curPrefix);
			
			synchronized (allNames) {
				while(!enumerationDone) {
					try {
						allNames.wait();
					} catch (InterruptedException e) {
						System.err.println("Enumeration didn't work properly!!");
					}
				}
				enumerationDone = false;
				
				//System.out.println(c.toString().replaceFirst("/", ""));
				String minName = getMinOccCntName(curPrefix, OccupancyCount);	
								
				if(minName == null) {
					if(currentCnt.intValue() == 0) {
						OccupancyCount.put(curPrefix, currentCnt+1);
						break;
					} else {
						curPrefix = null;
						break;
					}
				}	
				curPrefix = minName;
			}              			
			
		}
		
		return curPrefix;
	}


	
	private String getMinOccCntName(String curPrefix, HashMap<String, Integer> OccupancyCount) {

		Integer minCnt = Integer.MAX_VALUE;
		String minPrefix = null;
		Integer curCnt = 0;
		

		for (ContentName c : allNames){
			//System.out.println("getMinOccCntName called with - " + c.toString());
			if(c.toString().startsWith("/%")) {
				System.out.println("Reached leaf node - found following data item - " + c.toString());
				break;
			}
			curCnt = OccupancyCount.get(curPrefix + c.toString());
			if (curCnt < minCnt) {
				minCnt = curCnt;
				minPrefix = curPrefix + c.toString();
			}
		}      			
		
		return minPrefix;
	}


	
	private HashMap<String, Integer> InitOccupancyCount(String CPrefix) {
		HashMap<String, Integer> OccupancyCount = new HashMap<String, Integer>();
		List<String> prefixes = new ArrayList<String>();
		
		prefixes.add(CPrefix);
		int index = 0;
		
		//String basePrefixStr = CPrefix;
		
		String Current;
		while(index < prefixes.size()) {
			Current = prefixes.get(index);
			
			OccupancyCount.put(Current, new Integer(0));
			enumerateNames(Current);

			synchronized (allNames) {
				while(!enumerationDone) {
					try {
						allNames.wait();
					} catch (InterruptedException e) {
						System.err.println("Enumeration didn't work properly!!");
					}
				}
				enumerationDone = false;
				
				for (ContentName c : allNames){
					if(c.toString().startsWith("/%")) {
						System.out.println("Reached Leaf node in initOccCnt for node - " + c.toString());
						break;
					}
					System.out.println(c.toString().replaceFirst("/", ""));
					//names.append(c.toString().replaceFirst("/", ""));		//WHY?
					//names.append(c.toString());
					//names.append(",");
					prefixes.add(Current + c.toString());
				}
			}
			
			index++;
		}
		
		return OccupancyCount;
	}

	
	/*public Interest handleContent(ContentObject data,
			Interest interest) {		
		if(counter <= limit){
			System.out.println("Received Content-" + counter);
			this.testInterest();
			//NDNPrio obj = new NDNPrio();
	        //obj.testInterest();
		}
		else{
	        this.unregisterInterest();
		}
		return null;
	}*/
	
	/*
	public void insertToRepo(){
		String[] names = new String[20];
    	names[0] = "/ndn/greengps/zip1/st1";
    	names[1] = "/ndn/greengps/zip1/st2";
    	names[2] = "/ndn/greengps/zip2/st3";
    	names[3] = "/ndn/greengps/zip2/st4";
    	
    	try{
    		for(int l=0;l<4;l++){
	    		ContentName nodeName = ContentName.fromNative(names[l]);
	    		CCNOutputStream ostream = new RepositoryOutputStream(nodeName, handle);
	    		
    	    	int size = 4000;
    			byte [] buffer = new byte[size];
    			byte value = 'm';
    			
    			for (int i = 0; i < size; i++)
    				buffer[i] = value;
    			
    			ostream.write(buffer, 0, size);
    			
    			ostream.close(); 
	    	}   	
	    	System.out.println("****Data Inserted successfully****");    		
    	}catch(Exception e){
    			System.err.println("Exception handling interest");
    	}    	
	}
	*/
	
	public byte[] readRepo(String cname) {
		
		byte[] testBytes = new byte[4000];
		Arrays.fill(testBytes, (byte)'0');
		
		if(cname.equals("EOD")) {
			System.arraycopy("EOD".getBytes(), 0, testBytes, 0, 3);
			return testBytes;
		}
		
		try{
			ContentName name = ContentName.fromNative(cname);
			Thread.sleep(5000);
			CCNInputStream input = new CCNInputStream(name);
			
			input.read(testBytes,0,4000);
			
			System.out.println("Data read:"+new String(testBytes));
		}catch(Exception e){
			System.err.println("Exception:"+e.getMessage());
		}
		
		return testBytes;
	}
	
	
	public void enumerateNames(String CPrefix) {
		ContentName name = null;		
		StringBuffer names = new StringBuffer();
		try{
			name = ContentName.fromNative(CPrefix);
			//name = ContentName.fromURI("ccnx:/ndn/greengps");			//EXPERIMENT

			allNames = new TreeSet<ContentName>();
			
			ccnNE.registerPrefix(name);
			//Thread.sleep(1000);

			/*synchronized (allNames) {
				for (ContentName c : allNames){
					System.out.println(c.toString().replaceFirst("/", ""));
					//names.append(c.toString().replaceFirst("/", ""));		//WHY?
					names.append(c.toString());
					names.append(",");
				}
			}*/               
		}
        catch(Exception e){
            System.out.println("Exception in name enum:"+e.getMessage());
            e.printStackTrace();
        }
	}
	
	
	public int handleNameEnumerator(ContentName prefix, ArrayList<ContentName> names){
		ccnNE.cancelPrefix(prefix);
		
		synchronized(allNames){
			allNames.addAll(names);
			enumerationDone = true;
			allNames.notifyAll();
		}
		return 0;
	}

}
