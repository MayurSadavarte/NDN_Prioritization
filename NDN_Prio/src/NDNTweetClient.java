import java.io.IOException;
import java.util.ArrayList;

import org.ccnx.ccn.CCNContentHandler;
import org.ccnx.ccn.CCNHandle;
import org.ccnx.ccn.config.ConfigurationException;
import org.ccnx.ccn.protocol.ContentName;
import org.ccnx.ccn.protocol.ContentObject;
import org.ccnx.ccn.protocol.Interest;
import org.ccnx.ccn.protocol.MalformedContentNameStringException;



public class NDNTweetClient implements CCNContentHandler {
	CCNHandle gethandle;
	int limit = 100, counter = 0;
	private ContentName namespace;
    private String namespaceStr;
    private String baseNameSpace;
    private String currentTopic = null;
	
	private ArrayList<String> tweets = new ArrayList<String>();
	
	private boolean enoughTweetsRecvd = false;
	
    public static void main(String[] args) {
    	
    	System.out.println("In client main");
    	
    	NDNTweetClient ntclient = null;
    	String namespace = "/ndn/tweet";
    	
		try {
			ntclient = new NDNTweetClient(namespace);
		} catch (MalformedContentNameStringException e) {
			System.err.println("Not a valid ccn URI: " + args[0] + ": " + e.getMessage());
			e.printStackTrace();
		} catch (ConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ntclient.setNewTopic("egypt-election");
		ntclient.nextInterest();   	
        
		while( !ntclient.enoughTweetsRecvd ) {
			
		}
		
        //obj.readRepo();
        //obj.enumerateNames();
    }
    
    
	protected NDNTweetClient(String namespace) throws MalformedContentNameStringException, ConfigurationException, IOException {
		baseNameSpace = namespace;
		gethandle = CCNHandle.open();
	}
	
	void setNewTopic(String topic) {
		currentTopic = topic;
	}
	
	String getCurrentTopic(String topic) {
		return currentTopic;
	}
    
	public void getHandles(){
    	try{
    		gethandle = CCNHandle.open();
    	}catch(Exception e){
    		System.out.println("Exception:"+e.getMessage());
    	}    	
    }
	
	public void nextInterest(){
		
		String prefixNS = baseNameSpace + "/" + currentTopic + "/" + String.valueOf(counter++); 
		
		System.out.println("Sending interest for " + prefixNS);
		
		try{	    	
			Interest interestSent = new Interest(ContentName.fromNative(prefixNS));
			gethandle.expressInterest(interestSent, this);
		}catch(Exception e){
			System.out.println("Exception:"+e.getMessage());
		}    
	}


	public Interest handleContent(ContentObject data,
			Interest interest) {
		
		gethandle.cancelInterest(interest, this);
		
		tweets.add(data.toString());
		
		System.out.println("Received Content number - " + counter);
		System.out.println("Received Tweet - " + data.toString());
		
		if(counter <= limit){			
			this.nextInterest();
		}
		else{
			System.out.println("Total " + limit + " tweets received from the server");
		}
		
		enoughTweetsRecvd = true;
		
		return null;
	}


}
