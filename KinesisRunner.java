/*
 * Greg Misicko
 * cscie90 Cloud Computing 
 * Kinesis End to End Demo with Stream Management
 * Dec 15 2014
 */



import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

/**
 * Executes and end to end data flow using Amazon Kinesis
 */
public class KinesisRunner {
	
	private static final Log LOG = LogFactory.getLog(KinesisRunner.class);

    public static void main(String[] args) throws Exception {
    	// fetch my access credentials from the credentials file stored locally
	    AWSCredentials credentials = null;     
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/greg/.aws/credentials), and is in valid format.",
                    e);
        }
        
        // set up my access client using my account credentials
        AmazonKinesisClient kinesis = new AmazonKinesisClient(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        kinesis.setEndpoint("kinesis.us-east-1.amazonaws.com");
        kinesis.setRegion(usEast1);
        
        
        // instantiate my utility class which contains methods for the basic Kinesis operations
        KinesisUtils kus = new KinesisUtils();
        
        // set a stream name to create in AWS
        String streamName = "helloGreg2";
        // specify how many shards will be in the stream you are creating
        int streamSize = 2;
        
        // create the stream using the values previously set
        kus.createMyStream(kinesis, streamName, streamSize);
        
        // get a List of all of the streams in my account
        List<String> myStreamsList = kus.listMyStreams(kinesis);
        System.out.println("All streams in my account: "+myStreamsList+"\n");
        
        // put some String data up on the stream
        String hello = "hello";
        String session = "session";
        for (int x=0; x<10; x++) {
        	hello = "hello"+x;
        	kus.putMyData (kinesis, hello, session, streamName);
        }
       
        // get details about a particular stream, and all of the shards in it
        System.out.println("Stream details for "+streamName+":");
        List<Shard> myShards = kus.getMyShards(kinesis, streamName);
        
        // identify how many shards belong to this stream
        int shardsCount = myShards.size();
        System.out.println("There are "+shardsCount+" shards in this stream");
        
        // now to print out the data from each shard. In a real application we
        // would be doing more with the data than just printing it out, but understanding
        // how to access your data in the first place is the biggest challenge
        List<Record> streamData;
        // iterate through all shards
        for (int i=0; i< shardsCount; i++) {
        	// get the records from the stream
        	streamData = kus.getMyStreamData(kinesis, myShards.get(i) ,streamName);
        	// display the raw data info, but you won't be able to see the actual data objects
        	System.out.println("My data from shard "+i+" is: "+streamData+"\n");
        	
        	// data objects are all stored as ByteBuffers. In this demo they were simple
        	// strings, so lets convert them back to strings and display
        	for (int j=0; j<streamData.size(); j++){
        		Record r = streamData.get(j);
        		ByteBuffer b = r.getData();
        		String v = new String(b.array());
        		System.out.println(j+" Data value = "+v);
        	}
        }
        
        System.out.println("");
        
        // we don't need two shards, so let's merge the two into one
        System.out.println("Merging the two shards...");
        kus.mergeMyShards(kinesis, streamName, myShards.get(0), myShards.get(1));
        
        // now recheck the stream details to verify we're down to one shard
        // get details about a particular stream, and all of the shards in it
        System.out.println("Stream details for "+streamName+":");
        // throw away the old shard information
        myShards = null;
        // fetch the updated shard info
        myShards = kus.getMyShards(kinesis, streamName);
        
        // identify how many shards belong to this stream
        shardsCount = myShards.size();
        System.out.println("There are "+shardsCount+" shards in stream "+streamName);
        
        // get details about a particular stream, and all of the shards in it
        System.out.println("Stream details for "+streamName+":");
        myShards = kus.getMyShards(kinesis, streamName);   
        
        // put some String data up on the stream to populate the one shard
        for (int x=0; x<50; x++) {
        	hello = "stream2data"+x;
        	kus.putMyData (kinesis, hello, session, streamName);
        }
        
        System.out.println("Trying to split a shard...");
        kus.splitMyShard(kinesis, myShards.get(2), streamName);
        
        // update shard info again
        myShards = kus.getMyShards(kinesis, streamName);
        shardsCount = myShards.size();
        
        // since we split the shard with data into two and had it divide up the partition
        // keys, we'd expect to see the data divided between the two ACTIVE remaining shards
        for (int i=0; i< shardsCount; i++) {
        	// get the records from the stream
        	streamData = kus.getMyStreamData(kinesis, myShards.get(i) ,streamName);
        	// display the raw data info, but you won't be able to see the actual data objects
        	System.out.println("My data from shard "+i+" is: "+streamData+"\n");
        	
        	// data objects are all stored as ByteBuffers. In this demo they were simple
        	// strings, so lets convert them back to strings and display
        	for (int j=0; j<streamData.size(); j++){
        		Record r = streamData.get(j);
        		ByteBuffer b = r.getData();
        		String v = new String(b.array());
        		System.out.println(j+" Data value = "+v);
        	}
        }
        
        // put some String data up on the stream to populate the one shard
        for (int x=0; x<100; x++) {
        	hello = "stream3data"+x;
        	kus.putMyData (kinesis, hello, session, streamName);
        }
        
        // update shard info again
        myShards = kus.getMyShards(kinesis, streamName);
        shardsCount = myShards.size();
        
        // since we split the shard with data into two and had it divide up the partition
        // keys, we'd expect to see the data divided between the two ACTIVE remaining shards
        for (int i=0; i< shardsCount; i++) {
        	// get the records from the stream
        	streamData = kus.getMyStreamData(kinesis, myShards.get(i) ,streamName);
        	// display the raw data info, but you won't be able to see the actual data objects
        	System.out.println("My data from shard "+i+" is: "+streamData+"\n");
        	
        	// data objects are all stored as ByteBuffers. In this demo they were simple
        	// strings, so lets convert them back to strings and display
        	for (int j=0; j<streamData.size(); j++){
        		Record r = streamData.get(j);
        		ByteBuffer b = r.getData();
        		String v = new String(b.array());
        		System.out.println(j+" Data value = "+v);
        	}
        }
        
        // delete the stream we created for this sample (streams cost money...)
        kus.deleteMyStream(kinesis, streamName);
        
        System.out.println("done");
    }
    

}
