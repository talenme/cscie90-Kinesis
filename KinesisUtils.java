/*
 * Greg Misicko
 * cscie90 Cloud Computing 
 * Kinesis End to End Demo with Stream Management
 * Dec 15 2014
 */


import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.MergeShardsRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.SplitShardRequest;


public class KinesisUtils {
	
	public KinesisUtils(){
	}
	
	// Create a stream with a specific name and specified number of starting shards
	public void createMyStream(AmazonKinesisClient kinesis, String myStreamName, int streamSize) {
		System.out.println("Creating a stream named "+myStreamName+" (this will take a few moments)");
		// build a configurable request object
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		// name of the stream you wish to create, this will need to be known when putting data into kinesis
		createStreamRequest.setStreamName( myStreamName );
		// stream size could also be called shard count
		createStreamRequest.setShardCount( streamSize );
		// send the request to AWS
		try {
			kinesis.createStream(createStreamRequest);
		}
		// if a stream already exists with this name, you cannot create it
		catch (ResourceInUseException re) {
			System.out.println("**Stream not created: "+myStreamName+" is already in use\n");
		}
		
		// build a configurable request object
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		// identify the name of the stream to get the details for
		describeStreamRequest.setStreamName( myStreamName );
		
		// identify how long you are willing to wait for the stream to become active
		long startTime = System.currentTimeMillis();
		long endTime = startTime + ( 10 * 60 * 1000 );
		
		// this string will hold the status value of the stream being watched
		String status = "";
		// keep checking the status of the stream, up until the endTime limit
		while ( System.currentTimeMillis() < endTime ) {
			// wait a bit to make sure the request has been processed
			try {
				Thread.sleep(10 * 1000);
			} 
			catch ( Exception e ) {}
		  
		  try {
			  // ask AWS for the stream details
			  DescribeStreamResult describeStreamResponse = kinesis.describeStream( describeStreamRequest );
			  // identify the status of this stream
			  String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
			  // store status in case we want to show it in the exception message
			  status = streamStatus;
			  // if the stream is in ACTIVE status, we are done waiting and can return
			  if ( streamStatus.equals( "ACTIVE" ) ) {
				  System.out.println(myStreamName+" is ACTIVE\n");
				  break;
			  }
			  
			  // sleep for a tiny bit
			  try {
				  Thread.sleep( 1000 );
			  }
			  catch ( Exception e ) {}
		  	}
		  catch ( ResourceNotFoundException e ) {}
		}
		// if we've reached the end of the time limit...
		if ( System.currentTimeMillis() >= endTime ) {
			throw new RuntimeException( "TIMING OUT: Stream " + myStreamName + " status is "+status );
		}
	}
	
	// List all of the streams in your account
	public List<String> listMyStreams(AmazonKinesisClient kinesis) {
		// build a configurable request object
		ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
		// identify how many streams to return per request
		listStreamsRequest.setLimit(20);
		// fetch the list of streams from your account
		ListStreamsResult listStreamsResult = kinesis.listStreams(listStreamsRequest);
		
		// copy the names into a list
		List<String> streamNames = listStreamsResult.getStreamNames();
		
		// keep fetching those stream names until no more are left
		while (listStreamsResult.getHasMoreStreams()) 
		  {
			// if we have at least one stream name fetched already...
		    if (streamNames.size() > 0) {
		    	// set the request to get the stream name following the one we previously fetched
		    	listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
		    }
		    // send the request 
		    listStreamsResult = kinesis.listStreams(listStreamsRequest);
		    // add next name to the list
		    streamNames.addAll(listStreamsResult.getStreamNames());
		  }
		
		return streamNames;
	}
	
	// put data onto the specified stream, Kinesis will determine which shard to use
    public void putMyData (AmazonKinesisClient kinesis, String fileContent, String session, 
    		String myStreamName) throws Exception {	
    	// build a configurable request object
    	PutRecordRequest putRecordRequest = new PutRecordRequest();
    	// identify the stream name in the request object
    	putRecordRequest.setStreamName(myStreamName);
    	// ordering is not necessary for this data, so we don't need to set a sequence number
    	putRecordRequest.setSequenceNumberForOrdering(null);
    	
    	System.out.println("Sending this data to stream "+myStreamName+":");
    	System.out.println(fileContent);
    	
    	// add data to the request object. The data streams only take raw bytes, so your
    	// workers will need to know how to decode what you've put up
    	putRecordRequest.setData(ByteBuffer.wrap(fileContent.getBytes()));
    	// set a random number range. We'll use these numbers for our partition keys
    	// so that not all data goes to a single shard
    	int max=5;
    	int min=1;
    	Random rand = new Random();
    	int randomNum = rand.nextInt((max - min) + 1) + min;
    	// set a partition key
    	putRecordRequest.setPartitionKey(session+randomNum);
    	// send the request to AWS
    	PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
    	
    	System.out.println("Successfully put record with partition key= "+putRecordRequest.getPartitionKey()+"\n"
    			+ "ShardID= "+putRecordResult.getShardId()); 
    	System.out.println("Sequence Number= "+putRecordResult.getSequenceNumber()+"\n");	
    }
    
    public void deleteMyStream(AmazonKinesisClient kinesis, String myStreamName) {
    	// build a configurable request object
    	DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
    	// identify the name of the stream to be deleted
    	deleteStreamRequest.setStreamName(myStreamName);
    	
    	// try to delete the identified stream
    	try {
    		System.out.println("Deleting the stream named "+myStreamName+"\n");
    		kinesis.deleteStream(deleteStreamRequest);
    	}
    	// if it doesn't exist, announce that the request failed
    	catch (ResourceNotFoundException rnfe) {
    		System.out.println("**Stream not deleted: "+myStreamName+" not found in your account\n");
    	}
    	
    }
    
    public List<Record> getMyStreamData (AmazonKinesisClient kinesis, Shard shard, String myStreamName) {
    	String shardIterator;
    	// build a configurable request object
    	GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
    	// make an iterator for the identified stream
    	getShardIteratorRequest.setStreamName(myStreamName);
    	// now identify which shard to iterate over
    	getShardIteratorRequest.setShardId(shard.getShardId());
    	// there are four iterator types, and 'horizon' indicates that we start with the first record
    	getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

    	GetShardIteratorResult getShardIteratorResult = kinesis.getShardIterator(getShardIteratorRequest);
    	shardIterator = getShardIteratorResult.getShardIterator();
    	
    	// Record is a Kinesis data type
    	List<Record> records = null;
    	    
    	// normally you'd run this part indefinitely and let the worker to continue
    	// processing incoming data. In the case of this simple demo, we'll just 
    	// grab one bunch of records
//    	while (true) {
    	   
    	  // Create a new getRecordsRequest with an existing shardIterator 
    	  GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
    	  getRecordsRequest.setShardIterator(shardIterator);
    	  // set the maximum records to return to 25
    	  getRecordsRequest.setLimit(25); 

    	  GetRecordsResult result = kinesis.getRecords(getRecordsRequest);
    	  
    	  // Put the result into record list. The result can be empty.
    	  records = result.getRecords();
    	  
    	  try {
    	    Thread.sleep(1000);
    	  } 
    	  catch (InterruptedException exception) {
    	    throw new RuntimeException(exception);
    	  }
    	  
    	  shardIterator = result.getNextShardIterator();
//    	}  
    	  return records;
    }
    
    // the details of a stream will contain information about the shards it contains
    public List<Shard> getMyShards (AmazonKinesisClient kinesis, String myStreamName) {
    	// build a configurable request object
    	DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    	// identify the name of the stream to get shard details from
    	describeStreamRequest.setStreamName( myStreamName );
    	// instantiate a list of type shard, to store the shard information from the stream
    	List<Shard> shards = new ArrayList<>();
    	// essentially set the starting point for shard retrieval to the beginning
    	String exclusiveStartShardId = null;
    	do {
    		// set the start point in the request object
    	    describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
    	    // get results - this may be a batch of results, and not necessarily the complete list
    	    DescribeStreamResult describeStreamResult = kinesis.describeStream( describeStreamRequest );
    	    // add what you've retrieved to the list of shards
    	    shards.addAll( describeStreamResult.getStreamDescription().getShards() );
    	    System.out.println(describeStreamResult.toString());
    	    // check to see if there are more
    	    if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
    	    	// essentially increment your position in the list of shards to be retrieved
    	        exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
    	    } else {
    	    	// if you have reached the end of the list of shards, setting the indexer to null
    	    	// will break the loop
    	        exclusiveStartShardId = null;
    	    }
    	} while ( exclusiveStartShardId != null );
    	
    	System.out.println("\n");
    	return shards;
    }
    
    // this method won't analyze the shards, it will simply take two identified shards and attempt to merge them
    public void mergeMyShards (AmazonKinesisClient kinesis, String myStreamName, Shard shard1, Shard shard2) {
    	// build a configurable request object
    	MergeShardsRequest mergeShardsRequest = new MergeShardsRequest();
    	// identify the stream these shards are in
    	mergeShardsRequest.setStreamName(myStreamName);
    	// identify shard1 and shard2 - remember they must be adjacent
    	mergeShardsRequest.setShardToMerge(shard1.getShardId());
    	mergeShardsRequest.setAdjacentShardToMerge(shard2.getShardId());
    	// execute the request to merge the two shards
    	try {
    		kinesis.mergeShards(mergeShardsRequest);
    	}
    	// the resources can not be merged - this would occur if at least one of the
    	// shards was no longer ACTIVE
    	catch (ResourceInUseException re){
    		System.out.println("Merge failed: shards in use"); 
    		
    	}
    	catch (ResourceNotFoundException rnfe) {
    		System.out.println("Merge failed: resource not found");
    	}
    	
    	// similar to the create stream method, we'll now monitor the merging of these shards
    	// until the status turns to ACTIVE
    	DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    	describeStreamRequest.setStreamName( myStreamName );

    	long startTime = System.currentTimeMillis();
    	long endTime = startTime + ( 10 * 60 * 1000 );
    	while ( System.currentTimeMillis() < endTime ) 
    	{
    		// be patient - let it wait a bit
    		try {
    	    Thread.sleep(10 * 1000);
    	  } 
    	  catch ( Exception e ) {}
    	  
    	  try {
    	    DescribeStreamResult describeStreamResponse = kinesis.describeStream( describeStreamRequest );
    	    String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
    	    if ( streamStatus.equals( "ACTIVE" ) ) {
    	    	System.out.println("Merged shard is now ACTIVE\n");
    	    	break;
    	    }
    	    else {
    	    	System.out.println("Merged shard status is: "+streamStatus);
    	    }
    	   
    	    // sleep for one second
    	    try {
    	    	Thread.sleep( 1000 );
    	    }
    	    catch ( Exception e ) {}
    	  }
    	  catch ( ResourceNotFoundException e ) {}
    	}
    	if ( System.currentTimeMillis() >= endTime ) 
    	{
    	  throw new RuntimeException( "Stream " + myStreamName + " never went active" );
    	}
    	
    }
    
    // split a single shard into two
    public void splitMyShard (AmazonKinesisClient kinesis, Shard shard, String myStreamName) {
    	// build a configurable request object
    	SplitShardRequest splitShardRequest = new SplitShardRequest();
    	splitShardRequest.setStreamName(myStreamName);
    	splitShardRequest.setShardToSplit(shard.getShardId());
    	
    	// partition keys are set as Hash Keys per shard. To divide this particular shard, we'll
    	// divide up the partition keys from the middle
    	BigInteger startingHashKey = new BigInteger(shard.getHashKeyRange().getStartingHashKey());
    	BigInteger endingHashKey   = new BigInteger(shard.getHashKeyRange().getEndingHashKey());
    	String newStartingHashKey  = startingHashKey.add(endingHashKey).divide(new BigInteger("2")).toString();

    	splitShardRequest.setNewStartingHashKey(newStartingHashKey);
    	try {
    		kinesis.splitShard(splitShardRequest);
    		
    		try {
    	    	Thread.sleep( 30 * 1000 );
    	    }
    	    catch ( Exception e ) {}
    		
    		System.out.println("Successfully split one shard into two");
    	}
    	catch (InvalidArgumentException e) {
    		// this exception will be thrown if the shard is already in CLOSED state
    		System.out.println(e);
    	}
    	
    }

}
