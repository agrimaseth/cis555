package edu.upenn.cis455.mapreduce.worker;

import static spark.Spark.setPort;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sleepycat.je.DatabaseException;


import edu.upenn.cis.stormlite.Config;

import edu.upenn.cis.stormlite.DBWrapper;
import edu.upenn.cis.stormlite.DistributedCluster;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.distributed.WorkerHelper;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.routers.StreamRouter;
import edu.upenn.cis.stormlite.tuple.Tuple;
import spark.Request;
import spark.Response;
import spark.Route;
import spark.Spark;

/**
 * Simple listener for worker creation 
 * 
 * @author zives
 *
 */
class ShutDownThread extends Thread{
	@Override
	public void run() {
		try {
			Thread.sleep(2000);
			WorkerServer.shutdown();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}



public class WorkerServer {
    static Logger log = Logger.getLogger(WorkerServer.class);
        
    static public DistributedCluster cluster = new DistributedCluster();
    
    static String masterAddr = null;
    static public String storeDir = null;
    static int myPort;
    TopologyContext currentContext = null;
	String currentJob = null;
	
	static public DBWrapper bdb = new DBWrapper();
//	List<TopologyContext> contexts = new ArrayList<>();
//    static List<String> topologies = new ArrayList<>();
        
	private int getKeysRead()
	{
		if (currentContext == null)
			return 0;
		return currentContext.keysRead.get();
	}

	private int getKeysWritten() 
	{
		if (currentContext == null)
			return 0;
		return currentContext.keysWritten.get();
	}

	private String getResults() 
	{
		if (currentContext == null)
			return "No Results";
		synchronized (currentContext.results) 
		{
			return currentContext.results.toString();
		}
	}
	
	private String getWorkerState() 
	{
		if (currentContext == null)
			return "IDLE";
		else if (currentContext.getState() == TopologyContext.STATE.IDLE)
			return "IDLE";
		else if (currentContext.getState() == TopologyContext.STATE.MAP)
			return "MAP";
		else if (currentContext.getState() == TopologyContext.STATE.WAITING)
			return "WAITING";
		else if (currentContext.getState() == TopologyContext.STATE.REDUCE)
			return "REDUCE";
		return null;
	}
	
 //   public WorkerServer(int myPort) throws MalformedURLException 
    @SuppressWarnings("deprecation")
	public WorkerServer(String MasterAddr, String StoreDir, int MyPort) throws MalformedURLException, DatabaseException, NoSuchAlgorithmException
    {
                
        log.info("Creating server listener at socket " + myPort);
        
        //put worker params
        storeDir = StoreDir;
		this.myPort = MyPort;
		this.masterAddr = MasterAddr;
		
		
		//clean database for every run
		File dbHome = new File(WorkerServer.storeDir + "/");
		File[] files = dbHome.listFiles();
		if (files!=null)
		{
			for (File file : files) 
			{
//				if (file.delete())
//					System.out.println("delete:" + file.getAbsolutePath());
			    if (!file.isDirectory()) 
			    {
			        file.delete();
			        System.out.println("new delete:" + file.getAbsolutePath());
			    }
			}
		}

		dbHome.mkdirs();
        //initialize database
		bdb.initialise(dbHome, false);
		
		TimerTask timerTask = new TimerTask() 
		{

			@Override
			public void run() 
			{
				try {
					String[] s = masterAddr.split(":", 2);
					String masterIP = s[0];
					int masterPort = Integer.parseInt(s[1]);
					String strURL = "http://" + masterIP + ":" + masterPort + "/workerstatus";
					strURL += "?port=" + myPort;
					strURL += "&status=" + getWorkerState();
					strURL += "&job=" + URLEncoder.encode((currentJob == null ? "No Job" : currentJob), "UTF-8");
					strURL += "&keysRead=" + getKeysRead();
					strURL += "&keysWritten=" + getKeysWritten();
					strURL += "&results=" + URLEncoder.encode(getResults(), "UTF-8");
					URL respURL = new URL(strURL);
					HttpURLConnection conn = (HttpURLConnection) respURL.openConnection();
					conn.setDoOutput(true);
					conn.setRequestMethod("GET");
					
					System.out.println(respURL.toString());
					if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
						System.out.println("Response Not OK : " + conn.getResponseCode());
					}

				} catch (MalformedURLException e) {e.printStackTrace();} catch (IOException e) {e.printStackTrace();}
			}
		};
		
		//send status after 10 ms
		Timer timer = new Timer("MyTimer");
		timer.scheduleAtFixedRate(timerTask, 30, 10000);
		
		//create listner socket
		log.info("Creating server listener at socket " + myPort);
        setPort(myPort);
        final ObjectMapper om = new ObjectMapper();
        om.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        Spark.post("/definejob", new Route() 
        {

                @Override
                public Object handle(Request arg0, Response arg1) {
                        
                    WorkerJob workerJob;
                    try {
                        workerJob = om.readValue(arg0.body(), WorkerJob.class);
                                
                        try 
                        {
//                            log.info("Processing job definition request" + workerJob.getConfig().get("job") +" on machine " + workerJob.getConfig().get("workerIndex"));
//                            contexts.add(cluster.submitTopology(workerJob.getConfig().get("job"), workerJob.getConfig(), workerJob.getTopology()));
//                                                
//                            synchronized (topologies) { topologies.add(workerJob.getConfig().get("job"));}
                        	currentJob = workerJob.getConfig().get("jobname");
                        	currentContext = cluster.submitTopology(currentJob, workerJob.getConfig(),workerJob.getTopology(), storeDir);
                        	
                        } catch (ClassNotFoundException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return "Job launched";
                    } catch (IOException e) {
                        e.printStackTrace();
                                        
                        // Internal server error
                        arg1.status(500);
                        return e.getMessage();
                    } 
                        
                }
                
            });
        
        Spark.post("/runjob", new Route() 
        {

                @Override
                public Object handle(Request arg0, Response arg1) 
                {
                    log.info("Starting job!");
                    cluster.startTopology();
                                
                    return "Started";
                }
            });

        Spark.post("/push/:stream", new Route() 
        {

                @Override
                public Object handle(Request arg0, Response arg1) {
                    try {
                        String stream = arg0.params(":stream");
                        Tuple tuple = om.readValue(arg0.body(), Tuple.class);
                                        
                        log.debug("Worker received: " + tuple + " for " + stream);
                                        
                        // Find the destination stream and route to it
                        StreamRouter router = cluster.getStreamRouter(stream);
                                        
//                        if (contexts.isEmpty())
//                            log.error("No topology context -- were we initialized??");
                                        
                        if (!tuple.isEndOfStream())
                            //contexts.get(contexts.size() - 1).incSendOutputs(router.getKey(tuple.getValues()));
                        	currentContext.incSendOutputs(router.getKey(tuple.getValues()));
                                        
                        if (tuple.isEndOfStream())
                            //router.executeEndOfStreamLocally(contexts.get(contexts.size() - 1));
                        	router.executeEndOfStreamLocally(currentContext);
                        else
                           // router.executeLocally(tuple, contexts.get(contexts.size() - 1));
                        	router.executeLocally(tuple, currentContext);
                                        
                        return "OK";
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                                        
                        arg1.status(500);
                        return e.getMessage();
                    }
                                
                }
                
           });
        
		Spark.get("/shutdown",new Route() 
		{

			@Override
			public Object handle(Request arg0, Response arg1) 
			{
				new ShutDownThread().start();
				return "";
			}
		});
        
          

    }
        
//    public static void createWorker(Map<String, String> config) {
//        if (!config.containsKey("workerList"))
//            throw new RuntimeException("Worker spout doesn't have list of worker IP addresses/ports");
//
//        if (!config.containsKey("workerIndex"))
//            throw new RuntimeException("Worker spout doesn't know its worker ID");
//        else {
//            String[] addresses = WorkerHelper.getWorkers(config);
//            String myAddress = addresses[Integer.valueOf(config.get("workerIndex"))];
//
//            log.debug("Initializing worker " + myAddress);
//
//            URL url;
//            try {
//                url = new URL(myAddress);
//
//                new WorkerServer(url.getPort());
//            } catch (MalformedURLException e) {
//                // TODO Auto-generated catch block
//                e.printStackTrace();
//            }
//        }
//    }
    
    public static void createWorker(String MasterAddr, String StoreDir, int port) throws MalformedURLException, DatabaseException, NoSuchAlgorithmException 
    {
		new WorkerServer(MasterAddr, StoreDir, port);
	}

    public static void shutdown() 
    {
//        synchronized(topologies) {
//            for (String topo: topologies)
//                cluster.killTopology(topo);
//        }
//                
//        cluster.shutdown();
    	
//    	String strURL = "http://" + masterAddr + "/informshutdown";
//		strURL += "?port=" + myPort;
//		URL respURL = null;
//		try {
//			respURL = new URL(strURL);
//			HttpURLConnection conn = (HttpURLConnection) respURL.openConnection();
//			conn.setDoOutput(true);
//			conn.setRequestMethod("GET");
//			if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
//				System.out.println("Inform Shutdown Response Not OK : " + conn.getResponseCode());
//			}
//		} catch (MalformedURLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	System.out.println("shutdown executed");
		System.exit(0);
    }
}
