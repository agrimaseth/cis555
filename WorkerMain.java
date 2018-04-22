package edu.upenn.cis455.mapreduce.worker;

import java.net.MalformedURLException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.HashMap;

import com.sleepycat.je.DatabaseException;

import org.apache.log4j.BasicConfigurator;
//import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

//import javax.servlet.ServletException;

public class WorkerMain {
    public static void main(String[] args) throws NumberFormatException, DatabaseException, NoSuchAlgorithmException, IOException {
//        Map<String, String> config = new HashMap<String, String>();
//        config.put("workerList", "[localhost:8080]");
//        config.put("workerIndex", "0");
//        WorkerServer.createWorker(config);
    	
    	//start the worker with master addr, directory of storage and it's own port
//    	Map<String, String> worker_config = new HashMap<String, String>();
//    	worker_config.put("masterAddr", args[0]);
//    	worker_config.put("store_dir", args[1]);
//    	worker_config.put("port", args[2]);
    	
//    	PropertyConfigurator.configure("log4j.properties");
    	BasicConfigurator.configure();
    	WorkerServer.createWorker(args[0], args[1], Integer.parseInt(args[2]));
    	
    	
    	System.out.println("Press [Enter] to exit...");
		(new BufferedReader(new InputStreamReader(System.in))).readLine();

		WorkerServer.shutdown();
        System.exit(0);
    }
}
