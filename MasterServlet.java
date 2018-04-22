package edu.upenn.cis455.mapreduce.master;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import javax.servlet.*;
import javax.servlet.http.*;

import edu.upenn.cis.stormlite.Config;
import edu.upenn.cis.stormlite.Topology;
import edu.upenn.cis.stormlite.TopologyBuilder;
import edu.upenn.cis.stormlite.bolt.MapBolt;
import edu.upenn.cis.stormlite.bolt.ReduceBolt;
import edu.upenn.cis.stormlite.distributed.WorkerJob;
import edu.upenn.cis.stormlite.spout.FileSpout;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis455.mapreduce.MyFileSpout;
import edu.upenn.cis455.mapreduce.MyPrintBolt;

import com.fasterxml.jackson.databind.ObjectMapper;

class WorkerStatus {
	public String ip;
	public String port;
	public String status;
	public String job;
	public String keysRead;
	public String keysWritten;
	public String results;
	public long lastPost;
};
public class MasterServlet extends HttpServlet {

  static final long serialVersionUID = 455555001;
  private HashMap<String, WorkerStatus> workerStatus = new HashMap<String, WorkerStatus>();
  
	private static final String FILE_SPOUT = "FILE_SPOUT";
	private static final String MAP_BOLT = "MAP_BOLT";
	private static final String REDUCE_BOLT = "REDUCE_BOLT";
	private static final String PRINT_BOLT = "PRINT_BOLT";
	
	HttpURLConnection sendJob(String dest, String reqType, Config config, String job, String parameters) throws IOException 
	{
		URL url = new URL("http://" + dest + "/" + job);

		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setRequestMethod(reqType);

		if (reqType.equalsIgnoreCase("POST")) {
			conn.setRequestProperty("Content-Type", "application/json");

			OutputStream os = conn.getOutputStream();
			byte[] toSend = parameters.getBytes();
			os.write(toSend);
			os.flush();
		} else
			conn.getOutputStream();

		return conn;
	}
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws java.io.IOException
  {
    if(request.getRequestURI().equals("/status"))
    {
    	response.setContentType("text/html");
    	PrintWriter out = response.getWriter();
    	String content = new String();
    	content += "<html><head><title>Status Page</title></head>";
    	content += "<body><table>";
    	content +="<tr><th>Agrima Seth: agrima</th></tr>";
    	content += "<tr><th>Workers index</th><th>IP:port</th><th>status</th><th>job</th><th>keys read</th><th>keys written</th><th>results</th></tr>";
    	int counter=0;
    	
		Iterator<String> iterator = workerStatus.keySet().iterator();
		while (iterator.hasNext()) 
		{
			WorkerStatus ws = workerStatus.get(iterator.next());
			//if the last post was > 30 sec then don't display it move to the next entry and remove this entry.
			if (new Date().getTime() - ws.lastPost > 30000) 
			{
				System.out.println("Worker Expired");
				iterator.remove();
				continue;
			}
			//create table to be displayed.
			content += "<tr>";
			content += "<th>" + (++counter) + "</th>";
			content += "<th>" + ws.ip + ":" + ws.port + "</th>";
			content += "<th>" + ws.status + "</th>";
			content += "<th>" + ws.job + "</th>";
			content += "<th>" + ws.keysRead + "</th>";
			content += "<th>" + ws.keysWritten + "</th>";
			content += "<th>" + ws.results + "</th>";
			content += "</tr>";
		}
		content += "</table>";
		
		//create the form to get status
		content += "<form action=\"/status\" method=\"get\" />";
		content += "Class Name of the Job: <input type=\"text\" name=\"jobname\"/><br/>";
		content += "Input Directory: <input type=\"text\" name=\"inputdir\"/><br/>";
		content += "Output Directory: <input type=\"text\" name=\"outputdir\"/><br/>";
		content += "Number of Map Executor: <input type=\"text\" name=\"mapclass\"/><br/>";
		content += "Numer of Reduce Executor: <input type=\"text\" name=\"reduceclass\"/><br/>";
		content += "<input type=\"submit\" value=\"Submit New Job\" />";
		content += "</form>";
		content += "</body>";
		content += "</html>";
		out.write(content);
	
		//if form has been submitted
		if (!request.getParameterMap().isEmpty()) 
		{
			String jobName = request.getParameter("jobname");
			String inputDir = request.getParameter("inputdir");
			String outputDir = request.getParameter("outputdir");
			String mapNum = request.getParameter("mapclass");
			String reduceNum = request.getParameter("reduceclass");
			
			Config config = new Config();

			config.put("jobname", jobName);
			config.put("inputdir", inputDir);
			config.put("outputdir", outputDir);
			config.put("mapExecutors", mapNum);
			config.put("reduceExecutors", reduceNum);
			config.put("spoutExecutors", Integer.toString(1));
			
			FileSpout spout = new MyFileSpout();
			MapBolt mapBolt = new MapBolt();
			ReduceBolt reduceBolt = new ReduceBolt();
			MyPrintBolt printBolt = new MyPrintBolt();//created own PrintBolt from print bolt in test

			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout(FILE_SPOUT, spout, 1);
			builder.setBolt(MAP_BOLT, mapBolt, Integer.valueOf(config.get("mapExecutors"))).fieldsGrouping(FILE_SPOUT, new Fields("value"));
			builder.setBolt(REDUCE_BOLT, reduceBolt, Integer.valueOf(config.get("reduceExecutors"))).fieldsGrouping(MAP_BOLT, new Fields("key"));
			builder.setBolt(PRINT_BOLT, printBolt, 1).firstGrouping(REDUCE_BOLT);
			
			Topology topo = builder.createTopology();
			WorkerJob job = new WorkerJob(topo, config);

			ObjectMapper mapper = new ObjectMapper();
			mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
			
			String workerList = new String();
		
			workerList += "[";
			for (String worker : workerStatus.keySet()) 
			{
				WorkerStatus ws = workerStatus.get(worker);
				String dest = ws.ip + ":" + ws.port;
				//if (cc == 0)
				workerList += dest + ",";
				//else
				//workerList += "," + dest;
				//cc++;
			}
			//remove the last comma
			workerList=workerList.substring(0, workerList.length() - 1);
			//close the list
			workerList += "]";
			System.out.println(workerList);
			config.put("workerList", workerList);
			
			
			int count = 0;
			for (String worker : workerStatus.keySet()) 
			{
				config.put("workerIndex", String.valueOf(count++));
				WorkerStatus ws = workerStatus.get(worker);
				String dest = ws.ip + ":" + ws.port;

				if (sendJob(dest, "POST", config, "definejob",
						mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job))
								.getResponseCode() != HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job definition request failed");
				}
			}
			
			for (String worker : workerStatus.keySet()) {
				WorkerStatus ws = workerStatus.get(worker);
				String dest = ws.ip + ":" + ws.port;
				
				if (sendJob(dest, "POST", config, "runjob", "").getResponseCode() != HttpURLConnection.HTTP_OK) {
					throw new RuntimeException("Job execution request failed");
				}
			}
			
		}
    }
    else if (request.getRequestURI().equals("/workerstatus"))
    {
    	String ip = request.getRemoteAddr();
		String addr = ip + ":" + request.getParameter("port");
		WorkerStatus ws = workerStatus.get(addr);
		
		if (ws == null)
			ws = new WorkerStatus();
		ws.ip = ip;
		ws.port = request.getParameter("port");
		ws.status = request.getParameter("status");
		ws.job = request.getParameter("job");
		ws.keysRead = request.getParameter("keysRead");
		ws.keysWritten = request.getParameter("keysWritten");
		ws.results = request.getParameter("results");
		ws.lastPost = new Date().getTime();
		workerStatus.put(addr, ws);
    }
    else if (request.getRequestURI().equals("/shutdown")) 
    {
    	PrintWriter out = response.getWriter();
		String content = new String();
		int counter = 0;	
		for (String worker : workerStatus.keySet())
		{
			WorkerStatus ws = workerStatus.get(worker);
			String strURL = "http://" + ws.ip + ":" + ws.port + "/shutdown";
			String  addr = ws.ip+ws.port;
			URL respURL = new URL(strURL);
			System.out.println(strURL);
			HttpURLConnection conn = (HttpURLConnection) respURL.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("GET");
			if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) 
			{
				System.out.println("Shutdown Response Not OK : " + conn.getResponseCode());
			}
			else
			{
				workerStatus.remove(addr);
			}
			counter++;
		}
		content += "Successfully Shutdown " + counter + " Workers";
		out.write(content);
    }
//    else if (request.getRequestURI().equals("/informshutdown")) 
//    {
//
//		String ip = request.getRemoteAddr();
//		String port = request.getParameter("port");
//		String addr = ip + ":" + port;
//		if (workerStatus.remove(addr) == null)
//			System.out.println("No Such Worker");
//		else
//			System.out.println("Shut Down Worker : " + addr);
//	}
    
    
  }
}
  
