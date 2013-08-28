package com.YCSBSopeco;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sopeco.engine.measurementenvironment.AbstractMEController;
import org.sopeco.engine.measurementenvironment.InputParameter;
import org.sopeco.engine.measurementenvironment.ObservationParameter;
import org.sopeco.engine.measurementenvironment.app.MECApplication;
import org.sopeco.persistence.dataset.ParameterValueList;
import org.sopeco.persistence.entities.definition.ExperimentTerminationCondition;
import org.sopeco.persistence.entities.exceptions.ExperimentFailedException;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * YCSBMEC is a Measurement Environment
 * Controller (MEController). Each MEController has to extend the
 * AbstractMEController class which is contained in the
 * org.sopeco.core-VERSION-jar-with-dependencies.jar.
 * 
 * This MEController investigates the responses of key-value stores on
 * manufactured workloads using the Yahoo Cloud Serving Benchmark
 * 
 * @author Tom Zhang
 * 
 */
public class YCSBMEC extends AbstractMEController {
	/**	
	 * Logger used for debugging and log-information.
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(YCSBMEC.class);
	
	private String DBsetup="default";
	
	private String DBshutdown="default";

	/**
	 * String constant for the measurement environment controller name
	 */
	private static final String MEC_NAME = "YCSB";

	/**
	 * String constant for the measurement environment controller identifier
	 */
	private static final String MEC_ID = "YCSBTest";

	/**
	 * Port at which the SoPeCo SaaS instance is listening
	 */
	private static final int SOPECO_SAAS_PORT = 18089;

	/**
	 * Address of the SoPeCo SaaS instance to register
	 */
	private static final String SOPECO_SAAS_URL = "localhost";

	/**
	 * Indicates the number of operations to perform before termination
	 * (a value less than or equal to zero indicates no limit) 
	 */
	public static final String OPERATION_COUNT_PROPERTY="operationcount";

	/**
	 * Indicates the number of records to load
	 */
	public static final String RECORD_COUNT_PROPERTY="recordcount";

	/**
	 * Indicates the package which the workload file exists
	 */
	public static final String WORKLOAD_PROPERTY="workload";

	/**
	 * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
	 * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
	 * should support the "insertstart" property, which tells them which record to start at.
	 */
	public static final String INSERT_COUNT_PROPERTY="insertcount";

	/**
	 * The maximum amount of time (in seconds) for which the benchmark will be run.
	 */
	public static final String MAX_EXECUTION_TIME = "maxexecutiontime";


	/**
	 * Path to scripts. Generally installed directory.
	 */
	@InputParameter(namespace = "general")
	String scriptPath= "/home/tom/git/SoPeCoYCSB/res";
	
	/**
	 * Path to database
	 */
	@InputParameter(namespace = "general")
	String DBPath= "/home/master/tom/work/localhost/cassandra";
	
	/**
	 * Path of saved data of databases
	 */
	@InputParameter(namespace = "general")
	String DBdataDirectory = "~/work/localhost/data";
	
	/**
	 * Path of saved data of databases
	 */
	@InputParameter(namespace = "general")
	String YCSBpath = "/home/master/tom/work/n1/ycsb-0.1.4";

	/**
	 * Path of saved data of databases
	 */
	@InputParameter(namespace = "general")
	String JAVApath = "/home/master/tom/work/n1/java";
	
	/**
	 * the number of nodes the DB will operate on for that experiment run. Counts from the first node in the host list. 
	 */
	@InputParameter(namespace = "db")
	int numDBNodes= 1;

	/**
	 * the list of ip addresses where the db is run
	 */
	@InputParameter(namespace = "db")
	String hosts= "localhost";
	
	/**
	 * the placement Strategy of Cassandra
	 */
	@InputParameter(namespace = "db.cassandra")
	String placementStrategy = "NetworkTopologyStrategy";

	/**
	 * the strategy options of cassandra
	 */
	@InputParameter(namespace = "db.cassandra")
	String strategy_options = "replication_factor:3";
	
	/**
	 * the list of ip addresses where the workload is run
	 */
	@InputParameter(namespace = "ycsb.workload")
	String clients= "localhost";

	/**
	 * the workload implementation to use
	 */
	@InputParameter(namespace = "ycsb.workload")
	String workloadname = "com.yahoo.ycsb.workloads.CoreWorkload";

	/**
	 * the DB implementation to use
	 */
	@InputParameter(namespace = "ycsb.workload")
	String dbname = "cassandra-8";

	/**
	 * true to do transactions, false to insert data
	 */
	@InputParameter(namespace = "ycsb.workload")
	boolean dotransactions = false;

	/**
	 * the total number of threads 
	 */
	@InputParameter(namespace = "ycsb.workload")
	int tcount = 1;

	/**
	 * the target number of operations per second. By default, the YCSB 
	 * Client will try to do as many operations as it can. For example, 
	 * if each operation takes 100 milliseconds on average, the Client will
	 * do about 10 operations per second per worker thread. However, you 
	 * can throttle the target number of operations per second. For example,
	 * to generate a latency versus throughput curve, you can try different
	 * target throughputs, and measure the resulting latency for each. Zero 
	 * or negative input for default
	 */
	@InputParameter(namespace = "ycsb.workload")
	int ttarget = 0;

	/**
	 * the number of records inserted
	 */
	@InputParameter(namespace = "ycsb.workload")
	int recordcount= 1000;

	/**
	 * the maximum number of operations
	 * (zero or negative for no limit)
	 */
	@InputParameter(namespace = "ycsb.workload")
	int operationcount = 0;

	/**
	 * number of inserts 
	 */
	@InputParameter(namespace = "ycsb.workload")
	int insertcount = 0;

	/**
	 * number of fields in the database
	 */
	@InputParameter(namespace = "ycsb.workload")
	int fieldcount= 5;

	/**
	 * length of fields in the database
	 */
	@InputParameter(namespace = "ycsb.workload")
	int fieldlength= 10;

	/**
	 * the maximum possible scan length of a workload 
	 */
	@InputParameter(namespace = "ycsb.workload")
	int maxscanlength = 50;

	/**
	 * the number of seconds client threads
	 * are active until threads are terminated
	 */
	@InputParameter(namespace = "ycsb.workload")
	int maxexecutiontime = 30;

	/**
	 * fraction of reads in a generated workload
	 */
	@InputParameter(namespace = "ycsb.workload")
	double readproportion= 0.95;

	/**
	 * fraction of inserts in a generated workload
	 */
	@InputParameter(namespace = "ycsb.workload")
	double insertproportion= 0.05;

	/**
	 * fraction of scans in a generated workload
	 */
	@InputParameter(namespace = "ycsb.workload")
	double scanproportion= 0.0;

	/**
	 * fraction of inputs in a generated workload
	 */
	@InputParameter(namespace = "ycsb.workload")
	double inputproportion= 0.0;

	/**
	 * fraction of updates in a generated workload
	 */
	@InputParameter(namespace = "ycsb.workload")
	double updateproportion= 0.0;
	
	/**
	 * type of distribution when running workload
	 */
	@InputParameter(namespace = "ycsb.workload")
	String requestDistribution= "uniform";
	
	/**
	 * name of tested table
	 */
	@InputParameter(namespace = "ycsb.workload")
	String table="usertable";

	/**
	 * runtime of the experiment in milliseconds
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> runtime;

	/**
	 * average operations per a second for a workload
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Double> throughput;

	/**
	 * Total number of operations
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> operations;

	/**
	 * average latency(ms) of the experiment
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Double> averageLatency;

	/**
	 * minimum latency(ms) of the experiment
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> minLatency;

	/**
	 * maximum latency(ms) of the experiment
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> maxLatency;

	/**
	 * Latency(ms) to cover 95% of the operations
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> ninetyFifthPercentile;

	/**
	 * Latency(ms) to cover 99% of the operations
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> ninetyninePercentile;

	/**
	 * Constructor. Defines which termination conditions for an
	 * experiment are supported by the MEController. 
	 */
	public YCSBMEC() {
		addSupportedTerminationConditions(ExperimentTerminationCondition
				.createNumberOfRepetitionsTC());
	}

	/**
	 * This functions is called to define which observation parameters should be
	 * included into the result set.
	 */
	@Override
	protected void defineResultSet() {
		addParameterObservationsToResult(runtime);
		addParameterObservationsToResult(throughput);
		addParameterObservationsToResult(operations);
		addParameterObservationsToResult(averageLatency);
		addParameterObservationsToResult(minLatency);
		addParameterObservationsToResult(maxLatency);
	}

	/**
	 * This method is called when a experiment series is finished. In
	 * particular, this method can be used to do clean-up jobs after the
	 * execution of an experiment series. In this example we do not have to
	 * clean up anything, and thus, will not implement this method.
	 */
	@Override
	protected void finalizeExperimentSeries() {
		LOGGER.info("Finalizing experiment series");
	}

	/**
	 * This method is called to initialize the MEController. In this example, we
	 * initialization a random number generator to fill rawMatrix.
	 */
	@Override
	protected void initialize() {
		LOGGER.info("Initializing experiment series");
	}

	/**
	 * This method is called to prepare an experiment series. In this example,
	 * we do not have any preparation tasks for the experiment series.
	 */
	@Override
	protected void prepareExperimentSeries() {
		LOGGER.info("Preparing experiment series");
		dbname=dbname.toLowerCase();
		if (dbname.contains("cassandra"))
		{
			DBsetup="./setupCassandra.sh";
			DBshutdown="./shutdownCassandra.sh";
		}
		
		hosts=hosts.replace("\\s","");
	}

	protected void filteredOutput(String line)
	{
		Pattern pattern = Pattern.compile(".*(READ]|INSERT]).*,.*\\d+\\,.*\\d+");
		Matcher m = pattern.matcher(line); 
		if (!m.find())
		{
			LOGGER.info ("Stdout: " + line);
		}
	}

	protected int findInteger(String line)
	{
		Pattern p = Pattern.compile("[0-9]+");
		Matcher m = p.matcher(line); 
		m.find();
		return Integer.parseInt(m.group());
	}

	protected double findDouble(String line)
	{
		Pattern p = Pattern.compile("\\d*\\.\\d+");
		Matcher m = p.matcher(line); 
		m.find();
		return Double.parseDouble(m.group());
	}
	
	protected void abortRun(String expHosts)
	{
		String line;
		String command=DBshutdown;
		try {
			ProcessBuilder pb = new ProcessBuilder(command, Integer.toString(numDBNodes), expHosts, DBdataDirectory);
			pb.directory(new File(scriptPath));
			pb.redirectErrorStream(true);
			Process p = pb.start();
			InputStream stdout = p.getInputStream ();

			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));
			while ((line = reader.readLine ()) != null) {
				LOGGER.info ("Stdout: " + line);
			}
			int return_code=p.waitFor();
			LOGGER.info("Return Code:" + return_code);
		} catch (IOException e) {
			LOGGER.error("Cassandra failed to start");
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Executes a single experiment run. The values of all parameters annotated
	 * with @InputParameter are set automatically, such that the parameters can
	 * be used directly in this method.
	 */
	@Override
	protected void runExperiment() throws ExperimentFailedException {

		LOGGER.info("Starting experiment run");

		List<String> hostList = Arrays.asList(hosts.split(","));

		String startString="";
		String expHosts = startString;

		int size = Math.max(1, Math.min(numDBNodes, hostList.size()));

		for (int a=0;a<size;a++)
		{
			expHosts=expHosts.concat(hostList.get(a));
			if (a != size-1)
			{
				expHosts=expHosts.concat(",");
			}

		} 

		CassandraThread cRun = null; //extend all db threads from same parent class
		if (dbname.contains("cassandra"))
		{
			cRun = new CassandraThread(numDBNodes, expHosts, scriptPath, DBPath, DBdataDirectory);
		}
		else
		{
			LOGGER.info("Db type is currently not supported.");
			LOGGER.info("Key-value stores supported:");
			LOGGER.info("cassandra");
			LOGGER.info("\n");
			LOGGER.info("Aborting experiment run");
			return;
		}
		Thread dbThread = new Thread(cRun);
		dbThread.start();
		
		while (!cRun.isFinished())
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOGGER.error("Database not started correctly. Aborting experiment run.");
				abortRun(expHosts);
				try {
					dbThread.join();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				return;
			}
		}
		
		try 
		{
			Thread.sleep(5000);
		}
		catch (InterruptedException e)
		{
			LOGGER.error("Thread interrupted");
		}
		
		String line;
		String command=DBsetup;
		try {
			ProcessBuilder pb = new ProcessBuilder(command, Integer.toString(numDBNodes), expHosts, placementStrategy, strategy_options);
			pb.directory(new File(scriptPath));
			pb.redirectErrorStream(true);
			Process p = pb.start();
			InputStream stdout = p.getInputStream ();

			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));
			while ((line = reader.readLine ()) != null) {
				LOGGER.info ("Stdout: " + line);
			}
			int return_code=p.waitFor();
			LOGGER.info("Return Code:" + return_code);
		} catch (Exception e) {
			LOGGER.error("Did not create database necessary for YCSB. Aborting experiment run");
			abortRun(expHosts);
			try {
				dbThread.join();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			return;
		} 

		command="./runYCSB.sh";
		List<String> list = new ArrayList<String>();
		list.add(command);
		list.add(Boolean.toString(dotransactions));
		list.add(dbname);
		list.add(workloadname);
		list.add(expHosts);
		list.add(Integer.toString(tcount));
		list.add(Integer.toString(ttarget));
		list.add(Integer.toString(recordcount));
		list.add(Integer.toString(operationcount));
		list.add(Integer.toString(insertcount));
		list.add(Integer.toString(fieldcount));
		list.add(Integer.toString(fieldlength));
		list.add(Integer.toString(maxscanlength));
		list.add(Integer.toString(maxexecutiontime));
		list.add(Double.toString(readproportion));
		list.add(Double.toString(insertproportion));
		list.add(Double.toString(scanproportion));
		list.add(Double.toString(inputproportion));
		list.add(Double.toString(updateproportion));
		list.add(clients);
		list.add(requestDistribution);
		list.add(table);
		list.add(YCSBpath);
		list.add(JAVApath);
		
		try {	
			ProcessBuilder pb = new ProcessBuilder(list);
			pb.directory(new File(scriptPath));
			pb.redirectErrorStream(true);
			Process p = pb.start();
			InputStream stdout = p.getInputStream ();

			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));
			while ((line = reader.readLine ()) != null) {
				filteredOutput(line);
				
				if (line.contains("InvalidRequestException")||line.contains("Still waiting for thread"))
				{
					LOGGER.error("Improper Setup. Aborting Experiment Run");
					abortRun(expHosts);
					p.destroy();
					try {
						dbThread.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					return;
				}
				
				if (line.contains("RunTime(ms)"))
				{
					runtime.addValue(new Integer(findInteger(line)));
				}
				else if (line.contains("Throughput(ops/sec)"))
				{
					throughput.addValue(new Double (findDouble(line)));
				}
				else if (line.contains("Operations"))
				{
					operations.addValue(new Integer(findInteger(line)));
				}
				else if (line.contains("AverageLatency(us)"))
				{
					averageLatency.addValue(new Double (findDouble(line)));
				}
				else if (line.contains("MinLatency(us)"))
				{
					minLatency.addValue(new Integer(findInteger(line)));
				}
				else if (line.contains("MaxLatency(us)"))
				{
					maxLatency.addValue(new Integer(findInteger(line)));
				}
			}

			int return_code=p.waitFor();
			LOGGER.info("Return Code:" + return_code);
		} catch (IOException e) {
			LOGGER.error("YCSB failed to run");
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		abortRun(expHosts);
		
		try {
			dbThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		LOGGER.info("Finished experiment run");

	}

	/**
	 * The main method is required to start the MEController application and
	 * register the controller at the SoPeCo-Service. The MEController connects
	 * to the SoPeCo service on Port 8089 at the host app.sopeco.org with the
	 * given identifier. If not specified, a new identifier is automatically
	 * created every time when the MEController is started if not specified. The
	 * identifier acts as protection.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		MECApplication mecapp = MECApplication.get();
		mecapp.addMeasurementController(MEC_NAME,
				new YCSBMEC());
		mecapp.socketConnect(SOPECO_SAAS_URL, SOPECO_SAAS_PORT, MEC_ID);

	}

}
