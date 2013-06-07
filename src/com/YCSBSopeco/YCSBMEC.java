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

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBFactory;
import com.yahoo.ycsb.TerminatorThread;
import com.yahoo.ycsb.UnknownDBException;
import com.yahoo.ycsb.Workload;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementPackage;
import com.yahoo.ycsb.measurements.exporter.SoPeCoMeasurementsExporter;

/**
 * YCSBMEC is an example for a Measurement Environment
 * Controller (MEController). Each MEController has to extend the
 * AbstractMEController class which is contained in the
 * org.sopeco.core-VERSION-jar-with-dependencies.jar.
 * 
 * In this example we use the YCSBMEC MEController to
 * investigate the relationship between the response time of a matrix
 * multiplication operation (of different mathematical libraries) and the
 * corresponding matrix sizes.
 * 
 * @author Alexander Wert, Christoph Heger
 * 
 */
public class YCSBMEC extends AbstractMEController {
	/**	
	 * Logger used for debugging and log-information.
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(YCSBMEC.class);

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
	private static final int SOPECO_SAAS_PORT = 8089;

	/**
	 * Address of the SoPeCo SaaS instance to register
	 */
	private static final String SOPECO_SAAS_URL = "localhost";

	public static final String OPERATION_COUNT_PROPERTY="operationcount";

	public static final String RECORD_COUNT_PROPERTY="recordcount";

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

	@InputParameter(namespace = "my.input")
	String hosts= "localhost";

	@InputParameter(namespace = "my.input")
	String workloadname = "com.yahoo.ycsb.workloads.CoreWorkload";

	@InputParameter(namespace = "my.input")
	String dbname = "com.yahoo.ycsb.BasicDB";

	@InputParameter(namespace = "my.input")
	boolean dotransactions = false;

	@InputParameter(namespace = "my.input")
	boolean status = true;

	@InputParameter(namespace = "my.input")
	String measurementtype = "timeseries";

	@InputParameter(namespace = "my.input")
	int tcount = 1;

	@InputParameter(namespace = "my.input")
	int ttarget = 0;

	@InputParameter(namespace = "my.input")
	int recordcount= 1000;

	@InputParameter(namespace = "my.input")
	int operationcount = 0;

	@InputParameter(namespace = "my.input")
	int insertcount = 0;

	@InputParameter(namespace = "my.input")
	int fieldcount= 5;

	@InputParameter(namespace = "my.input")
	int fieldlength= 10;

	@InputParameter(namespace = "my.input")
	int maxscanlength = 50;

	@InputParameter(namespace = "my.input")
	int maxexecutiontime = -1;

	@InputParameter(namespace = "my.input")
	double readproportion= 0.95;

	@InputParameter(namespace = "my.input")
	double insertproportion= 0.05;

	@InputParameter(namespace = "my.input")
	double scanproportion= 0.0;

	@InputParameter(namespace = "my.input")
	double inputproportion= 0.0;

	@InputParameter(namespace = "my.input")
	double updateproportion= 0.0;

	@InputParameter(namespace = "my.input")
	String label = "";
	/**
	 * The sole observation parameter is the response time. Each observation
	 * parameter must be of the type ParameterValueList<T>, whereby T describes
	 * the actual type of the observation parameter. In this case it is a Double
	 * parameter. Observation parameter have to be marked with the @ObservationParameter
	 * annotation!
	 */
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> runtime;

	/*
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Double> throughput;

	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> operations;

	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Double> averageLatency;

	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> minLatency;

	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> maxLatency;

	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> ninetyFifthPercentile;

	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Integer> ninetyninePercentile;

	/*
	@ObservationParameter(namespace = "my.output")
	ParameterValueList<HashMap<Integer,Integer>> returncodes;

	@ObservationParameter(namespace = "my.output")
	ParameterValueList<Vector<Integer>> histogram;

	@ObservationParameter(namespace = "my.output")
	ParameterValueList<HashMap<Integer,Integer>> averageTimes;
	 */

	/**
	 * Constructor. Here, we define which termination conditions for an
	 * experiment are supported by this MEController. If required, you can
	 * specify your own termination conditions.
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
		/*
		addParameterObservationsToResult(throughput);
		addParameterObservationsToResult(operations);
		addParameterObservationsToResult(averageLatency);
		addParameterObservationsToResult(minLatency);
		addParameterObservationsToResult(maxLatency);
		addParameterObservationsToResult(ninetyFifthPercentile);
		addParameterObservationsToResult(ninetyninePercentile);
		addParameterObservationsToResult(returncodes);
		addParameterObservationsToResult(histogram);
		addParameterObservationsToResult(averageTimes);
		*/

	}

	protected void SetMeasurements(MeasurementPackage mPack){
		/*
		if (mPack==null){
			LOGGER.debug("No Measurements.");
		}
		if (mPack.getRuntime()>=0){
			System.out.println ("Runtime: "+ mPack.getRuntime());
			runtime.addValue(new Integer((int) mPack.getRuntime()));
		}
		if (mPack.getThroughput()>=0){
			throughput.addValue(new Double (mPack.getThroughput()));
		}
		if (mPack.getOperations()>=0){
			operations.addValue(new Integer(mPack.getOperations()));
		}
		if (mPack.getAverageLatency()>=0){
			averageLatency.addValue(new Double(mPack.getAverageLatency()));
		}
		if (mPack.getMinLatency()>=0){
			minLatency.addValue(new Integer(mPack.getMinLatency()));
		}
		if (mPack.getMaxLatency()>=0){
			maxLatency.addValue(new Integer(mPack.getMaxLatency()));
		}
		if (mPack.getNinetyFifthPercentile()>=0){
			ninetyFifthPercentile.addValue(new Integer(mPack.getNinetyFifthPercentile()));
		}*/

		System.out.println ("Runtime: "+ mPack.getRuntime());
		runtime.addValue(new Integer((int) mPack.getRuntime()));
		
		/*
		throughput.addValue(new Double (mPack.getThroughput()));
		operations.addValue(new Integer(mPack.getOperations()));
		averageLatency.addValue(new Double(mPack.getAverageLatency()));
		minLatency.addValue(new Integer(mPack.getMinLatency()));
		maxLatency.addValue(new Integer(mPack.getMaxLatency()));
		ninetyFifthPercentile.addValue(new Integer(mPack.getNinetyFifthPercentile()));
		/*
		if (!mPack.getReturncodes().isEmpty()){
			returncodes.addValue(mPack.getReturncodes());
		}
		if (!mPack.getHistogram().isEmpty()){
			histogram.addValue(mPack.getHistogram());
		}
		if (!mPack.getAverageTimes().isEmpty()){
			averageTimes.addValue(mPack.getAverageTimes());
		}
		 */
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
		LOGGER.info("Preparing experiment series - nothing todo");
	}

	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * @throws IOException Either failed to write to output stream or failed to close it.
	 */
	private void exportMeasurements(Properties props, int opcount, long runtime)
			throws IOException
			{
		/*
		SoPeCoMeasurementsExporter exporter = new SoPeCoMeasurementsExporter(new MeasurementPackage());

		exporter.write("OVERALL", "RunTime(ms)", runtime);
		double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
		exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

		Measurements.getMeasurements().exportMeasurements(exporter);

		SetMeasurements(exporter.getMeasurementPackage());

		if (exporter != null)
		{
			exporter.close();
		}
		*/
	
		

			}

	/**
	 * Executes a single experiment run. The values of all parameters annotated
	 * with @InputParameter are set automatically, such that the parameters can
	 * be used directly in this method.
	 */
	@Override
	protected void runExperiment() throws ExperimentFailedException {
		LOGGER.info("Starting experiment run");

		Properties props=new Properties();
		Properties fileprops=new Properties();

		int threadcount=1;
		int target=0;

		props.setProperty("threadcount", tcount+"");
		props.setProperty("target", ttarget+"");
		props.setProperty("db",dbname);
		props.setProperty(WORKLOAD_PROPERTY, workloadname);
		props.setProperty("hosts", hosts);
		props.setProperty(RECORD_COUNT_PROPERTY, recordcount+"");
		props.setProperty(INSERT_COUNT_PROPERTY, insertcount+"");
		props.setProperty(OPERATION_COUNT_PROPERTY, operationcount+"");
		props.setProperty("fieldcount", fieldcount+"");
		props.setProperty("fieldlength", fieldlength+"");
		props.setProperty("maxscanlength", maxscanlength+"");
		props.setProperty("measurementtype", measurementtype);
		props.setProperty("readproportion", readproportion+"");
		props.setProperty("insertproportion", insertproportion+"");
		props.setProperty("scanproportion", scanproportion+"");
		props.setProperty("updateproportion", updateproportion+"");
		props.setProperty(MAX_EXECUTION_TIME, maxexecutiontime+"");

		//set up logging
		//BasicConfigurator.configure();

		//overwrite file properties with properties from the command line

		//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
		for (Enumeration<?> e=props.propertyNames(); e.hasMoreElements(); )
		{
			String prop=(String)e.nextElement();

			fileprops.setProperty(prop,props.getProperty(prop));
		}

		props=fileprops;

		long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

		//get number of threads, target and db
		threadcount=Integer.parseInt(props.getProperty("threadcount","1"));
		dbname=props.getProperty("db","com.yahoo.ycsb.BasicDB");
		target=Integer.parseInt(props.getProperty("target","0"));

		//compute the target throughput
		double targetperthreadperms=-1;
		if (target>0)
		{
			double targetperthread=((double)target)/((double)threadcount);
			targetperthreadperms=targetperthread/1000.0;
		}	 

		LOGGER.info("YCSB Client 0.1");
		LOGGER.error("Loading workload...");

		//show a warning message that creating the workload is taking a while
		//but only do so if it is taking longer than 2 seconds 
		//(showing the message right away if the setup wasn't taking very long was confusing people)
		Thread warningthread=new Thread() 
		{
			public void run()
			{
				try
				{
					sleep(2000);
				}
				catch (InterruptedException e)
				{
					return;
				}
				LOGGER.error("might take a few minutes for large data sets");
			}
		};

		warningthread.start();

		//set up measurements
		Measurements.setProperties(props);

		//load the workload
		ClassLoader classLoader = YCSBMEC.class.getClassLoader();

		Workload workload=null;

		try 
		{
			Class<?> workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

			workload=(Workload)workloadclass.newInstance();
		}
		catch (Exception e) 
		{  
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			workload.init(props);
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		warningthread.interrupt();

		//run the workload

		LOGGER.info("Starting test.");

		int opcount;
		if (dotransactions)
		{
			System.out.println("Doing Transactions");
			opcount=Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,"0"));
		}
		else
		{
			System.out.println ("Not doing transactions");
			if (props.containsKey(INSERT_COUNT_PROPERTY))
			{
				opcount=Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY,"0"));
			}
			else
			{
				opcount=Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY,"0"));
			}
		}

		Vector<Thread> threads=new Vector<Thread>();

		for (int threadid=0; threadid<threadcount; threadid++)
		{
			DB db=null;
			try
			{
				db=DBFactory.newDB(dbname,props);
			}
			catch (UnknownDBException e)
			{
				System.out.println("Unknown DB "+dbname);
				System.exit(0);
			}

			Thread t=new ClientThread(db,dotransactions,workload,threadid,threadcount,props,opcount/threadcount,targetperthreadperms);

			threads.add(t);
			//t.start();
		}

		StatusThread statusthread=null;

		if (status)
		{
			boolean standardstatus=false;
			if (props.getProperty("measurementtype","").compareTo("timeseries")==0) 
			{
				standardstatus=true;
			}	
			statusthread=new StatusThread(threads,label,standardstatus);
			statusthread.start();
		}

		long st=System.currentTimeMillis();

		for (Thread t : threads)
		{
			t.start();
		}

		Thread terminator = null;

		if (maxExecutionTime > 0) {
			terminator = new TerminatorThread(maxExecutionTime, threads, workload);
			terminator.start();
		}

		int opsDone = 0;

		for (Thread t : threads)
		{
			try
			{
				t.join();
				opsDone += ((ClientThread)t).getOpsDone();
			}
			catch (InterruptedException e)
			{
			}
		}

		long en=System.currentTimeMillis();

		if (terminator != null && !terminator.isInterrupted()) {
			terminator.interrupt();
		}

		if (status)
		{
			statusthread.interrupt();
		}

		try
		{
			workload.cleanup();
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		runtime.addValue(new Integer((int) (en - st)));
		
		try
		{
			exportMeasurements(props, opsDone, en - st);
		} catch (IOException e)
		{
			System.err.println("Could not export measurements, error: " + e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}

		System.exit(0);
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
