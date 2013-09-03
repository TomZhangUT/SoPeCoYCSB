package com.YCSBSopeco;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBthread implements Runnable {

	private int numDBNodes;
	private String scriptPath;
	private String dbPath;
	private String dbData;
	private String hosts;
	
	private boolean finished = false;
	
	/**	
	 * Logger used for debugging and log-information.
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(YCSBMEC.class);
	
	public boolean isFinished()
	{
		return finished;
	}
	
	@Override
	public void run() {
		String command = "./DEFAULT";
		String line;
		//String command = scriptPath+"/runCassandra.sh";
		
		try {
			ProcessBuilder pb = new ProcessBuilder(command, Integer.toString(numDBNodes), hosts, dbPath, dbData);
            pb.directory(new File(scriptPath));
			pb.redirectErrorStream(true);
			Process p = pb.start();
			
			InputStream stdout = p.getInputStream ();
			
			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));
			while ((line = reader.readLine ()) != null) {
				LOGGER.info ("Stdout: " + line);
				if (line.contains("Now serving reads."))
				{
					finished = true;
				}
				else if (line.contains("java.net.BindException"))
				{
					LOGGER.debug("Cassandra already running");
					finished = true;
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("Check script path");
		}
	}

}
