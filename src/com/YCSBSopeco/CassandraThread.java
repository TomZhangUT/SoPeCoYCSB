package com.YCSBSopeco;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraThread implements Runnable {

	private int numDBNodes;
	private String scriptPath;
	
	/**	
	 * Logger used for debugging and log-information.
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(YCSBMEC.class);
	
	public CassandraThread(int nodes, String path)
	{
			numDBNodes=nodes;
			scriptPath=path;
	}
	
	@Override
	public void run() {
		String command = "./runCassandra.sh";
		String line;
		//String command = scriptPath+"/runCassandra.sh";
		
		try {
			ProcessBuilder pb = new ProcessBuilder(command, Integer.toString(numDBNodes));
			//ProcessBuilder pb = new ProcessBuilder(command, Integer.toString(numDBNodes));
            pb.directory(new File(scriptPath));
			pb.redirectErrorStream(true);
			Process p = pb.start();
			
			InputStream stdout = p.getInputStream ();
			
			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));
			while ((line = reader.readLine ()) != null) {
				LOGGER.info ("Stdout: " + line);
			}
			
			//int return_code=p.waitFor();
			//LOGGER.info("Return Code:" + return_code);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
