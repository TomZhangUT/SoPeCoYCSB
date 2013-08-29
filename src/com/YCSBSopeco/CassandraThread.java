package com.YCSBSopeco;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraThread extends DBthread {

	private int numDBNodes;
	private String scriptPath;
	private String tokenList;
	private List<String> commandList;
	private boolean finished = false;
	
	/**	
	 * Logger used for debugging and log-information.
	 */
	private static final Logger LOGGER = LoggerFactory
			.getLogger(YCSBMEC.class);
	
	public CassandraThread(List<String> list, String sPath)
	{	
			commandList=list;
			numDBNodes=Integer.parseInt(list.get(2));
			tokenList=calculateTokes(numDBNodes);
			commandList.add(tokenList);
			
			scriptPath=sPath;
			finished=false;
	}
	
	protected String calculateTokes(int numNodes)
	{
		String tokenList="";
		for (int i=0;i<numNodes;i++)
		{
			Integer node = i;
	        Integer total = numNodes;
	        
	        BigInteger token = BigInteger.valueOf(node);
	        BigInteger pow = BigInteger.valueOf(2).pow(127).subtract(BigInteger.ONE);
	        token = token.multiply(pow).divide(BigInteger.valueOf(total));
	        
	        tokenList=tokenList.concat(token.abs().toString());
	        tokenList=tokenList.concat(",");
	        
	        System.out.println("Token "+node+" of "+total+": "+token.abs().toString());
		}
        return tokenList;
	}
	
	public boolean isFinished()
	{
		return finished;
	}
	
	@Override
	public void run() {
		String line;
		
		try {
			ProcessBuilder pb = new ProcessBuilder(commandList);
			//ProcessBuilder pb = new ProcessBuilder(command, Integer.toString(numDBNodes));
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
			
			//int return_code=p.waitFor();
			//LOGGER.info("Return Code:" + return_code);
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("Check script path");
		}
	}

}
