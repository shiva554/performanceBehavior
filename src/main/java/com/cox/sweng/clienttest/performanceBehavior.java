package com.cox.sweng.clienttest;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class performanceBehavior {
	public static void updateAverage(String grpKey, Logger logger, FileHandler logFile1, double totalData,
			double totalCount, double sd, Map<String, Float> latencyData, Map<String, Integer> counter,
			ArrayList timestampArr, String type) throws InterruptedException, RuntimeException, IOException {
		Integer j = 0;
		Float average = (float) 0;

		Double totalAvgData = totalData / totalCount;

		for (Map.Entry m : latencyData.entrySet()) {

			String key = (String) m.getKey();
			if (key.contains(grpKey)) {
				// System.out.println(key);
				average = (float) (latencyData.get(key) / counter.get(key));

				String[] cols = key.split("-");

				if (average >= 0) {
					Float result = (float) ((Math.abs(totalAvgData - average)) - sd);
					if (result <= 0) {
						System.out.println("In range " + key);
						logger.setLevel(Level.INFO);
						SimpleFormatter formatter3 = new SimpleFormatter();
						logFile1.setFormatter(formatter3);
						logger.info("in Range " + key);
					} else {

						// System.out.println("Out of range " +key);

						logger.setLevel(Level.WARNING);
						SimpleFormatter formatter3 = new SimpleFormatter();
						logFile1.setFormatter(formatter3);

						logger.warning("This key " + key + "is out of standard deviation");

					}
				}

				if (timestampArr.size() - 1 > j) {
					j++;
				}

			}
		}

	}

	public static void main(String[] args) throws Exception, IOException {

		Logger logger = Logger.getLogger(performanceBehavior.class.getName());
		FileHandler logFile1 = new FileHandler(args[0], true);
		logger.addHandler(logFile1);

		Connection con = null;
		Statement stmt = null;
		ArrayList timestampArr = new ArrayList();
		int indx = 0;
		String lastTS = null;

		// String csvFile = args[0];
		// String cvsSplitBy = ",";
		// FileHandler logFile = new FileHandler(args[1], true);

		// String line = "";

		// String cvsSplitBy = ","; //float llg; String avgofn = args[3]; int
		/*
		 * i = 0; long lastTimeStamp = -1; int startingMin = -1; int endMin = 0;
		 * int x = 0; Integer[] numLine;
		 */

		Map<String, Float> totalLatencyData = new TreeMap<String, Float>();
		Map<String, Float> totalJitterData = new TreeMap<String, Float>();
		Map<String, Float> totalLossData = new TreeMap<String, Float>();
		Map<String, Integer> totalCounter = new TreeMap<String, Integer>();

		Map<String, Float> latencyData = new TreeMap<String, Float>();
		Map<String, Float> jitterData = new TreeMap<String, Float>();
		Map<String, Float> lossData = new TreeMap<String, Float>();
		Map<String, Integer> counter = new TreeMap<String, Integer>();

		try {

			Properties prop = new Properties();
			InputStream input = null;
			// input = new
			// FileInputStream("/etc/cox/pnh/cox-pnh-mysql-service/config.properties");
			input = new FileInputStream("src/deb/cfg/config.properties");
			// input = new
			// FileInputStream("/home/shiva/packagetest/perf_behavior/config.properties");
			prop.load(input);

			// Connecting to mysql db
			
			 Class.forName(prop.getProperty("jdbcdrivers"));
			 con=DriverManager.getConnection(prop.getProperty("url"),prop.
			 getProperty("uname"),prop.getProperty("pwd"));
			 stmt=con.createStatement();
			 
			// get the req from config

			String starttime = performanceBehavior.Dateconverter(prop.getProperty("starttime"));
			System.out.println("starttimestamp " + starttime);
			String endtime = performanceBehavior.Dateconverter(prop.getProperty("endtime"));
			System.out.println("endtimestamp   " + endtime);

			ResultSet rs = stmt.executeQuery("select * from PERFORMANCE_LATENCY where timestamp BETWEEN '" + starttime + "' AND '" + endtime + "')");
			
			//select *, 'LATENCY' AS type from PERFORMANCE_LATENCY where timestamp BETWEEN '1522261502' AND '1522261802' union select *, 'JITTER' AS type from PERFORMANCE_JITTER  where timestamp BETWEEN '1522261502' AND '1522261802' union select *, 'LOSS' AS type from PERFORMANCE_LOSS where timestamp BETWEEN '1522261502' AND '1522261802';


			while (rs.next()) {

				// BufferedReader br = new BufferedReader(new
				// FileReader(csvFile));

				// while ((line = br.readLine()) != null) {

				// System.out.print("testing data " + indx);

				System.out.print(rs.getString("timestamp") + " - " + rs.getString("verifierid") + " - "+ rs.getString("peerverifier") + " - " + rs.getString("slaName") + " - "+ rs.getString("pathid"));
				String src = rs.getString("verifierid");
				String dstn = rs.getString("peerverifier");
				String service = rs.getString("slaName");
				String path = rs.getString("pathid");
				String type = rs.getString("type");
				Float latency = (float) 0.0;
				Float jitterAvg = (float) 0.0;
				Float loss = (float) 0.0;
				 
				
				 
			//	 Float jitterAvg = rs.getFloat("jitterAverageToResponder");
			//	Float loss = rs.getFloat("lostPacketsToResponder");
				String ts = rs.getString("timestamp");
				lastTS = ts;

					//ues sss
				/* 
				 * * String[] Column = line.split(cvsSplitBy); lastTS =
				 * Column[0]; String src = Column[1]; String service =
				 * Column[2]; String path = Column[3]; String dstn = Column[4];
				 * // Float latency = Float.parseFloat(Column[5]); // Float
				 * jitterAvg = Float.parseFloat(Column[6]); Float loss =
				 * Float.parseFloat(Column[5]);
				 */

				String uniqueKey = src + "-" + dstn + "-" + service + "-" + path + "-" + lastTS; // unique
																									// keys
																									// for
																									// src
																									// dstn....

				// System.out.print("print key -- " + uniqueKey );
				String uniqueKeyfortotal = src + "-" + dstn + "-" + service + "-" + path; // unique
																							// key
																							// to
																							// get
																							// total
																							// matches
				
				 if(type.equals("JITTER")) { 
					 jitterAvg = rs.getFloat("endEndDelayAVg");
					 
					 if (totalJitterData.containsKey(uniqueKeyfortotal)) {
						  totalJitterData.put(uniqueKeyfortotal, (float)(totalJitterData.get(uniqueKeyfortotal) + (jitterAvg)));
						  totalCounter.put(uniqueKeyfortotal, totalCounter.get(uniqueKeyfortotal) + 1);
					} else {
						  totalJitterData.put(uniqueKeyfortotal, (jitterAvg));
						  totalCounter.put(uniqueKeyfortotal, 1);
					}

					if (jitterData.containsKey(uniqueKey)) {
						
						jitterData.put(uniqueKey, (float)(jitterData.get(uniqueKey) + (jitterAvg)));
					    counter.put(uniqueKey, counter.get(uniqueKey) + 1);
					} else {
						  
						  jitterData.put(uniqueKey, (jitterAvg));	
						  counter.put(uniqueKey, 1);
						  timestampArr.add(lastTS);
					}
				 }
				 
				 if(type.equals("LATENCY")) {
					  latency = rs.getFloat("endEndDelayAVg");
					  
					  if (totalLatencyData.containsKey(uniqueKeyfortotal)) {
						  totalLatencyData.put(uniqueKeyfortotal, (float)(totalLatencyData.get(uniqueKeyfortotal) + (latency)));
						  totalCounter.put(uniqueKeyfortotal, totalCounter.get(uniqueKeyfortotal) + 1);
					} else {
						  totalLatencyData.put(uniqueKeyfortotal, (latency));
						  totalCounter.put(uniqueKeyfortotal, 1);
					}

				
					if (latencyData.containsKey(uniqueKey)) {
						
						latencyData.put(uniqueKey, (float)(latencyData.get(uniqueKey) + (latency)));			
					    counter.put(uniqueKey, counter.get(uniqueKey) + 1);
					} else {
						  latencyData.put(uniqueKey, (latency));		
						  counter.put(uniqueKey, 1);
						  timestampArr.add(lastTS);
					}
					
				 }
				 
				 if(type.equals("LOSS")) {
					  loss = rs.getFloat("endEndDelayAVg");
					  
					  if (totalLossData.containsKey(uniqueKeyfortotal)) {
						  totalLossData.put(uniqueKeyfortotal, (float) (totalLossData.get(uniqueKeyfortotal) + (loss)));
						  totalCounter.put(uniqueKeyfortotal, totalCounter.get(uniqueKeyfortotal) + 1);
					} else {
						  totalLossData.put(uniqueKeyfortotal, (loss));
						  totalCounter.put(uniqueKeyfortotal, 1);
					}

					if (lossData.containsKey(uniqueKey)) {
						
					    lossData.put(uniqueKey, (float) (lossData.get(uniqueKey) + (loss)));
					    counter.put(uniqueKey, counter.get(uniqueKey) + 1);
					} else {
						  lossData.put(uniqueKey, (loss));
						  counter.put(uniqueKey, 1);
						  timestampArr.add(lastTS);
					}
				 }

			
				

				indx++;

			}

			// stmt.close();

			// timestampArr.add(lastTS);

			
			 for (Map.Entry m : totalLatencyData.entrySet()) {
			  
			  String key = (String) m.getKey();
			 
			  double sd = performanceBehavior.calculateSD(key,
			  totalLatencyData.get(key), totalCounter.get(key), latencyData,
			 counter);
			 
			 updateAverage(key, logger,logFile1, totalLatencyData.get(key),totalCounter.get(key), sd, latencyData, counter, timestampArr,"latency"); 
			 
			 }
			  
			  for (Map.Entry m : totalJitterData.entrySet()) {
			 
			  // here we iterate 1 by 1 keys in map
			  
			  String key = (String) m.getKey();
			 
			  double sd = performanceBehavior.calculateSD(key,
			  totalJitterData.get(key), totalCounter.get(key), jitterData,
			  counter);
			  
			  updateAverage(key, logger, logFile1, totalJitterData.get(key),totalCounter.get(key), sd, jitterData, counter, timestampArr,"jitter");
			  
			  }
			 

			for (Map.Entry m : totalLossData.entrySet()) {

				// here we iterate 1 by 1 keys in map

				String key = (String) m.getKey();

				double sd = performanceBehavior.calculateSD(key, totalLossData.get(key), totalCounter.get(key),
						lossData, counter);

				updateAverage(key, logger, logFile1, totalLossData.get(key), totalCounter.get(key), sd, lossData,
						counter, timestampArr, "loss");
			}
			 

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (logFile1 != null) {
				try {
					logFile1.close();
				} catch (Exception e) {
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}
		System.exit(0);
	}

	public static double calculateSD(String grpKey, double totalData, double totalCount, Map<String, Float> latencyData,
			Map<String, Integer> counter) {

		Float average = (float) 0;

		double sum = 0.0;
		double standardDeviation = 0;

		double mean = totalData / totalCount;

		for (Map.Entry m : latencyData.entrySet()) {

			String key = (String) m.getKey();

			if (key.contains(grpKey)) {

				average = (float) (latencyData.get(key) / counter.get(key));

				standardDeviation += Math.pow(average - 1 * mean, 2);
			}
		}
		return Math.sqrt(standardDeviation / totalCount);
	}

	public static String Dateconverter(String input) throws Exception {

		if (input.equals("now")) {
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/yy hh:mm:ss a");
			LocalDateTime now = LocalDateTime.now();
			String currentDate = dtf.format(now);
			input = currentDate;
		}

		String parsedDate = input.replaceAll("\"", "");

		DateFormat formatter = new SimpleDateFormat("MM/dd/yy hh:mm:ss a");
		Date date = formatter.parse(parsedDate);

		DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("MM/dd/yy hh:mm:ss a");
		LocalDateTime dateTime = LocalDateTime.parse(parsedDate, formatter1);

		long lastTimeStamp = date.getTime() / 1000;
		String ts = Long.toString(lastTimeStamp);
		return ts;

	}
}