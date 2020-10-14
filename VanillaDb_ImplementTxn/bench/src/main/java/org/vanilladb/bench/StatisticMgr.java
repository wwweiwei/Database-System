/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Collections; //as2


import org.vanilladb.bench.util.BenchProperties;

public class StatisticMgr {
	private static Logger logger = Logger.getLogger(StatisticMgr.class.getName());

	private static final File OUTPUT_DIR;


	static {
		String outputDirPath = BenchProperties.getLoader().getPropertyAsString(StatisticMgr.class.getName()
				+ ".OUTPUT_DIR", null);
		
		if (outputDirPath == null) {
			OUTPUT_DIR = new File(System.getProperty("user.home"), "benchmark_results");
		} else {
			OUTPUT_DIR = new File(outputDirPath);
		}

		// Create the directory if that doesn't exist
		if (!OUTPUT_DIR.exists())
			OUTPUT_DIR.mkdir();
		
		
	}

	private static class TxnStatistic {
		private BenchTransactionType mType;
		private int txnCount = 0;
		private long totalResponseTimeNs = 0;

		public TxnStatistic(BenchTransactionType txnType) {
			this.mType = txnType;
		}

		public BenchTransactionType getmType() {
			return mType;
		}

		public void addTxnResponseTime(long responseTime) {
			txnCount++;
			totalResponseTimeNs += responseTime;
		}

		public int getTxnCount() {
			return txnCount;
		}

		public long getTotalResponseTime() {
			return totalResponseTimeNs;
		}
	}

	private List<TxnResultSet> resultSets = new ArrayList<TxnResultSet>();
	private List<BenchTransactionType> allTxTypes;
	private String fileNamePostfix = "";
	private long recordStartTime = -1;
	
	public StatisticMgr(Collection<BenchTransactionType> txTypes) {
		allTxTypes = new LinkedList<BenchTransactionType>(txTypes);
	}
	
	public StatisticMgr(Collection<BenchTransactionType> txTypes, String namePostfix) {
		allTxTypes = new LinkedList<BenchTransactionType>(txTypes);
		fileNamePostfix = namePostfix;
	}
	
	/**
	 * We use the time that this method is called at as the start time for recording.
	 */
	public synchronized void setRecordStartTime() {
		if (recordStartTime == -1)
			recordStartTime = System.nanoTime();
	}

	public synchronized void processTxnResult(TxnResultSet trs) {
		if (recordStartTime == -1)
			recordStartTime = trs.getTxnEndTime();
		resultSets.add(trs);
	}

	public synchronized void outputReport() {
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss"); // E.g. "20180524-200824"
			String fileName = formatter.format(Calendar.getInstance().getTime());
			if (fileNamePostfix != null && !fileNamePostfix.isEmpty())
				fileName += "-" + fileNamePostfix; // E.g. "20200524-200824-postfix"
			
			outputDetailReport(fileName);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Finnish creating tpcc benchmark report");
	}
	
	private void outputDetailReport(String fileName) throws IOException {
		Map<BenchTransactionType, TxnStatistic> txnStatistics = new HashMap<BenchTransactionType, TxnStatistic>();
		Map<BenchTransactionType, Integer> abortedCounts = new HashMap<BenchTransactionType, Integer>();
		
		for (BenchTransactionType type : allTxTypes) {
			txnStatistics.put(type, new TxnStatistic(type));
			abortedCounts.put(type, 0);
		}
		
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(OUTPUT_DIR, fileName + ".txt")));
			BufferedWriter writer1 = new BufferedWriter(new FileWriter(new File(OUTPUT_DIR, fileName + ".csv")))
		) {
			// First line: total transaction count
						
			writer.write("# of txns (including aborted) during benchmark period: " + resultSets.size());
			writer.newLine();
			
			writer1.write("time(sec), throughput(txs), avg_latency(ms), min(ms), max(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms)" );
			writer1.newLine();
			
			
			//as2
			//Timer timer = new Timer();
			//timer.schedule(new DateTask(), 3000, 5000);
			
			long totalTime = 0;
			long timePeriod = 0;
			int i = 1;
			List<Long> time = new ArrayList<Long>();
			//as2
			
			// Detail latency report
			for (TxnResultSet resultSet : resultSets) {
				// as2
				long curTime = resultSet.getTxnResponseTime();
				// as2
				
				if (resultSet.isTxnIsCommited()) {
					// Write a line: {[Tx Type]: [Latency]}
					writer.write(resultSet.getTxnType() + ": "
							+ TimeUnit.NANOSECONDS.toMillis(resultSet.getTxnResponseTime()) + " ms");
					writer.newLine();
					
					// Count transaction for each type
					TxnStatistic txnStatistic = txnStatistics.get(resultSet.getTxnType());
					txnStatistic.addTxnResponseTime(resultSet.getTxnResponseTime());	
					
				} else {
					writer.write(resultSet.getTxnType() + ": ABORTED");
					writer.newLine();
					
					// Count transaction for each type
					Integer count = abortedCounts.get(resultSet.getTxnType());
					abortedCounts.put(resultSet.getTxnType(), count + 1);
				}
				
				// as2
				TxnStatistic txnStatistic = txnStatistics.get(resultSet.getTxnType());
				if (txnStatistic != null) {
					if (totalTime < i * 5000) {	
						// not yet
						long milliSec = TimeUnit.NANOSECONDS.toMillis(curTime);
						time.add(milliSec);
						totalTime += milliSec;
						timePeriod += milliSec;
					} else {
						// every 5 seconds
						int size = time.size();
						Collections.sort(time);
						long UpTime = i*5+BenchmarkerParameters.WARM_UP_INTERVAL/1000;
						// time(sec), throughput(txs), avg_latency(ms), min(ms),max(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms)
						writer1.write(UpTime + "," 
								+ size + "," 
								+ timePeriod / size + "," 
								+ time.get(0) + ","
								+ time.get(size - 1) + "," 
								+ time.get(size / 4) + "," 
								+ time.get(size / 2) + ","
								+ time.get(size * 3 / 4));
						writer1.newLine();
						i++;
						time.clear();
						timePeriod = 0;
					}
				}
				
			}

			
			//timer.cancel();
			writer.newLine();
			
			// Last few lines: show the statistics for each type of transactions
			int abortedTotal = 0;
			for (Entry<BenchTransactionType, TxnStatistic> entry : txnStatistics.entrySet()) {
				TxnStatistic value = entry.getValue();
				int abortedCount = abortedCounts.get(entry.getKey());
				abortedTotal += abortedCount;
				long avgResTimeMs = 0;
				
				if (value.txnCount > 0) {
					avgResTimeMs = TimeUnit.NANOSECONDS.toMillis(
							value.getTotalResponseTime() / value.txnCount);
				}
				
				writer.write(value.getmType() + " - committed: " + value.getTxnCount() +
						", aborted: " + abortedCount + ", avg latency: " + avgResTimeMs + " ms");
				writer.newLine();
			}
			
			// Last line: Total statistics
			int finishedCount = resultSets.size() - abortedTotal;
			double avgResTimeMs = 0;
			if (finishedCount > 0) { // Avoid "Divide By Zero"
				for (TxnResultSet rs : resultSets)
					avgResTimeMs += rs.getTxnResponseTime() / finishedCount;
			}
			writer.write(String.format("TOTAL - committed: %d, aborted: %d, avg latency: %d ms", 
					finishedCount, abortedTotal, Math.round(avgResTimeMs / 1000000)));
			
		}
		 
		
		
	}
	
	
	
}