/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
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
package org.vanilladb.core.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class TimerStatistics {
	
	private static final long REPORT_PERIOD = 3000;
	private static final AtomicBoolean IS_REPORTING = new AtomicBoolean(false); 
	
	private static List<Object> components = new LinkedList<Object>();
	private static Map<Object, ComponentStatistic> stats = new ConcurrentHashMap<Object, ComponentStatistic>();
	private static Object[] anchors = new Object[1099];
	
	private static class ComponentStatistic {
		long recordTime;
		int lastCount, count;
		
		synchronized void addTime(long time) {
			recordTime += time;
			count++;
		}
		
		synchronized double calAverage() {
			double avg = 0;
			if (count != 0) {
				avg = ((double) recordTime) / ((double) count);
			}
			recordTime = 0;
			lastCount = count;
			count = 0;
			return avg;
		}
		
		synchronized int getLastCount() {
			return lastCount;
		}
	}
	
	static {
		for (int i = 0; i < anchors.length; i++)
			anchors[i] = new Object();
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				long startTime = System.currentTimeMillis();
				long lastTime = startTime;
				
				while (true) {
					long currentTime = System.currentTimeMillis();
					
					if (currentTime - lastTime > REPORT_PERIOD) {
						lastTime = currentTime;
						
						double t = (currentTime - startTime) / 1000;
						StringBuilder sb = new StringBuilder();
						
						sb.append("===================================\n");
						sb.append(String.format("At %.2f:\n", t));
						
						synchronized (components) {
							for (Object key : components) {
								ComponentStatistic stat = stats.get(key);
								double avg = stat.calAverage();
								int count = stat.getLastCount();
								
								if (count > 0) {
									sb.append(String.format("%s: average %.2f us", key, avg));
									sb.append(String.format(" with count %d\n", count));
								}
							}
						}
						
						sb.append("===================================\n");
						
						if (IS_REPORTING.get())
							System.out.println(sb.toString());
					}
					
					try {
						Thread.sleep(REPORT_PERIOD / 10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
		}).start();
	}
	
	public static void startReporting() {
		IS_REPORTING.set(true);
	}
	
	public static void stopReporting() {
		IS_REPORTING.set(false);
	}
	
	static void add(Timer timer) {
		if (IS_REPORTING.get()) {
			for (Object com : timer.getComponents()) {
				ComponentStatistic stat = stats.get(com);
				if (stat == null)
					stat = createNewStatistic(com);
				stat.addTime(timer.getComponentTime(com));
			}
		}
	}
	
	private static Object getAnchor(Object key) {
		int code = key.hashCode();
		code = Math.abs(code); // avoid negative value
		return anchors[code % anchors.length];
	}
	
	private static ComponentStatistic createNewStatistic(Object key) {
		Object anchor = getAnchor(key);
		synchronized (anchor) {
			
			ComponentStatistic stat = stats.get(key);
			
			if (stat == null) {
				stat = new ComponentStatistic();
				stats.put(key, new ComponentStatistic());
				synchronized (components) {
					components.add(key);
				}
			}
			
			return stat;
		}
	}
}
