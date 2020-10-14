/*/*******************************************************************************
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
/*package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
//import org.vanilladb.bench.util.RandomValueGenerator;
import java.util.Random;

public class UpdateItemTxnJdbcJob implements JdbcJob {
	private static Logger logger = Logger.getLogger(UpdateItemTxnJdbcJob.class
			.getName());
	
	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {
		// Parse parameters
		int readCount = (Integer) pars[0];
		int[] itemIds = new int[readCount];
		for (int i = 0; i < readCount; i++)
			itemIds[i] = (Integer) pars[i + 1];
		
		// Output message
		StringBuilder outputMsg = new StringBuilder("[");
		
		// Execute logic
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = null;
			for (int i = 0; i < 10; i++) {
				String sql = "SELECT i_name, i_price FROM item WHERE i_id = " + itemIds[i];
				rs = statement.executeQuery(sql);
				//modification====================================
				double getPrice = 0;//rs.getDouble("i_price");
				if (getPrice > As2BenchConstants.MAX_PRICE)	getPrice = As2BenchConstants.MIN_PRICE;
				else
				{
					Random rnd = new Random(); 
					double num = rnd.nextDouble();
					getPrice += num*5;
				}
				String UpdateSql = "Update item SET i_price = "+getPrice+"WHERE i_id = "+itemIds[i];
				statement.executeUpdate(UpdateSql);
				//modification=====================================
				rs.beforeFirst();
				if (rs.next()) {
					outputMsg.append(String.format("'%s', ", rs.getString("i_name")));
				} else
					throw new RuntimeException("cannot find the record with i_id = " + itemIds[i]);
				rs.close();
			}
			conn.commit();
			
			outputMsg.deleteCharAt(outputMsg.length() - 2);
			outputMsg.append("]");
			
			return new VanillaDbJdbcResultSet(true, outputMsg.toString());
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.toString());
			return new VanillaDbJdbcResultSet(false, "");
		}
	}
}*/
package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Random;

import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;
import org.vanilladb.bench.benchmarks.as2.As2BenchConstants; //as2

public class UpdateItemTxnJdbcJob implements JdbcJob {
	private static Logger logger = Logger.getLogger(ReadItemTxnJdbcJob.class
			.getName());
	
	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {
		// Parse parameters
		int readCount = (Integer) pars[0];
		int[] itemIds = new int[readCount];
		for (int i = 0; i < readCount; i++)
			itemIds[i] = (Integer) pars[i + 1];		
		
		//as2
		int updateCount = (Integer) pars[0];
		int[] itemIds2 = new int[updateCount];
		for (int i = 0; i < updateCount; i++)
			itemIds2[i] = (Integer) pars[i + 1];
		
		double [] itemPrice = new double[updateCount];
		for (int i = 0; i < updateCount; i++) {
			Random ran = new Random();
	        double r = ran.nextDouble();
	        r *= 5;
	        //System.out.println(r);
	        itemPrice[i] = r;
		}
		//as2

		// Output message
		StringBuilder outputMsg = new StringBuilder("[");
	//	System.out.print("***");

		// Execute logic
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = null;
			for (int i = 0; i < 10; i++) {
				String sql = "SELECT i_name, i_price FROM item WHERE i_id = " + itemIds[i];
				
				rs = statement.executeQuery(sql);
				rs.beforeFirst();	
				if (rs.next()) {
				//	System.out.println(rs.getDouble("i_price"));
					outputMsg.append(String.format("'%s', ", rs.getString("i_name")));
				} else
					throw new RuntimeException("cannot find the record with i_id = " + itemIds[i]);
				//rs.close();
				
				//as2
				int rs1;
			//	String sql1 = "UPDATE item SET i_price = " + As2BenchConstants.MIN_PRICE + "WHERE i_id = "+ itemIds2[i];
			//	rs1 = statement.executeUpdate(sql1);
				
				if (rs.getDouble("i_price") > (double) As2BenchConstants.MAX_PRICE) {
					String sql1 = "UPDATE item SET i_price = " + As2BenchConstants.MIN_PRICE + "WHERE i_id = "+ itemIds2[i];
					rs1 = statement.executeUpdate(sql1);
				} else {
					double new_price;
					new_price = rs.getDouble("i_price") + itemPrice[i] ; //as2
					String sql1 = "UPDATE item SET i_price = " + new_price + "WHERE i_id = "+ itemIds2[i];
					//System.out.println(new_price);
					rs1 = statement.executeUpdate(sql1);
				}
				
				rs.close();
			}
	
			conn.commit();
			
			outputMsg.deleteCharAt(outputMsg.length() - 2);
			outputMsg.append("]");
			
			return new VanillaDbJdbcResultSet(true, outputMsg.toString());
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.toString());
			return new VanillaDbJdbcResultSet(false, "");
		}
	}
}
