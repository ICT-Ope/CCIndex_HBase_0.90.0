/**
 * Copyright 2009 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.ccindex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Random;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ccindex.CCIndexAdmin;
import org.apache.hadoop.hbase.client.ccindex.CCIndexConstants;
import org.apache.hadoop.hbase.client.ccindex.CCIndexDescriptor;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

public class Checker extends Thread {
	public static final Log LOG = LogFactory.getLog(Checker.class);
	private final Configuration conf;
	private final byte[] targetTableName;
	private CCIndexDescriptor des;
	private HashMap<IndexSpecification, HTable> CCITS = new HashMap<IndexSpecification, HTable>();
	private IndexSpecification indexOrg = null;
	private HTable baseTable = null;
	private long sleepTime = 10;
	private int maxSpace = 200;
	private boolean isbase = false;
	private HRegionServer server;

	public Checker(Configuration conf2, byte[] targetTableName,
			CCIndexDescriptor des, HRegionServer server) {
		this.targetTableName = targetTableName;
		this.des = des;
		this.conf = conf2;
		this.server = server;

	}

	public void init() {
		byte[] base = this.des.getBaseTableDescriptor().getName();
		if (Bytes.compareTo(base, targetTableName) != 0) {
			// if this checker's target table is not base table.
			HTable indexTable = null;
			try {
				indexTable = new HTable(conf, base);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (indexTable != null) {
				this.baseTable = indexTable;
			}
		} else {
			this.isbase = true;
		}
		for (IndexSpecification index : des.getIndexes()) {
			byte[] name = index.getCCITName();
			if (Bytes.compareTo(name, targetTableName) != 0) {
				HTable indexTable = null;
				try {
					indexTable = new HTable(conf, name);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (indexTable != null)
					this.CCITS.put(index, indexTable);
			} else {
				this.indexOrg = index;
			}
		}
	}

	private static boolean emptyRow(Result oldResult) {
		if (oldResult == null || oldResult.raw() == null
				|| oldResult.list() == null) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("static-access")
	private boolean recoveryDataForBase(byte[]row,HTable target,SortedMap<byte[], byte[]> columnValues)
	{
		boolean success=false;
		for (IndexSpecification ind : this.CCITS
				.keySet()) {
			try {
				byte[] indexRow = ind
						.getKeyGenerator()
						.createIndexKey(
								row,
								columnValues
										.get(ind
												.getIndexedColumn()));
				LOG.info("recover from"
						+ ind.getIndexId()+"recover from get row :"+new String(indexRow));
				Result recover = this.CCITS.get(ind)
						.get(new Get(indexRow));
				SortedMap<byte[], byte[]> columns = IndexedRegion
						.convertToValueMap(recover);
				Put update = IndexMaintenanceUtils
						.recoverPut(row,
								columns,true,this.des);
				
				target.put(update);
				success=true;
				break;
			} catch (Throwable e) {
				e.printStackTrace();
				System.out.println("get from:"
						+ ind.getIndexId() + " error");
				continue;
				// TODO Auto-generated catch block
			}
		}
		return success;
	}
	private boolean recoveryDataForCCIT(byte[] row,HTable org,SortedMap<byte[], byte[]> columnValues)
	{
		boolean success=false;
		System.out.println("indexed table lost data");
		SimpleIndexKeyGenerator sg = (SimpleIndexKeyGenerator) this.indexOrg
				.getKeyGenerator();

		byte[] rowBase = sg.getOrgRowKey(row);
		Result recovery = null;
		try {
			if (this.baseTable != null)
				recovery = this.baseTable.get(new Get(
						rowBase));
		} catch (Throwable e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			System.out.println("get from:base error");

		}
		if (!this.emptyRow(recovery)) {
			SortedMap<byte[], byte[]> columns = IndexedRegion
					.convertToValueMap(recovery);
			Put update = IndexMaintenanceUtils
					.recoverPut(row, columns,false,this.des);
			try {
				org.put(update);
				success = true;
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (!success) {
			for (IndexSpecification ind : this.CCITS
					.keySet()) {
				if(Bytes.equals(ind.getCCITName(),org.getTableName()))
					continue;
				try {
					byte[] indexRow = ind
							.getKeyGenerator()
							.createIndexKey(
									rowBase,
									columnValues
											.get(ind
													.getIndexedColumn()));

					Result recovery2 = this.CCITS.get(
							ind).get(new Get(indexRow));
					if (!this.emptyRow(recovery2)) {

						SortedMap<byte[], byte[]> columns = IndexedRegion
								.convertToValueMap(recovery2);
						Put update = IndexMaintenanceUtils
								.recoverPutSimple(row,
										columns);
						org.put(update);
						success = true;
						break;
					}
				} catch (NotServingRegionException e1) {
					continue;
				} catch (Throwable e) {
					// TODO Auto-generated catch
					// block
					e.printStackTrace();
					System.out.println("get from:"
							+ ind.getIndexId()
							+ " error");
					continue;
				}

			}
		}
		return success;
	}
	private int batchRecover(Result r,HTable org,ResultScanner scanner,int setOfOnceRecovery)
	{
		Result oldResult=null;
		int recoverNum=0;
		while (this.emptyRow(oldResult) && (!this.emptyRow(r))) {
			boolean recovered = false;
			SortedMap<byte[], byte[]> columnValues = IndexedRegion
					.convertToValueMap(r);

			LOG.info("table :"
					+ new String(this.targetTableName)
					+ "  lost data: "+ r);
			if (isbase) {
				recovered=recoveryDataForBase(r.getRow(),org,columnValues);
			} else {
				recovered=recoveryDataForCCIT(r.getRow(),org,columnValues);
			}
			if (recovered == false) {
				LOG.info("recover failed for row:"+r+" in table:"+org);
			}
			try {
				r = scanner.next();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			recoverNum++;
			if (!this.emptyRow(r)) {
				oldResult = null;
				setOfOnceRecovery--;
				if (setOfOnceRecovery == 0)
					try {
						setOfOnceRecovery =100;
						Get get = new Get(r.getRow());
						oldResult = org.get(get);
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						LOG.debug("there are data lost in :"+org);
						LOG.debug("lost data is:"+ r);
					}
			}
			
		}
		return recoverNum;
	}
	private void recovery(ResultScanner scanner, HTable org, int space) {
		Result r = null;
		try {
			while ((r = scanner.next()) != null && space > 0
					&& !this.server.isStopped()) {
				if (!this.emptyRow(r)) {
					Get get = new Get(r.getRow());
					Result oldResult = null;
					try {
						oldResult = org.get(get);
					} catch (Exception e2) {
						e2.printStackTrace();
					}
					int setOfOnceRecovery = 100;
					if (this.emptyRow(oldResult)) {
						space-=batchRecover(r,org,scanner,100);
					}
				}
				
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@SuppressWarnings("static-access")
	public void run() {
		this.init();
		HTable targetCCT = null;
		HTable targetCCIT = null;
		try {
			targetCCIT = new HTable(conf, this.targetTableName);
			targetCCIT.setWriteBufferSize(1024 * 1024 * 5);
			targetCCT = new HTable(conf, Bytes.add(this.targetTableName, Bytes
					.toBytes("-"), Bytes.toBytes(CCIndexConstants.CCT_TAIL)));
			targetCCT.setScannerCaching(400);
			Scan scan = new Scan();
			ResultScanner CCTScanner = null;
			CCTScanner = targetCCT.getScanner(scan);
			Result r = null;
			Random d = new Random(System.currentTimeMillis());
			Result start = null;
			while (!this.emptyRow((r = CCTScanner.next()))
					&& !this.server.isStopped()) {
				int orgSpace = d.nextInt(this.maxSpace);
				int space = orgSpace;
				try {
					sleep(this.sleepTime);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				while (space > 0 && !this.emptyRow(r)) {
					r = CCTScanner.next();
					space--;
				}
				if (!this.emptyRow(r)) {
					Get get = new Get(r.getRow());
					Result oldResult = null;
					try {
						oldResult = targetCCIT.get(get);
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (this.emptyRow(oldResult)) {
						Scan scanForR = new Scan();
						if (start != null) {
							scanForR.setStartRow(start.getRow());
						}
						ResultScanner scannerForR = null;
						scannerForR = targetCCT.getScanner(scanForR);
						this.recovery(scannerForR, targetCCIT, orgSpace);
					}
					start = r;

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (targetCCIT != null)
			try {
				targetCCIT.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}