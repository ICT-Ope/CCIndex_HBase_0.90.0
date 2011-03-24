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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.ccindex.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ccindex.CCIndexConstants;
import org.apache.hadoop.hbase.client.ccindex.CCIndexDescriptor;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.CCIndexAdmin;
import org.apache.hadoop.hbase.client.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.hash.Hash;
import org.apache.zookeeper.KeeperException;

/**
 * RegionServer which maintains secondary indexes.
 * @author liujia09@software.ict.ac.cn 
 **/
public class IndexedRegionServer extends HRegionServer {

	private static final Log LOG = LogFactory.getLog(IndexedRegionServer.class);
	public static ConcurrentHashMap<Integer, java.util.concurrent.ConcurrentLinkedQueue> taskQueue = new ConcurrentHashMap<Integer, java.util.concurrent.ConcurrentLinkedQueue>();
	public static ConcurrentHashMap<Integer, Worker> workers = new ConcurrentHashMap<Integer, Worker>();
	public static TreeMap<WorkSet, Integer> refineSet = new TreeMap<WorkSet, Integer>(new WorkSet());
	private static int workerN = 20;
	private static int idleWorker = 0;
	public static int jobQueue = 8;
	public static int sequence = 0;
	public static  int checkMasterN=4;
	public static ConcurrentHashMap<IndexSpecification, HTable> indexSpecToTables[];
	public static ConcurrentHashMap<IndexSpecification, HTable> indexSpecToCCTS[];
	public static ConcurrentHashMap<String, HTable> tableToCCTS;
	public static ConcurrentHashMap<String, CCIndexDescriptor> indexTableDescriptorMap;
//if you set the data replica number to one and want to use CCT to check and recovery data.
//is not work so well now. 
//first:	if there are data lost, the HDFS and HBase will retry many times to make sure the data is really lost.
//second:	HBase don't hand the region data lost very well, in 0.20.0 the region server will break down.
//third:	HBase don't have upl evel api to adjust replica number of different tables.In 0.20.0 we adjust 
//some code in HStoreFile to manage the replica number.
	public static final boolean startCheckAdnRecovery=false;
	public static final boolean flushRegions=false;
	
	
	public boolean shouldAddCheckerMaster()
	{
		
		ZooKeeperWatcher zk=super.getZooKeeper();
		String numberN=ZKUtil.joinZNode(zk.baseZNode,CCIndexConstants.CheckNumNode);
		try {
			if(ZKUtil.checkExists(zk,numberN)!=-1)
			{
				ZKUtil.createSetData(zk, numberN, Bytes.toBytes(1));
			}
			else
			{
				int num=Bytes.toInt(ZKUtil.getData(zk, numberN));
				if(num<this.checkMasterN)
				{
					ZKUtil.setData(zk, numberN, Bytes.toBytes(num+1));
					return true;
				}
				else
				{
					return false;
				}
			}
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	private class RegionFlusher extends Thread {
		HRegionServer server;
		int sleepN = 150;

		public RegionFlusher(HRegionServer server) {
			this.server = server;
	
		}
		public void run() {
			while (!this.server.isStopped()) {

				for (HRegionInfo region : this.server.getOnlineRegions()) {
					try {
						this.server.flushRegion(region);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				try {
					for (int i = 0; i < this.sleepN
							&& (!this.server.isStopped()); i++)
						this.sleep(60000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}

	/**
	 * flush all regions to HDFS
	 */
	public synchronized  static void flushTable() {
		for (int i = 0; i < jobQueue; i++) {
			for (HTable table : indexSpecToTables[i].values())
			{
				try {
					// System.out.println("begain flush table!");
					table.flushCommits();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			for (HTable table : indexSpecToCCTS[i].values())

			{
				try {
					// System.out.println("begain flush table!");
					table.flushCommits();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		for (HTable table : tableToCCTS.values())

		{
			try {
				// System.out.println("begain flush table!");
				table.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		IndexedRegion.flushTable();
	}

	public IndexedRegionServer(Configuration conf) throws IOException, InterruptedException {
		super(conf);
		this.init();
	}

	public void init() {
		for (int i = 0; i < this.workerN; i++) {
			java.util.concurrent.ConcurrentLinkedQueue queue = new java.util.concurrent.ConcurrentLinkedQueue();
			taskQueue.put(i, queue);
			workers.put(i, new Worker(queue, this));
			workers.get(i).start();
		}
		indexSpecToTables = new ConcurrentHashMap[jobQueue];
		indexSpecToCCTS = new ConcurrentHashMap[jobQueue];
		tableToCCTS = new ConcurrentHashMap<String, HTable>();
		indexTableDescriptorMap = new ConcurrentHashMap<String, CCIndexDescriptor>();
		for (int i = 0; i < this.jobQueue; i++) {
			indexSpecToTables[i] = new ConcurrentHashMap<IndexSpecification, HTable>();
			indexSpecToCCTS[i] = new ConcurrentHashMap<IndexSpecification, HTable>();
		}
		if(startCheckAdnRecovery&&this.shouldAddCheckerMaster())
		{
			CheckerMaster master = new CheckerMaster(conf,
				this.indexTableDescriptorMap, this);
			master.start();
		}
		RegionFlusher flusher = new RegionFlusher(this);
		flusher.start();
	}

	static int workerF = 0;

	public synchronized static int getIdleWorker() {
		workerF++;
		if (workerF >= workerN) {
			workerF = 0;
		}
		return workerF;

	}

	public static void addTask(IndexedRegion r, IndexSpecification indexSpec,
			byte[] row, SortedMap<byte[], byte[]> columnValues, Integer lockId,
			Result d) {

		idleWorker = getIdleWorker();
		synchronized (refineSet) {
			WorkSet set = new WorkSet(r, indexSpec, row, columnValues, lockId,
					d, sequence);
			taskQueue.get(idleWorker).add(set);
			refineSet.put(set, sequence);
			sequence++;
		}
	}

	static public class Worker extends Thread {
		long sleepTime[] = { 100, 100, 300, 300, 500, 1000, 3000 };
		java.util.concurrent.ConcurrentLinkedQueue<WorkSet> queue;
		int flag = 0;
		HRegionServer server;
		boolean free = false;

		public Worker(ConcurrentLinkedQueue queue, HRegionServer server) {
			this.server = server;
			this.queue = queue;
		}

		public void run() {
			while (!this.server.isStopped()) {
				try {

					if (!queue.isEmpty()) {
						//System.out.println("queue not empty");
						this.free = false;
						flag = 0;
						WorkSet set = queue.poll();
						synchronized (refineSet) {
							if (set.sequence != refineSet.get(set)) {
								LOG.info("Job queue cache hit, the row is obsolete "+set.getRow());
								System.out.println("cache hits");
								continue;
							} else {
								refineSet.remove(set);
							}
						}
						//System.out.println("get job");
						set.getR().updateCCIndex(
								set.getIndexSpec(), set.getRow(),
								set.getColumnValues(), set.d);

					} else {
						
						//System.out.println("queue is empty");
						try {
							this.free = true;
							if (flag >= this.sleepTime.length - 1) {

								flushTable();

								// System.out.println(this.getId()
								// + ":there is no jobs");
								sleep(this.sleepTime[flag]);
							} else {
								// flushTable();
								sleep(this.sleepTime[flag++]);
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				} catch (Exception e) {
					continue;
				}
			}
		}
	}

	static public class WorkSet implements Comparator<WorkSet>{

		int sequence = 0;

		Hash hash = Hash.getInstance(Hash.JENKINS_HASH);;
		byte[] ID;

		public boolean equals(Object arg0) {

			if (arg0 instanceof WorkSet) {
				WorkSet arg = (WorkSet) arg0;
				if (this.indexSpec.equals(arg.indexSpec)
						&& Bytes.equals(this.row, arg.row))
					return true;
				else
					return false;
			}
			return false;
		}

		public int hashCode() {
			if (this.ID == null)
				this.ID = Bytes.add(row, indexSpec.getIndexId());

			if (hash == null) {
				System.out.println("null hash");
			} else if (this.ID == null) {
				System.out.println("null.eslse");
			}
			return hash.hash(this.ID);
		}

		IndexedRegion r;
		IndexSpecification indexSpec;
		byte[] row;
		SortedMap<byte[], byte[]> columnValues;
		Integer lockId;

		WorkSet next = null;
		Result d = null;

		public WorkSet(IndexedRegion r, IndexSpecification indexSpec,
				byte[] row, SortedMap<byte[], byte[]> columnValues,
				Integer lockId, Result d, int seq) {
			this.r = r;
			this.indexSpec = indexSpec;
			this.row = row;
			this.columnValues = columnValues;
			this.lockId = lockId;
			this.d = d;
			this.sequence = seq;
			this.ID = Bytes.add(row, indexSpec.getCCITName());
		}

		public WorkSet() {
			// TODO Auto-generated constructor stub
		}

		public SortedMap<byte[], byte[]> getColumnValues() {
			return columnValues;
		}

		public void setColumnValues(SortedMap<byte[], byte[]> columnValues) {
			this.columnValues = columnValues;
		}

		public IndexSpecification getIndexSpec() {
			return indexSpec;
		}

		public void setIndexSpec(IndexSpecification indexSpec) {
			this.indexSpec = indexSpec;
		}

		public Integer getLockId() {
			return lockId;
		}

		public void setLockId(Integer lockId) {
			this.lockId = lockId;
		}

		public WorkSet getNext() {
			return next;
		}

		public void setNext(WorkSet next) {
			this.next = next;
		}

		public IndexedRegion getR() {
			return r;
		}

		public void setR(IndexedRegion r) {
			this.r = r;
		}

		public byte[] getRow() {
			return row;
		}

		public void setRow(byte[] row) {
			this.row = row;
		}

		@Override
		public int compare(WorkSet o1, WorkSet o2) {
			// TODO Auto-generated method stub
			return Bytes.compareTo(o1.ID, o2.ID);
		}

	}

}
