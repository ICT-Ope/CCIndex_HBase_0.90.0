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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Leases;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.ccindex.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ccindex.CCIndexConstants;
import org.apache.hadoop.hbase.client.ccindex.CCIndexDescriptor;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.CCIndexAdmin;

import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import org.apache.hadoop.hbase.regionserver.wal.HLog;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class IndexedRegion extends HRegion {

	private static final Log LOG = LogFactory.getLog(IndexedRegion.class);
	private final Configuration conf;
	private boolean baseRegion;
	private CCIndexDescriptor indexTableDescriptor;
	private static Map<IndexSpecification, HTable> indexSpecToTable[] = IndexedRegionServer.indexSpecToTables;
	private static Map<IndexSpecification, HTable> indexSpecToCCTS[] = IndexedRegionServer.indexSpecToCCTS;
	private static Map<String, HTable> tableToCCTS = IndexedRegionServer.tableToCCTS;
	private static HTable orgTable = null;
	private static HTable CCTBase = null;
	public static void flushTable()
	{
		try {
			if(orgTable!=null)
			orgTable.flushCommits();
			if(CCTBase!=null)
			CCTBase.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	public HTable getCCTBase() {
		if (CCTBase == null)
			try {
				CCTBase = new HTable(conf, this.indexTableDescriptor
						.getBaseCCTName());
				CCTBase.setWriteBufferSize(1000 * 1000 * 20);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return CCTBase;
	}
	public HTable getBase()
	{
		if (orgTable == null) {
			try {
				HTableDescriptor td = getTableDesc();
				orgTable = new HTable(conf, td.getName());
				orgTable.setWriteBufferSize(1000 * 1000 * 100);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return this.orgTable;
		
	}

	public void setCCTBase(HTable cCTBase) {
		CCTBase = cCTBase;
	}

	public static final byte[] NO_VAlUE_INDEX = Bytes.toBytes("_NO_VALUE_");

	public IndexedRegion(Path tableDir, HLog log, FileSystem fs,
			Configuration conf, HRegionInfo regionInfo,
			FlushRequester flushRequester) {

		super(tableDir, log, fs, conf, regionInfo, flushRequester);
		this.conf = super.getConf();
		HTableDescriptor des = regionInfo.getTableDesc();
		if (des.getValue(CCIndexConstants.BASE_KEY) == null) {
			baseRegion = false;
			return;
		} else {
			baseRegion = true;
			try {
				this.indexTableDescriptor = new CCIndexDescriptor(regionInfo
						.getTableDesc());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (IndexedRegionServer.indexTableDescriptorMap
					.get(this.indexTableDescriptor.getBaseTableDescriptor()
							.getNameAsString()) == null) {
				IndexedRegionServer.indexTableDescriptorMap.put(
						this.indexTableDescriptor.getBaseTableDescriptor()
								.getNameAsString(), this.indexTableDescriptor);
			}

			Collection<IndexSpecification> c = this.getIndexes();

		}
	}

	static int tflag = 0;
	static int cctflag = 0;

	public static synchronized ConcurrentHashMap<IndexSpecification, HTable> getTableMap() {
		tflag++;
		if (tflag >= IndexedRegionServer.jobQueue) {
			tflag = 0;
		}
		return (ConcurrentHashMap<IndexSpecification, HTable>) indexSpecToTable[tflag];
	}

	public static synchronized ConcurrentHashMap<IndexSpecification, HTable> getCCTMap() {
		cctflag++;
		if (cctflag >= IndexedRegionServer.jobQueue) {
			cctflag = 0;
		}
		return (ConcurrentHashMap<IndexSpecification, HTable>) indexSpecToCCTS[cctflag];
	}

	private synchronized HTable getCCT(IndexSpecification index)
			throws IOException {
		ConcurrentHashMap<IndexSpecification, HTable> tMap = this.getCCTMap();
		HTable indexTable = tMap.get(index);
		if (indexTable == null) {
			indexTable = new HTable(conf, index.getCCTName());
			tMap.put(index, indexTable);
		}
		indexTable.setWriteBufferSize(1000 * 1000 * 100);
		return indexTable;
	}

	private synchronized HTable getCCIT(IndexSpecification index)
			throws IOException {
		ConcurrentHashMap<IndexSpecification, HTable> tMap = this.getTableMap();
		HTable indexTable = tMap.get(index);
		if (indexTable == null) {
			indexTable = new HTable(conf, index.getCCITName());
			tMap.put(index, indexTable);

		}
		indexTable.setWriteBufferSize(1000 * 1000 * 100);
		return indexTable;
	}


	private Collection<IndexSpecification> getIndexes() {
		return indexTableDescriptor.getIndexes();
	}

	/**
	 * @param batchUpdate
	 * @param lockid
	 * @param writeToWAL
	 *            if true, then we write this update to the log
	 * @throws IOException
	 */
	@Override
	public OperationStatusCode[] put(Pair<Put, Integer>[] putsAndLocks)
			throws IOException {
		OperationStatusCode[] ret = null;
		if (!baseRegion) {
			return super.put(putsAndLocks);
		} else {
			ret = super.put(putsAndLocks);
			for (Pair<Put, Integer> p : putsAndLocks) {
				Put putCCTBase = this.getOldEntryAndAddTask(p.getFirst(), p
						.getSecond());
				if (putCCTBase != null) {
					this.getCCTBase().put(putCCTBase);
				}
			}

		}
		return ret;
	}

	/**
	 * @param put
	 * @param lockId
	 * @param writeToWAL
	 *            if true, then we write this update to the log
	 * @throws IOException
	 */
	@Override
	public void put(Put put, Integer lockId, boolean writeToWAL)
			throws IOException {
		if (!baseRegion) {
			super.put(put, lockId, writeToWAL);
			return;
		}
		super.put(put, lockId, writeToWAL);
		Put putCCT = this.getOldEntryAndAddTask(put, lockId);
		if (putCCT != null) {

			this.getCCTBase().put(putCCT);
		}
		
	}

	/**
	 * get old entry in base table and add index update task
	 * 
	 * @param put
	 * @param lockId
	 * @return the update for base table's CCT
	 */

	private Put getOldEntryAndAddTask(Put put, Integer lockId) {
		NavigableMap<byte[], byte[]> newColumnValues = getColumnsFromPut(put);

	
		Result d = null;
		try {
			d = this.getOldValueInBaseTable(this, put.getRow(),
					newColumnValues, lockId);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		SortedMap<byte[], byte[]> oldColumnValues = convertToValueMap(d);
		for(byte[] newColumn:newColumnValues.keySet())
		{
			oldColumnValues.put(newColumn, newColumnValues.get(newColumn));
		}
		for (IndexSpecification index : getIndexes()) {
			if (oldColumnValues.get(index.getIndexedColumn()) == null)
				oldColumnValues.put(index.getIndexedColumn(), CCIndexConstants.EMPYT_VALUE);
		}
		
		for (IndexSpecification index : getIndexes()) {
			IndexedRegionServer.addTask(this, index, put.getRow(),
					oldColumnValues,lockId,d);
		}
		return IndexMaintenanceUtils.createBaseCCTUpdate(put.getRow(),
				oldColumnValues, this.indexTableDescriptor);
	}

	
	private NavigableMap<byte[], byte[]> getColumnsFromPut(Put put) {
		NavigableMap<byte[], byte[]> columnValues = new TreeMap<byte[], byte[]>(
				Bytes.BYTES_COMPARATOR);
		for (List<KeyValue> familyPuts : put.getFamilyMap().values()) {
			for (KeyValue kv : familyPuts) {
				columnValues.put(kv.makeColumn(kv.getFamily(), kv
						.getQualifier()), kv.getValue());
			}
		}
		return columnValues;
	}

	public  Result getOldValueInBaseTable(IndexedRegion r, byte[] row,
			SortedMap<byte[], byte[]> columnValues, Integer lockId)
			throws IOException {
		Get oldGet = new Get(row);
		Result res = this.getBase().get(oldGet);
		return res;
	}


	/**
	 * test if result is null
	 * 
	 * @param oldResult
	 * @return
	 */
	private boolean resultNotNull(Result oldResult) {
		if (oldResult != null && oldResult.raw() != null
				&& oldResult.list() != null) {
			return true;
		}
		return false;
	}

	private void dealOldEntries(
			IndexSpecification indexSpec, byte[] row, Result oldResult) {
		if (this.resultNotNull(oldResult)) {
//			SortedMap<byte[], byte[]> oldColumnValues = this
//					.convertToValueMap(oldResult);
			
//			Get get = new Get(indexSpec.getKeyGenerator().createIndexKey(row,
//					oldColumnValues.get(indexSpec.getIndexedColumn())));
			
			Delete delete = new Delete(row);
			try {
				this.getBase().delete(delete);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//because IndexedRegion's delete will delete the old data in CCIT and CCT,we don't need do the delete job here.
//			try {
//				this.getCCIT(indexSpec).delete(delete);
//				this.getCCT(indexSpec).delete(delete);
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}


		}

	}

	private void dealNewEntries(
			SortedMap<byte[], byte[]> newEntries, IndexSpecification indexSpec,
			byte[] row) {

		Put putCCIT = IndexMaintenanceUtils.createCCITUpdate(indexSpec, row,
				newEntries);
		Put putCCT = IndexMaintenanceUtils.createCCTUpdate(indexSpec, row,
				newEntries);
		try {
			//System.out.println("put CCIT "+putCCIT);
			this.getCCIT(indexSpec).put(putCCIT);
			//System.out.println("put CCT "+putCCT);
			this.getCCT(indexSpec).put(putCCT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
/**
 * maintain CCIT and CCT
 * @param indexSpec the index which need to be update
 * @param row the base table row value
 * @param columnValues new values
 * @param oldResult old value in base table
 * @throws IOException
 */
	public void updateCCIndex(IndexSpecification indexSpec, byte[] row,
			SortedMap<byte[], byte[]> columnValues, Result oldResult)
			throws IOException {
			this.dealOldEntries(indexSpec,
				row, oldResult);
		this.dealNewEntries(columnValues, indexSpec, row);
	}




	@Override
	
	public void delete(Delete delete, final Integer lockid, boolean writeToWAL)
			throws IOException {
		if (!this.baseRegion) {
			super.delete(delete, lockid, writeToWAL);
			return;
		} // First remove the existing indexes.
		Result oldValue= this.getBase().get(new Get(delete.getRow()));
		super.delete(delete, lockid, writeToWAL);
		
		Delete d=IndexMaintenanceUtils.getBaseCCTDelete( delete, this.indexTableDescriptor);
		this.getCCTBase().delete(d);
		
		for(IndexSpecification spec:this.indexTableDescriptor.getIndexes())
		{
			d=IndexMaintenanceUtils.getCCITDelete(oldValue, delete, spec);
			this.getCCIT(spec).delete(d);
			d=IndexMaintenanceUtils.getCCTDelete(oldValue, delete, spec);
			this.getCCT(spec).delete(d);
		}
	}

	public static SortedMap<byte[], byte[]> convertToValueMap(Result result) {
		SortedMap<byte[], byte[]> currentColumnValues = new TreeMap<byte[], byte[]>(
				Bytes.BYTES_COMPARATOR);

		if (result == null || result.raw() == null) {
			return currentColumnValues;
		}
		List<KeyValue> list = result.list();
		if (list != null) {
			for (KeyValue kv : result.list()) {
				currentColumnValues.put(kv.makeColumn(kv.getFamily(), kv
						.getQualifier()), kv.getValue());
			}
		}
		return currentColumnValues;
	}

}
