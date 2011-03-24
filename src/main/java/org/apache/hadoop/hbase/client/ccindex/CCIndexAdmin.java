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
package org.apache.hadoop.hbase.client.ccindex;

import java.io.IOException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ColumnNameParseException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.regionserver.ccindex.IndexMaintenanceUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Extension of HBaseAdmin that creates,updates,adds and removes CCIndex.
 * 
* @author liujia
* @version 90.0
 */
public class CCIndexAdmin extends HBaseAdmin {

	private static final Log LOG = LogFactory.getLog(CCIndexAdmin.class);

	/**
	 * Constructor
	 * 
	 * @param conf
	 *            Configuration object
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException 
	 */
	public CCIndexAdmin(Configuration conf)
			throws MasterNotRunningException, ZooKeeperConnectionException {
		super(conf);
	}

	/**
	 * Creates a new indexed table
	 * 
	 * @param desc
	 *            table descriptor for table
	 * 
	 * @throws IOException
	 */
	public void createIndexedTable(CCIndexDescriptor desc) throws IOException {
		super.createTable(desc.getBaseTableDescriptor());
		this.createIndexTables(desc);
	}
	private void updateInfoCCT(CCIndexDescriptor indexDesc)
	{
		HTableDescriptor baseTableDes=indexDesc.getBaseTableDescriptor();
		byte[] baseTableName = indexDesc.getBaseTableDescriptor().getName();
		boolean createOrg = false;
		for (IndexSpecification indexSpec : indexDesc.getIndexes()) {
			try {
				if (!createOrg) {
					HTableDescriptor indexTableDescO = createCCTDesc(baseTableDes,
							indexSpec, true);
					this.disableTable(indexTableDescO.getName());
					super.modifyTable(indexTableDescO.getName(),indexTableDescO );
					super.enableTable(indexTableDescO.getName());
					createOrg = true;
				}
				
			
				HTableDescriptor indexTableDesc = createCCTDesc(baseTableDes,
						indexSpec, false);
				this.disableTable(indexTableDesc.getName());
				super.modifyTable(indexTableDesc.getName(),indexTableDesc );
				super.enableTable(indexTableDesc.getName());
			} catch (ColumnNameParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private void createIndexTables(CCIndexDescriptor indexDesc)
			throws IOException {
		HTableDescriptor baseTableDes=indexDesc.getBaseTableDescriptor();
		byte[] baseTableName = indexDesc.getBaseTableDescriptor().getName();
		for (IndexSpecification indexSpec : indexDesc.getIndexes()) {
			HTableDescriptor indexTableDesc = createCCITDesc(baseTableDes,
					indexSpec);
			LOG.info("create table :"+indexTableDesc);
			super.createTable(indexTableDesc);
		}
		boolean createOrg = false;
		for (IndexSpecification indexSpec : indexDesc.getIndexes()) {
			if (!createOrg) {
				HTableDescriptor indexTableDescO = createCCTDesc(baseTableDes,
						indexSpec, true);
				LOG.info("create table :"+indexTableDescO);
				super.createTable(indexTableDescO);
				createOrg = true;
			}
			HTableDescriptor indexTableDesc = createCCTDesc(baseTableDes,
					indexSpec, false);
			LOG.info("create table :"+indexTableDesc);
			super.createTable(indexTableDesc);
		}

	}
	/**
	 * create the desc for CCIT
	 * @param baseTableDesc 
	 * @param indexSpec
	 * @return
	 * @throws ColumnNameParseException
	 */
	private HTableDescriptor createCCITDesc(HTableDescriptor baseTableDesc,
			IndexSpecification indexSpec) throws ColumnNameParseException {
		
		byte []baseTableName=baseTableDesc.getName();
		HTableDescriptor indexTableDesc = new HTableDescriptor(indexSpec
				.getCCITName());
		Set<byte[]> families = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		families.add(CCIndexConstants.INDEX_COL_FAMILY_NAME);
		for (byte[] column : indexSpec.getAllColumns()) {
			byte[][]fq=KeyValue.parseColumn(column);
			indexTableDesc.addFamily(new HColumnDescriptor(fq[0]));
		}

		for (byte[] colFamily : families) {
			indexTableDesc.addFamily(new HColumnDescriptor(colFamily));
		}
		byte[] info = Bytes.add(baseTableName,
				CCIndexConstants.ORGTABLE_COLUMN_DELIMITER, indexSpec
						.getIndexedColumn());
		indexTableDesc.setValue(CCIndexConstants.CCIT_KEY, info);
		indexTableDesc.setValue(CCIndexConstants.INDEXES_KEY, baseTableDesc.getValue(CCIndexConstants.INDEXES_KEY));
		
		return indexTableDesc;
	}

	private HTableDescriptor createCCTDesc(HTableDescriptor baseTableDesc,
			IndexSpecification indexSpec, boolean org)
			throws ColumnNameParseException {
		byte []baseTableName=baseTableDesc.getName();
		HTableDescriptor indexTableDesc=org?new HTableDescriptor(indexSpec
				.getCCTNameBase(baseTableDesc.getName())):new HTableDescriptor(indexSpec
						.getCCTName());
	
		Set<byte[]> families = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		families.add(CCIndexConstants.INDEX_COL_FAMILY_NAME);
		for (byte[] column : indexSpec.getIndexedColumns()) {
			byte[][] fq=KeyValue.parseColumn(column);
			families.add(fq[0]);
		}
		for (byte[] colFamily : families) {
			//System.out.println(new String(colFamily));
			indexTableDesc.addFamily(new HColumnDescriptor(colFamily));
		}
		byte[] info = Bytes.add(baseTableName, new byte[] {});
		for (byte[] columns : indexSpec.getIndexedColumns()) {
			info = Bytes.add(info, CCIndexConstants.ORGTABLE_COLUMN_DELIMITER,
					columns);
		}
		indexTableDesc.setValue(CCIndexConstants.CCT_KEY, info);
		indexTableDesc.setValue(CCIndexConstants.INDEXES_KEY, baseTableDesc.getValue(CCIndexConstants.INDEXES_KEY));
		//indexTableDesc.setValue(key, value)
		return indexTableDesc;
	}

	/**
	 * Remove an index for a table.
	 * 
	 * @throws IOException
	 * 
	 */
	public void removeIndex(byte[] baseTableName, byte[] indexId)
			throws IOException {
		super.disableTable(baseTableName);
		HTableDescriptor desc = super.getTableDescriptor(baseTableName);
		
		
		CCIndexDescriptor indexDesc = new CCIndexDescriptor(desc);
		IndexSpecification spec = indexDesc.getIndex(indexId);
		indexDesc.removeIndex(indexId);
		this.disableTable(spec.getCCITName());
		this.deleteTable(spec.getCCITName());
		this.disableTable(spec.getCCTName());
		this.deleteTable(spec.getCCTName());
		super.modifyTable(baseTableName, desc);
		super.enableTable(baseTableName);
		this.updateInfoCCT(indexDesc);
	}

	/** Add an index to a table. */
	public void addIndex(byte[] baseTableName, IndexSpecification indexSpec)
			throws IOException {
		LOG.warn("Adding index to existing table ["
				+ Bytes.toString(baseTableName)
				+ "], this may take a long time");
		// TODO, make table read-only
		LOG
				.warn("Not putting table in readonly, if its being written to, the index may get out of sync");
		HTable baseTable=new HTable(baseTableName);
		
		HTableDescriptor baseDesc=baseTable.getTableDescriptor();
		HTableDescriptor CCITDesc = createCCITDesc(baseDesc, indexSpec);
		HTableDescriptor CCTDesc = createCCTDesc(baseDesc, indexSpec,false);
		super.createTable(CCITDesc);
		super.createTable(CCTDesc);
		super.disableTable(baseTableName);
		CCIndexDescriptor indexDesc = new CCIndexDescriptor(super
				.getTableDescriptor(baseTableName));
		indexDesc.addIndex(indexSpec);
		super.modifyTable(baseTableName, indexDesc.getBaseTableDescriptor());
		super.enableTable(baseTableName);
		/* now is empty */
		reIndexTable(baseTableName, indexSpec);
	}

	private void reIndexTable(byte[] baseTableName, IndexSpecification indexSpec)
			throws IOException {
		// HTable baseTable = new HTable(baseTableName);
		// HTable indexTable = new
		// HTable(indexSpec.getIndexedTableName(baseTableName));
		// for (RowResult rowResult :
		// baseTable.getScanner(indexSpec.getAllColumns())) {
		// SortedMap<byte[], byte[]> columnValues = new TreeMap<byte[],
		// byte[]>(Bytes.BYTES_COMPARATOR);
		// for (Entry<byte[], Cell> entry : rowResult.entrySet()) {
		// columnValues.put(entry.getKey(), entry.getValue().getValue());
		// }
		// if (IndexMaintenanceUtils.doesApplyToIndex(indexSpec, columnValues))
		// {
		// Put indexUpdate = IndexMaintenanceUtils.createIndexUpdate(indexSpec,
		// rowResult.getRow(), columnValues);
		// indexTable.put(indexUpdate);
		// }
		// }
	}
}
