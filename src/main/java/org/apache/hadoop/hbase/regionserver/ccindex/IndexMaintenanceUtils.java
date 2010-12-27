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

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ColumnNameParseException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ccindex.CCIndexConstants;
import org.apache.hadoop.hbase.client.ccindex.CCIndexDescriptor;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.IndexedTable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Singleton class for index maintence logic.
 */
public class IndexMaintenanceUtils {

	private static final Log LOG = LogFactory
			.getLog(IndexMaintenanceUtils.class);

	public static Put createIndexUpdateMultTable(
			final IndexSpecification indexSpec, final byte[] row,
			final SortedMap<byte[], byte[]> columnValues, byte[] indexedColumn) {
		for (byte[] col : indexSpec.getIndexedColumns()) {
			if (columnValues.get(col) != null) {

			}
		}

		return null;
	}

	public static Put addColumnValue(final IndexSpecification indexSpec,
			final SortedMap<byte[], byte[]> columnValues, Put put) {
		for (byte[] col : indexSpec.getAdditionalColumns()) {
			byte[] val = columnValues.get(col);
			if (val != null) {
				byte[][] colSeperated = KeyValue.parseColumn(col);
				put.add(colSeperated[0], colSeperated[1], val);
			}
		}
		return put;
	}



	public static Put createCCITUpdate(final IndexSpecification indexSpec,
			final byte[] row, final SortedMap<byte[], byte[]> columnValues) {
		byte[] indexRow = indexSpec.getKeyGenerator().createIndexKey(row,
				columnValues.get(indexSpec.getIndexedColumn()));
		Put update = new Put(indexRow);

		// update.add(IndexedTable.INDEX_COL_FAMILY_NAME,
		// IndexedTable.INDEX_BASE_ROW, row);

		for (byte[] col : indexSpec.getIndexedColumns()) {
			byte[] val = columnValues.get(col);
			if (val == null) {
				throw new RuntimeException("Unexpected missing column value. ["
						+ Bytes.toString(col) + "]");
			}
			byte[][] colSeperated = KeyValue.parseColumn(col);
			update.add(colSeperated[0], colSeperated[1], val);
		}

		for (byte[] col : indexSpec.getAdditionalColumns()) {
			byte[] val = columnValues.get(col);
			if (val != null) {
				byte[][] colSeperated = KeyValue.parseColumn(col);
				update.add(colSeperated[0], colSeperated[1], val);
			}
		}

		return update;
	}

	public static Put createBaseCCTUpdate(final byte[] row,
			final SortedMap<byte[], byte[]> columnValues,
			final CCIndexDescriptor des) {
		Put update = new Put(row);
		for (byte[] col : des.getIndexedColumns()) {
			byte[] val = columnValues.get(col);
			if (val != null) {
				byte[][] colSeperated = KeyValue.parseColumn(col);
				update.add(colSeperated[0], colSeperated[1], val);
			}
//			if (val != null&&!Bytes.equals(val, CCIndexConstants.EMPYT_ROW)) {
//				byte[][] colSeperated = KeyValue.parseColumn(col);
//				update.add(colSeperated[0], colSeperated[1], val);
//			}
		}
		return update;
	}

	public static Put createCCTUpdate(final IndexSpecification indexSpec,
			final byte[] row, final SortedMap<byte[], byte[]> columnValues) {
		byte[] indexRow = indexSpec.getKeyGenerator().createIndexKey(row,
				columnValues.get(indexSpec.getIndexedColumn()));
		Put update = new Put(indexRow);
		for (byte[] col : indexSpec.getIndexedColumns()) {
			byte[] val = columnValues.get(col);
			if (val != null&&!Bytes.equals(val, CCIndexConstants.EMPYT_ROW)) {
				byte[][] colSeperated = KeyValue.parseColumn(col);
				update.add(colSeperated[0], colSeperated[1], val);
			}
		}
		return update;
	}
	public static Delete getBaseCCTDelete(Delete delete,CCIndexDescriptor desc)
	{
		byte[] row=delete.getRow();
		if(delete.getFamilyMap().size()==0)
		{
			return new Delete(row);
		}
		Delete ret=new Delete(row);
		Map<byte[],List<KeyValue>> familyMap=delete.getFamilyMap();
		for(byte[] family:familyMap.keySet())
		{
			for(KeyValue kv:familyMap.get(family))
			{
				if(desc.getIndexedFamiliesSet().contains(kv.getFamily()))
				{
					ret.deleteColumns(kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
				}
			}
		}
		return ret;
	}
	public static Delete getCCITDelete(Result oldValue,Delete delete,IndexSpecification spec)
	{
		
		byte[] row=delete.getRow();
		byte[] columnV=oldValue.getValue(spec.getFamily(), spec.getColumn())==null?
				CCIndexConstants.EMPYT_ROW:oldValue.getValue(spec.getFamily(), spec.getColumn());
		byte[] newRow=spec.getKeyGenerator().createIndexKey(row, columnV);
		Delete ret=new Delete(newRow);
		Map<byte[],List<KeyValue>> familyMap=delete.getFamilyMap();
		for(byte[] family:familyMap.keySet())
		{
			for(KeyValue kv:familyMap.get(family))
			{
					ret.deleteColumns(kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
			}
		}
		return ret;
	}
	public static Delete getCCTDelete(Result oldValue,Delete delete,IndexSpecification spec)
	{
		byte[] row=delete.getRow();
		byte[] columnV=oldValue.getValue(spec.getFamily(), spec.getColumn())==null?
				CCIndexConstants.EMPYT_ROW:oldValue.getValue(spec.getFamily(), spec.getColumn());
		byte[] newRow=spec.getKeyGenerator().createIndexKey(row, columnV);
		Delete ret=new Delete(newRow);
		Map<byte[],List<KeyValue>> familyMap=delete.getFamilyMap();
		for(byte[] family:familyMap.keySet())
		{
			for(KeyValue kv:familyMap.get(family))
			{
				if(Bytes.equals(spec.getFamily(),kv.getFamily()))
				{
					
					ret.deleteColumns(kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
				}
			}
		}
		return ret;
	}
	public static Put createOrgUpdate(final byte[] row,
			final SortedMap<byte[], byte[]> columnValues) {
		byte[] indexRow = row;
		Put update = null;
		update = new Put(indexRow);
		for (byte[] col : columnValues.keySet()) {
			try {
				byte[] val = columnValues.get(col);
				byte[][] colSeperated = KeyValue.parseColumn(col);
				update.add(colSeperated[0], colSeperated[1], val);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				continue;
			}
		}

		return update;
	}

	/**
	 * Ask if this update does apply to the index.
	 * 
	 * @param indexSpec
	 * @param columnValues
	 * @return true if possibly apply.
	 */
	public static boolean doesApplyToIndex(final IndexSpecification indexSpec,
			final SortedMap<byte[], byte[]> columnValues) {

		for (byte[] neededCol : indexSpec.getIndexedColumns()) {
			if (!columnValues.containsKey(neededCol)) {
				LOG.debug("Index [" + indexSpec.getIndexId()
						+ "] can't be updated because ["
						+ Bytes.toString(neededCol) + "] is missing");
				return false;
			}
		}
		return true;
	}
}
