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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
/**
 * 
 * CCIndexDescriptor holds the base information of a CCIndex.
 *  a CCIndex includes many IndexSpecifications,
 *  on IndexSpecificaton holds the information of CCIT and corresponding CCT.
 *
 */
public class CCIndexDescriptor {

	private final HTableDescriptor baseTableDescriptor;
	private final Map<byte[], IndexSpecification> indexes = new TreeMap<byte[], IndexSpecification>(
			Bytes.BYTES_COMPARATOR);
	private byte indexedColumns[][];
	private Set<byte[]> indexedColumnsSet;
	private Set<byte[]> indexedFamiliesSet;
	public Set<byte[]> getIndexedColumnsSet() {
		return indexedColumnsSet;
	}
	public void setIndexedColumnsSet(Set<byte[]> indexedColumnsSet) {
		this.indexedColumnsSet = indexedColumnsSet;
	}
	public Set<byte[]> getIndexedFamiliesSet() {
		return indexedFamiliesSet;
	}
	public void setIndexedFamiliesSet(Set<byte[]> indexedFamiliesSet) {
		this.indexedFamiliesSet = indexedFamiliesSet;
	}
	public byte[][] getIndexedColumns() {
		return indexedColumns;
	}
	public byte[] getBaseCCTName()
	{
		return Bytes.add(this.baseTableDescriptor.getName(),Bytes.toBytes("-"),Bytes.toBytes( CCIndexConstants.CCT_TAIL));
	}
	public void setIndexedColumns(byte[][] indexedColumns) {
		this.indexedColumns = indexedColumns;
		this.initSet();
		try {
			this.writeToTable();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public CCIndexDescriptor(HTableDescriptor baseTableDescriptor)
			throws IOException {
		this.baseTableDescriptor = baseTableDescriptor;
		readFromTable();
	}

	public HTableDescriptor getBaseTableDescriptor() {
		return this.baseTableDescriptor;
	}

	private void readFromTable() throws IOException {

		byte[] bytes = baseTableDescriptor
				.getValue(CCIndexConstants.INDEXES_KEY);
		if (bytes == null) {
			return;
		}
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bais);
		int size = dis.readInt();
		this.indexedColumns = new byte[size][];
		
		for (int i = 0; i < size; i++) {
			indexedColumns[i] = Bytes.readByteArray(dis);
		}
		this.initSet();
		IndexSpecificationArray indexArray = new IndexSpecificationArray(indexedColumns);
		indexArray.readFields(dis);
		for (Writable index : indexArray.getIndexSpecifications()) {
			IndexSpecification indexSpec = (IndexSpecification) index;
			indexSpec.setCCIndex(this);
			indexes.put(indexSpec.getIndexId(), indexSpec);
		}
	}
	private void initSet()
	{
		this.indexedColumnsSet=new TreeSet(Bytes.BYTES_COMPARATOR);
		this.indexedFamiliesSet=new TreeSet(Bytes.BYTES_COMPARATOR);
		for(byte[] columns:this.indexedColumns)
		{
			byte[][]fq=KeyValue.parseColumn(columns);
			this.indexedColumnsSet.add(columns);
			this.indexedFamiliesSet.add(fq[0]);
		}
	}

	private void writeToTable() throws Exception {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		DataOutputStream dos = new DataOutputStream(baos);
		if (this.indexedColumns == null) {
			dos.writeInt(0);
		} else {

			dos.writeInt(this.indexedColumns.length);
			for (int i = 0; i < this.indexedColumns.length; i++) {
				Bytes.writeByteArray(dos, this.indexedColumns[i]);
			}
		}
		for(IndexSpecification index:this.indexes.values())
		{
			index.setIndexedColumns(indexedColumns);
		}
		
		IndexSpecificationArray indexArray = new IndexSpecificationArray(
				indexes.values().toArray(new IndexSpecification[0]));

		try {
			indexArray.write(dos);
			dos.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		baseTableDescriptor.setValue(CCIndexConstants.INDEXES_KEY, baos
				.toByteArray());
		baseTableDescriptor.setValue(CCIndexConstants.BASE_KEY,
				CCIndexConstants.BASE_KEY);

	}

	public Collection<IndexSpecification> getIndexes() {
		return indexes.values();
	}

	public IndexSpecification getIndex(byte[] indexId) {
		return indexes.get(indexId);
	}

	public void addIndex(IndexSpecification index) {
		indexes.put(index.getIndexId(), index);
		try {
			writeToTable();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void removeIndex(byte[] indexId) {
		indexes.remove(indexId);
		try {
			writeToTable();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder(baseTableDescriptor.toString());

		if (!indexes.isEmpty()) {
			s.append(", ");
			s.append("INDEXES");
			s.append(" => ");
			s.append(indexes.values());
		}
		return s.toString();
	}
}
