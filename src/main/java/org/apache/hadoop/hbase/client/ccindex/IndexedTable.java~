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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/** HTable extended with indexed support. */
public class IndexedTable extends HTable {
	static final Log LOG = LogFactory.getLog(IndexedTable.class);
	private static SimpleOptimizer refiner;
	private final CCIndexDescriptor indexedTableDescriptor;
	private TreeMap<byte[], byte[]> columnToIndexID = new TreeMap<byte[], byte[]>(
			Bytes.BYTES_COMPARATOR);
	private Map<byte[], HTable> indexIdToTable = new TreeMap<byte[], HTable>(
			Bytes.BYTES_COMPARATOR);

	private byte[][] makeAllColumns(Range[] range, int flag) {
		byte[][] base = new byte[range.length
				+ range[flag].getBaseColumns().length][];
		int i = 0;
		for (Range r : range) {
			base[i++] = r.getColumn();
		}
		int j = 0;
		for (byte[] b : range[flag].getBaseColumns())
			base[i++] = range[flag].getBaseColumns()[j++];
		return base;
	}

	public IndexedTable(final HBaseConfiguration conf,
			final byte[] baseTableName) throws IOException {
		super(conf, baseTableName);
		if (refiner == null) {
			refiner = new SimpleOptimizer(conf, this);
			refiner.refresh();
		}
		this.indexedTableDescriptor = new CCIndexDescriptor(super
				.getTableDescriptor());
		for (IndexSpecification spec : this.indexedTableDescriptor.getIndexes()) {
			{
				indexIdToTable.put(spec.getIndexId(), new HTable(conf, spec
						.getCCITName()));
			}
			if (baseTableName != null) {
				indexIdToTable.put(CCIndexConstants.indexID_Base, new HTable(
						conf, baseTableName));
			}
		}
		if (columnToIndexID.size() == 0)
			this.initColumnIndexMap();
	}

	public TreeMap<byte[], byte[]> getColumnToIndexID() {
		return columnToIndexID;
	}

	public void setColumnToIndexID(TreeMap<byte[], byte[]> columnToIndexID) {
		this.columnToIndexID = columnToIndexID;
	}

	public Map<byte[], HTable> getIndexIdToTable() {
		return indexIdToTable;
	}

	public void setIndexIdToTable(Map<byte[], HTable> indexIdToTable) {
		this.indexIdToTable = indexIdToTable;
	}

	public CCIndexDescriptor getCCIndexDescriptor() {
		return this.indexedTableDescriptor;
	}

	public void initColumnIndexMap() {
		Collection<IndexSpecification> col = this.indexedTableDescriptor
				.getIndexes();
		for (IndexSpecification index : col) {
			byte[] column = index.getIndexedColumn();
			this.columnToIndexID.put(column, index.getIndexId());
		}
	}

	/**
	 * if provided queries like A or B, one SingleReader will process A and
	 * another SingleReader will process B they work parallel.
	 * 
	 * @param range
	 *            the restrictions 
	 * @return
	 */
	private SingleReader getSingleReader(Range[] range) {
		if (this.columnToIndexID.keySet().size() == 0) {
			this.initColumnIndexMap();
		}
		int flag = refiner.whichToScan(range);
		byte[] indexId = this.columnToIndexID.get(range[flag].getColumn());
		Range[] newRange = new Range[range.length - 1];
		for (int i = 0, j = 0; i < range.length; i++) {
			if (i != flag) {
				newRange[j++] = range[i];
			}
		}
		try {
			ResultScanner scanner = this.getIndexedScanner(indexId, range[flag]
					.getStart(), range[flag].getEnd(),
					new byte[][] { range[flag].getColumn() }, this
							.makeAllColumns(range, flag));
			return new SingleReader(scanner, newRange);
		} catch (IndexNotFoundException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getMessage());
		}
		return null;

	}

	/**
	 * get a result reader for Multi-Dimensional Range Query
	 * 
	 * @param range 
	 * 								the restrictions of the query
	 * @param limit 
	 * 								number of records user want to get
	 * @return ResultReader
	 */
	public ResultReader MDRQScan(Range[][] range, long limit) {
		Vector<SingleReader> records = new Vector<SingleReader>();
		String query = "";
		// to get a text to print the query in log
		try {
			int i = 0, j = 0;
			for (Range[] r : range) {
				j = 0;
				for (Range ran : r) {
					if (ran.getStart() != null) {
						query += "(" + new String(ran.getColumn()) + ">"
								+ new String(ran.getStart()) + ")";
					}
					if (ran.getEnd() != null) {
						query += "(" + new String(ran.getColumn()) + "<"
								+ new String(ran.getEnd()) + ")";
					}
					if (j != r.length - 1)
						query += "&&";
					j++;
				}
				if (i != range.length - 1)
					query += "||";
				i++;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		LOG.info("The input query is: " + query);
		for (Range[] r : range) {
			SingleReader reader = this.getSingleReader(r);
			if (reader != null)
				records.add(reader);
		}
		return new ResultReader(records, limit);
	}

	public ResultScanner getIndexedScanner(byte[] indexId,
			final byte[] indexStartRow, final byte[] indexStopRow,
			byte[][] indexColumns, final byte[][] baseColumns)
			throws IndexNotFoundException, IOException {

		byte[] start = null;
		byte[] end = null;
		if (indexStartRow != null) {
			start = indexStartRow;
		}
		if (indexStopRow != null) {
			end = Bytes.add(indexStopRow, CCIndexConstants.maxRowKey);
		}
		return getIndexedScanner(indexId, start, end, indexColumns, null,
				baseColumns);
	}

	/**
	 * Open up an indexed scanner. Results will come back in the indexed order,
	 * but will contain RowResults from the original table.
	 * 
	 * @param indexId
	 *            the id of the index to use
	 * @param indexStartRow
	 *            (created from the IndexKeyGenerator)
	 * @param indexStopRow
	 *            (created from the IndexKeyGenerator)
	 * @param indexColumns
	 *            in the index table
	 * @param indexFilter
	 *            filter to run on the index'ed table. This can only use columns
	 *            that have been added to the index.
	 * @param baseColumns
	 *            from the original table
	 * @return scanner
	 * @throws IOException
	 * @throws IndexNotFoundException
	 * @throws IOException
	 * @throws IndexNotFoundException
	 */
	public ResultScanner getIndexedScanner(byte[] indexId,
			final byte[] indexStartRow, final byte[] indexStopRow,
			byte[][] indexColumns, final Filter indexFilter,
			final byte[][] baseColumns) throws IOException,
			IndexNotFoundException {
		IndexSpecification indexSpec = this.indexedTableDescriptor
				.getIndex(indexId);
		if (indexSpec == null) {
			throw new IndexNotFoundException("Index " + indexId
					+ " not defined in table "
					+ super.getTableDescriptor().getNameAsString());
		}
		verifyIndexColumns(indexColumns, indexSpec);
		HTable indexTable = indexIdToTable.get(indexId);
		Scan indexScan = new Scan();
		indexScan.setFilter(indexFilter);
		indexScan.addColumns(baseColumns);
		indexScan.addColumns(indexColumns);
		indexScan.setCaching(1000);
		if (indexStartRow != null) {
			indexScan.setStartRow(indexStartRow);
		}
		if (indexStopRow != null) {
			indexScan.setStopRow(indexStopRow);
		}
		ResultScanner indexScanner = indexTable.getScanner(indexScan);
		//System.out.println("the scan is:" + indexScan);
		return new ScannerWrapper(indexScanner, baseColumns);
	}

	private void verifyIndexColumns(byte[][] requestedColumns,
			IndexSpecification indexSpec) {
		if (requestedColumns == null) {
			return;
		}
		for (byte[] requestedColumn : requestedColumns) {
			boolean found = false;
			for (byte[] indexColumn : indexSpec.getAllColumns()) {
				if (Bytes.equals(requestedColumn, indexColumn)) {
					found = true;
					break;
				}
			}
			if (!found) {
				throw new RuntimeException("Column ["
						+ Bytes.toString(requestedColumn) + "] not in index "
						+ indexSpec.getIndexId());
			}
		}
	}

	public class ScannerWrapper implements ResultScanner {

		private ResultScanner indexScanner;
		private byte[][] columns;
		private TreeMap<byte[], Boolean> col;
		public ScannerWrapper(ResultScanner indexScanner, byte[][] columns) {
			this.indexScanner = indexScanner;
			this.columns = columns;
			this.col = this.getColumnMap(columns);
		}

		/** {@inheritDoc} */
		public Result next() throws IOException {
			Result[] result = next(1);
			if (result == null || result.length < 1)
				return null;
			return result[0];
		}

		public TreeMap<byte[], Boolean> getColumnMap(byte[][] col) {
			if (col == null) {
				return null;
			} else {
				TreeMap<byte[], Boolean> ret = new TreeMap<byte[], Boolean>(
						Bytes.BYTES_COMPARATOR);
				for (byte[] b : col) {
					ret.put(b, true);
				}
				return ret;
			}
		}
		public Result[] next(int nbRows) throws IOException {
			Result[] indexResult = indexScanner.next(nbRows);

			if (indexResult == null) {
				return null;
			} else if (indexResult != null) {
				return indexResult;
			}

			// System.out.println(indexResult.length);
			Result[] result = new Result[indexResult.length];
			for (int i = 0; i < indexResult.length; i++) {
				Result row = indexResult[i];

				byte[] baseRow = row.getValue(
						CCIndexConstants.INDEX_COL_FAMILY,
						CCIndexConstants.INDEX_BASE_ROW);
				if (baseRow == null) {
					System.out.println("error");
					return null;

				}

				Result baseResult = null;

				/*
				 * if (columns != null && columns.length > 0) {
				 * LOG.debug("Going to base table for remaining columns"); Get
				 * baseGet = new Get(baseRow); baseGet.addColumns(columns);
				 * baseResult = IndexedTable.this.get(baseGet); }
				 */

				List<KeyValue> results = new ArrayList<KeyValue>();
				for (KeyValue indexKV : row.list()) {
					if (indexKV
							.matchingFamily(CCIndexConstants.INDEX_COL_FAMILY_NAME)) {
						continue;
					}

					results.add(new KeyValue(baseRow, indexKV.getFamily(),
							indexKV.getQualifier(), indexKV.getTimestamp(),
							KeyValue.Type.Put, indexKV.getValue()));
				}

				if (baseResult != null) {
					results.addAll(baseResult.list());
				}

				result[i] = new Result(results);
			}
			return result;
		}

		/** {@inheritDoc} */
		public void close() {
			indexScanner.close();
		}

		// Copied from HTable.ClientScanner.iterator()
		public Iterator<Result> iterator() {
			return new Iterator<Result>() {
				// The next RowResult, possibly pre-read
				Result next = null;

				// return true if there is another item pending, false if there
				// isn't.
				// this method is where the actual advancing takes place, but
				// you need
				// to call next() to consume it. hasNext() will only advance if
				// there
				// isn't a pending next().
				public boolean hasNext() {
					if (next == null) {
						try {
							next = ScannerWrapper.this.next();
							return next != null;
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
					return true;
				}

				// get the pending next item and advance the iterator. returns
				// null if
				// there is no next item.
				public Result next() {
					// since hasNext() does the real advancing, we call this to
					// determine
					// if there is a next before proceeding.
					if (!hasNext()) {
						return null;
					}

					// if we get to here, then hasNext() has given us an item to
					// return.
					// we want to return the item and then null out the next
					// pointer, so
					// we use a temporary variable.
					Result temp = next;
					next = null;
					return temp;
				}

				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		}

	}
}