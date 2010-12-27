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
import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;


public class SimpleOptimizer extends Thread implements Optimizer {
	static final Log LOG = LogFactory.getLog(SimpleOptimizer.class);
	private final HBaseConfiguration conf;
	private static HashMap<String, Vector<HRegionInfo>> tableToRegion = new HashMap<String, Vector<HRegionInfo>>();
	IndexedTable father;
	private long inter = 60000;
	
	public int whichToScan(Range[] ranges) {
		long[][] regionSize = new long[ranges.length][];
		for (int i = 0; i < ranges.length; i++) {
			regionSize[i] = this.getEstimatedRegionDisAndNumDis(ranges[i]);
			//System.out.println(ranges[i]);
		
		}
		int min = this.getMin(regionSize);
		LOG.info("mini range is:" + min);
		return min;
	}

	private long disCompare(long[] dis1, long[] dis2) {
		for (int i = 0; i < dis1.length; i++) {
			if (dis1[i] != dis2[i])
				return (dis1[i] - dis2[i]);
		}
		return 0;
	}

	private int getMin(long[][] dis) {
		long[] tmp = new long[] { Long.MAX_VALUE, Long.MAX_VALUE,
				Long.MAX_VALUE };
		int min = 0;
		for (int i = 0; i < dis.length; i++) {
			if (this.disCompare(dis[i], tmp) < 0L) {
				tmp = dis[i];
				min = i;
			}
		}
		return min;
	}

	public long[] getRegionDis(Vector<HRegionInfo> v) {

		if (v.size() == 0) {
			return new long[] { 0, 0 };
		}
		long indexDis = 0;
		long numDis = 0;
		long completeRegion = 0;
		for (HRegionInfo info : v) {
			if (info.getStartKey().length != 0 && info.getEndKey().length != 0) {

				completeRegion++;
				long dis[] = Utilities.disRowKey(info.getStartKey(), info
						.getEndKey());
				indexDis += dis[0];
				numDis += dis[1];
			}
		}
		if (completeRegion == 0) {
			if (v.get(0).getStartKey().length != 0) {
				return Utilities.disRowKey(v.get(0).getStartKey(), null);
			} else {
				return new long[] { 0, 0 };
			}

		}
		return new long[] {
				(long) Math.ceil(((float) indexDis / completeRegion)),
				(long) Math.ceil(((float) numDis / completeRegion)) };
	}
	public long[] getEstimatedRegionDisAndNumDis(Range r) {
		byte[] startKey = null;
		byte[] endKey = null;
		if (r.getStart() != null)
			startKey = r.getStart();
		if (r.getEnd() != null) {
			endKey = r.getEnd();
		}
		byte[] index = this.father.getColumnToIndexID().get(
				r.getColumn());
		HTable t = this.father.getIndexIdToTable().get(index);
		String tableName = new String(t.getTableName());
		if (this.tableToRegion.size() == 0) {
			this.refresh();
		}
		Vector<HRegionInfo> infos = this.tableToRegion.get(tableName);
		if (infos == null)
			return new long[] { 0, 0, 0 };
		int startRegion = 0;
		int endRegion = 0;
		boolean startset = false;
		boolean endset = false;
		for (int i = 0; i < infos.size(); i++) {
			if (startset && endset)
				break;
			if (startKey != null) {
				if (infos.get(i).getStartKey().length != 0) {
					if (Bytes.compareTo(startKey, infos.get(i).getStartKey()) >= 0) {
						startRegion = i;
					} else {
						startset = true;
					}
				}
			}
			if (endKey != null && (!endset)) {
				if (infos.get(i).getEndKey().length != 0) {
					if (Bytes.compareTo(endKey, infos.get(i).getEndKey()) < 0) {
						endRegion = i;
						endset = true;
					}
				} else {
					endRegion = i;
					endset = true;
				}
			} else if (endKey == null) {
				endRegion = i;
			}
		}
		if (startKey != null && endKey != null) {
			long[] ndis = Utilities.dis(startKey, endKey);
			return new long[] { endRegion - startRegion, ndis[0], ndis[1] };
		} else if (startKey == null && endKey == null) {
			return new long[] { endRegion - startRegion, Long.MAX_VALUE,
					Long.MAX_VALUE };
		} else if (startKey == null && endKey != null) {

			startKey = infos.get(startRegion).getEndKey().length == 0 ? infos
					.get(startRegion).getEndKey() : infos.get(startRegion)
					.getStartKey();
			if (startKey.length == 0) {
				return new long[] { endRegion - startRegion, 0, 0 };
			} else {
				long[] ndis = Utilities.dis(startKey, endKey);
				return new long[] { endRegion - startRegion, ndis[0], ndis[1] };
			}

		} else if (startKey != null && endKey == null) {

			endKey = infos.get(endRegion).getStartKey().length == 0 ? infos
					.get(endRegion).getStartKey() : infos.get(endRegion)
					.getEndKey();
			if (endKey.length == 0) {
				return new long[] { endRegion - startRegion, 0, 0 };
			} else {
				long[] ndis = Utilities.dis(startKey, endKey);
				return new long[] { endRegion - startRegion, ndis[0], ndis[1] };
			}

		}
		return new long[] { endRegion - startRegion, 0, 0 };
	}

	private long[] disAdd(long[] dis, long[] dis2) {
		long[] ret = new long[3];
		ret[0] = dis[0] + dis2[0];
		if (dis[1] == dis2[1]) {
			ret[1] = dis[1];
			ret[2] = Math.max(dis[2], dis2[2]);
		} else {
			ret[1] = Math.max(dis[1], dis2[1]);
			ret[2] = dis[1] > dis2[2] ? dis[2] : dis2[2];
		}
		return ret;
	}

	public SimpleOptimizer(final HBaseConfiguration conf, IndexedTable father) {
		this.father = father;
		this.conf = conf;
		this.refresh();
	}

	synchronized public void refresh() {

		HTable metaTable;
		try {

			metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
			metaTable.setAutoFlush(false);
			metaTable.setWriteBufferSize(1024 * 1024 * 12);
			metaTable.setScannerCaching(1000);
			Scan indexScan = new Scan();
			indexScan.setCaching(1000);
			ResultScanner indexScanner = metaTable.getScanner(indexScan);
			Result r = null;
			while ((r = indexScanner.next()) != null) {
				// System.out.println(r);
				byte[] value = r.getValue(HConstants.CATALOG_FAMILY,
						HConstants.REGIONINFO_QUALIFIER);
				if (value == null || value.length == 0) {
					throw new IOException("HRegionInfo was null or empty in ");
				}
				// convert the row result into the HRegionLocation we need!
				HRegionInfo regionInfo = (HRegionInfo) Writables.getWritable(
						value, new HRegionInfo());

				if (this.tableToRegion.get(regionInfo.getTableDesc()
						.getNameAsString()) == null) {
					this.tableToRegion.put(regionInfo.getTableDesc()
							.getNameAsString(), new Vector<HRegionInfo>());

				}
				this.tableToRegion.get(
						regionInfo.getTableDesc().getNameAsString()).add(
						regionInfo);
				// System.out.println("StartKey:"+new
				// String(regionInfo.getStartKey())+"EndKey:"+new
				// String(regionInfo.getEndKey()));
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void run() {
		while (true) {
			this.refresh();
			try {
				sleep(this.inter);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String args[]) {
		SimpleOptimizer r = new SimpleOptimizer(null, null);
		r.refresh();
	}
}
