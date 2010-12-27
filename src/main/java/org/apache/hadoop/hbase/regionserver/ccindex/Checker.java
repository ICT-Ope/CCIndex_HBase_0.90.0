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

	private final Configuration conf;
	private final byte[] targetTableName;
	private CCIndexDescriptor des;
	private HashMap<IndexSpecification, HTable> CCTS = new HashMap<IndexSpecification, HTable>();
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
					this.CCTS.put(index, indexTable);
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
	private void recovery(ResultScanner scanner, HTable org, int space) {
		Result r = null;
		try {
			space = space + 10;
			while ((r = scanner.next()) != null && space > 0
					&& !this.server.isStopped()) {
				if (!this.emptyRow(r)) {
					Get get = new Get(r.getRow());
					Result oldResult = null;
					try {
						try {
							oldResult = org.get(get);
						} catch (Throwable e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
					} catch (Exception e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					int setOfOnceRecovery = 100;
					if (this.emptyRow(oldResult)) {
						while (this.emptyRow(oldResult)
								&& (!this.emptyRow(r))) {
							if (setOfOnceRecovery <= 0) {
								setOfOnceRecovery = 100;
							}
							boolean recovered = false;
							SortedMap<byte[], byte[]> columnValues = IndexedRegion
									.convertToValueMap(r);
							System.out.println("table :"
									+ new String(this.targetTableName)
									+ "  lost data");
							System.out.println("data is:" + r);
							if (isbase) {
								System.out.println("base table lost data");
								for (IndexSpecification ind : this.CCTS
										.keySet()) {
									try {
										byte[] indexRow = ind
												.getKeyGenerator()
												.createIndexKey(r.getRow(),
														columnValues.get(ind.getIndexedColumn()));
										System.out.println("recover from"
												+ ind.getIndexId());
										System.out
												.println("recover from get row"
														+ new String(
																indexRow));

										Result recover = this.CCTS.get(ind)
												.get(new Get(indexRow));

										SortedMap<byte[], byte[]> columns = IndexedRegion
												.convertToValueMap(recover);
										Put update = IndexMaintenanceUtils
												.createOrgUpdate(
														r.getRow(), columns);

										System.out.println("recover from:"
												+ ind.getIndexId());
										System.out.println(update);
										recovered = true;
										org.put(update);
										break;
									} catch (Throwable e) {
										e.printStackTrace();
										System.out.println("get from:"
												+ ind.getIndexId()
												+ " error");
										continue;
										// TODO Auto-generated catch block

									}

								}
							} else {
								System.out
										.println("indexed table lost data");
								SimpleIndexKeyGenerator sg = (SimpleIndexKeyGenerator) this.indexOrg
										.getKeyGenerator();
								byte[] row = r.getRow();
								System.out.println("row is:"
										+ new String(row));
								byte[] roworg = sg.getOrgRowKey(row);
								System.out.println("org row is:"
										+ new String(roworg));
								Result recovery = null;
								try {
									if (this.baseTable != null)
										recovery = this.baseTable
												.get(new Get(roworg));
								} catch (Throwable e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
									System.out
											.println("get from:base error");

								}
								if (!this.emptyRow(recovery)) {
									SortedMap<byte[], byte[]> columns = IndexedRegion
											.convertToValueMap(recovery);
									Put update = IndexMaintenanceUtils
											.createOrgUpdate(row, columns);
									System.out.println("recover from:"
											+ "base");
								//	System.out.println(update);
									recovered = true;
									try {
										org.put(update);
									} catch (Throwable e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
								if (!recovered) {
									for (IndexSpecification ind : this.CCTS
											.keySet()) {
										try {
											byte[] indexRow = ind
													.getKeyGenerator()
													.createIndexKey(roworg,
															columnValues.get(ind.getIndexedColumn()));
											System.out
													.println("recover from"
															+ ind
																	.getIndexId());
											System.out
													.println("recover from get row"
															+ new String(
																	indexRow));

											Result recovery2 = this.CCTS
													.get(ind)
													.get(new Get(indexRow));
											if (!this.emptyRow(recovery2)) {

												SortedMap<byte[], byte[]> columns = IndexedRegion
														.convertToValueMap(recovery2);
												Put update = IndexMaintenanceUtils
														.createOrgUpdate(r
																.getRow(),
																columns);

												System.out
														.println("recover from:"
																+ ind
																		.getIndexId());
											//	System.out.println(update);
												recovered = true;
												org.put(update);
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
							}
							if (recovered == false) {
								System.out
										.println("!!!!!!!!!!!!!!!!!!!!!can't be recover");
							}
							r = scanner.next();
							if (!this.emptyRow(r)) {
								get = new Get(r.getRow());
								oldResult = null;

								setOfOnceRecovery--;
								if (setOfOnceRecovery == 0)
									try {
										oldResult = org.get(get);
									} catch (Throwable e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
							}
						}
					}
				}
				space--;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void run() {
		this.init();
		HTable table;
		HTable org = null;
		try {
			org = new HTable(conf, this.targetTableName);
			org.setWriteBufferSize(1024 * 1024 * 5);
			
			table = new HTable(conf, Bytes.add(this.targetTableName,Bytes.toBytes("-"),
					Bytes.toBytes(CCIndexConstants.CCT_TAIL)));
			table.setScannerCaching(400);
			Scan scan = new Scan();
			ResultScanner scanner = null;
			scanner = table.getScanner(scan);
			Result r = null;
			Random d = new Random(System.currentTimeMillis());
			boolean first = true;
			Result start = null;
			while (!this.emptyRow((r = scanner.next()))
					&& !this.server.isStopped()) {
				int orgspace = d.nextInt(this.maxSpace);
				int space = orgspace;
				if (first) {
					if (!this.emptyRow(r)) {
						Get get = new Get(r.getRow());

						Result oldResult = null;
						try {
							oldResult = org.get(get);
						} catch (Throwable e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						if (this.emptyRow(oldResult)) {
							Scan scan2 = new Scan();
							scan2.setStartRow(r.getRow());
							ResultScanner scanner2 = null;
							scanner2 = table.getScanner(scan2);
							this.recovery(scanner2, org, orgspace);
							start = r;
						} else {
							start = r;
						}

					}
					first = false;
				}
				try {
					sleep(this.sleepTime);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				while (space > 0 && !this.emptyRow(r)) {
					r = scanner.next();
					space--;
				}
				if (!this.emptyRow(r)) {
					Get get = new Get(r.getRow());
					Result oldResult = null;
					try {
						oldResult = org.get(get);
					} catch (Throwable e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if (this.emptyRow(oldResult)) {
						Scan scan2 = new Scan();
						scan2.setStartRow(start.getRow());
						ResultScanner scanner2 = null;
						scanner2 = table.getScanner(scan2);
						this.recovery(scanner2, org, orgspace);
					} else {
						start = r;
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (org != null)
			try {
				org.flushCommits();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	}
}