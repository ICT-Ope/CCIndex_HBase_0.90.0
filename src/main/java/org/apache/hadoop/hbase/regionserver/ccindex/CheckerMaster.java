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

import java.util.concurrent.ConcurrentHashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ccindex.CCIndexDescriptor;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;

import org.apache.hadoop.hbase.regionserver.HRegionServer;



public class CheckerMaster extends Thread {
	long sleeptime = 260000;
	ConcurrentHashMap<String, CCIndexDescriptor> indexTableDescriptorMap;
	private final Configuration conf;
	int flag = 0;
	HRegionServer server;

	public CheckerMaster(

			Configuration conf,
			ConcurrentHashMap<String, CCIndexDescriptor> indexTableDescriptorMap,
			HRegionServer server) {
		this.conf = conf;
		this.server = server;
		this.indexTableDescriptorMap = indexTableDescriptorMap;

	}

	private void checkTable(CCIndexDescriptor des) {
		if (des.getIndexes().size() == 0)
			return;
		Checker[] checkers = new Checker[des.getIndexes().size() + 1];
		checkers[0] = new Checker(conf, des.getBaseTableDescriptor()
				.getName(), des, this.server);
		int i = 1;
		for (IndexSpecification index : des.getIndexes()) {
			checkers[i] = new Checker(conf, index.getCCITName(), des, this.server);
			i++;
		}
		for (int j = 0; j < checkers.length; j++) {
			checkers[j].start();
		}
		for (int j = 0; j < checkers.length; j++) {
			try {
				checkers[j].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void run() {
		while (!this.server.isStopped()) {
			try {

				sleep(this.sleeptime);
				int size = IndexedRegionServer.workers.size();
				int freeN = 0;
				for (IndexedRegionServer.Worker w : IndexedRegionServer.workers.values()) {
					if (w.free) {
						freeN++;
					}
				}

				if (freeN > size / 2)
					for (String key : this.indexTableDescriptorMap.keySet()) {

						CCIndexDescriptor des = this.indexTableDescriptorMap
								.get(key);
						this.checkTable(des);

					}

			} catch (Exception e) {
				continue;
			}
		}
	}
}