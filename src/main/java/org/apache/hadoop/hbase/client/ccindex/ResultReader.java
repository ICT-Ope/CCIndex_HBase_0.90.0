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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;

public class ResultReader {

	static final Log LOG = LogFactory.getLog(ResultReader.class);
	private Vector<SingleReader> records;
	public long limit = Long.MAX_VALUE;
	private int finishFlag = 0;
	private ConcurrentLinkedQueue<Result> results = new ConcurrentLinkedQueue<Result>();
	private long[] latence = { 300, 100, 100, 300, 300, 500, 1000, 1000, 1000,
			5000 };
	public long recordN = 0;

	public boolean resultEnough() {
		if(this.limit==0)
			return false;
		if (this.recordN >= this.limit) {
			return true;
		} else {
			return false;
		}
	}

	class ReaderWorker extends Thread {
		ConcurrentLinkedQueue result;
		SingleReader reader;
		ResultReader father;
		public String name;

		public ReaderWorker(SingleReader reader, ConcurrentLinkedQueue result,
				ResultReader father, String name)

		{
			this.name = name;
			this.reader = reader;
			this.result = result;
			this.father = father;
		}

		public void run() {

			try {

				Vector<Result> r = reader.next();
				while ((!(reader.isOver() && r.size() == 0))
						&& (!this.father.resultEnough())) {

					this.result.addAll(r);
					r = reader.next();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				LOG.error(e.getMessage());
			}
			this.father.finishFlag++;
		}
	}
	/**
	 * 
	 * @param records
	 * @param limit
	 */
	public ResultReader(Vector<SingleReader> records, long limit) {
		this.records = records;
		if (limit != 0) {
			this.limit = limit;
		}
	}

	public void init() {
		this.finishFlag = 0;
		int i = 0;
		for (SingleReader reader : this.records) {
			ReaderWorker worker = new ReaderWorker(reader, this.results, this,
					("" + i));
			worker.setName("" + i++);
			worker.start();
		}
	}

	public Result next() {
		if ((this.finishFlag >= this.records.size()) && this.results.isEmpty()) {
			return null;
		} else {
			int i = 0;
			while (this.results.isEmpty()) {
				try {
					if (i < this.latence.length) {
						Thread.currentThread().sleep(this.latence[i++]);
						LOG.info("buffer is empty to sleep");
					} else {
						return null;
					}
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					LOG.error(e.getMessage());
				}
			}
			if (this.resultEnough()) {
				return null;
			} else {
				this.recordN++;
				return this.results.poll();
			}

		}
	}

}
