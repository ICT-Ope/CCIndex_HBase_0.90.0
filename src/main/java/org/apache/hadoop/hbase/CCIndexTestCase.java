/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ccindex.CCIndexConstants;
import org.apache.hadoop.hbase.client.ccindex.CCIndexDescriptor;
import org.apache.hadoop.hbase.client.ccindex.IndexKeyGenerator;
import org.apache.hadoop.hbase.client.ccindex.IndexNotFoundException;
import org.apache.hadoop.hbase.client.ccindex.IndexSpecification;
import org.apache.hadoop.hbase.client.ccindex.IndexedTable;
import org.apache.hadoop.hbase.client.ccindex.CCIndexAdmin;

import org.apache.hadoop.hbase.client.ccindex.Range;
import org.apache.hadoop.hbase.client.ccindex.ResultReader;
import org.apache.hadoop.hbase.client.ccindex.SimpleIndexKeyGenerator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Script used evaluating of CCIndex,test case use one base table three CCITs
 * and four CCTs.
 * <p>
 * users can use sequentialWrite to create a CCIndex with three indexes and use
 * clusterScan to perform multi-dimensional range query.
 * </p>
 * @author liujia
 * @version 90.0
 */
public class CCIndexTestCase {
	private static final Log LOG = LogFactory.getLog(CCIndexTestCase.class
			.getName());
	private static Vector<Long> time = new Vector<Long>();
	private static final int ROW_LENGTH = 1024;
	private static final int ONE_GB = 1024 * 1000 * 100;

	private static final long ROWS_PER_GB = ONE_GB / ROW_LENGTH;

	protected static HTableDescriptor TABLE_DESCRIPTOR_ORG;
	protected static CCIndexDescriptor TABLE_DESCRIPTOR;
	protected static IndexSpecification INDEX_SPECIFICATION;
	protected static IndexSpecification INDEX_SPECIFICATION_2;
	protected static IndexSpecification INDEX_SPECIFICATION_3;
	protected static byte[] INDEX_ID = Bytes.toBytes("indexed_INDEX");
	protected static byte[] INDEX_ID_2 = Bytes.toBytes("indexed2_INDEX");
	protected static byte[] INDEX_ID_3 = Bytes.toBytes("indexed3_INDEX");
	public static final byte[] FAMILY_NAME = CCIndexConstants.INDEX_COL_FAMILY_NAME;
	public static final byte[] FAMILY_NAME_INDEX = CCIndexConstants.INDEX_COL_FAMILY_NAME;
	public static final byte[] FAMILY_NAME_INDEX_2 = CCIndexConstants.INDEX_COL_FAMILY_NAME;
	public static final byte[] FAMILY_NAME_INDEX_3 = CCIndexConstants.INDEX_COL_FAMILY_NAME;
	public static final byte[] QUALIFIER_NAME1 = Bytes.toBytes("INDEX1");
	public static final byte[] QUALIFIER_NAME2 = Bytes.toBytes("INDEX2");
	public static final byte[] QUALIFIER_NAME3 = Bytes.toBytes("INDEX3");
	public static final byte[] QUALIFIER_NAME_DATA = Bytes.toBytes("data");

	public static final byte[] FAMILY_NAME_AND_QUALIFIER_NAME_DATA = KeyValue
			.makeColumn(FAMILY_NAME, QUALIFIER_NAME_DATA);

	public static final byte[] FAMILY_NAME_AND_QUALIFIER_NAME = KeyValue
			.makeColumn(FAMILY_NAME_INDEX, QUALIFIER_NAME1);
	public static final byte[] FAMILY_NAME_AND_QUALIFIER_NAME_2 = KeyValue
			.makeColumn(FAMILY_NAME_INDEX_2, QUALIFIER_NAME2);
	public static final byte[] FAMILY_NAME_AND_QUALIFIER_NAME_3 = KeyValue
			.makeColumn(FAMILY_NAME_INDEX_3, QUALIFIER_NAME3);

	static {
		TABLE_DESCRIPTOR_ORG = new HTableDescriptor("TestTable");
		TABLE_DESCRIPTOR_ORG.addFamily(new HColumnDescriptor(FAMILY_NAME));

		try {
			TABLE_DESCRIPTOR = new CCIndexDescriptor(TABLE_DESCRIPTOR_ORG);
			TABLE_DESCRIPTOR.setIndexedColumns(new byte[][] {
					FAMILY_NAME_AND_QUALIFIER_NAME,
					FAMILY_NAME_AND_QUALIFIER_NAME_2,
					FAMILY_NAME_AND_QUALIFIER_NAME_3 });
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		INDEX_SPECIFICATION = new IndexSpecification(INDEX_ID,
				FAMILY_NAME_AND_QUALIFIER_NAME,
				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA},
				TABLE_DESCRIPTOR);
		INDEX_SPECIFICATION_2 = new IndexSpecification(INDEX_ID_2,
				FAMILY_NAME_AND_QUALIFIER_NAME_2,
				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA},
				TABLE_DESCRIPTOR);
		INDEX_SPECIFICATION_3 = new IndexSpecification(INDEX_ID_3,
				FAMILY_NAME_AND_QUALIFIER_NAME_3,
				new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA},
				TABLE_DESCRIPTOR);
		TABLE_DESCRIPTOR.addIndex(INDEX_SPECIFICATION);
		TABLE_DESCRIPTOR.addIndex(INDEX_SPECIFICATION_2);
		TABLE_DESCRIPTOR.addIndex(INDEX_SPECIFICATION_3);
	}
	private static final String ClUSTER_SCAN = "clusterScan";
	private static final String SEQUENTIAL_WRITE = "sequentialWrite";
	private static final List<String> COMMANDS = Arrays.asList(new String[] {
			SEQUENTIAL_WRITE, ClUSTER_SCAN });
	volatile HBaseConfiguration conf;
	private int N = 1;
	private long R = ROWS_PER_GB;
	private static final Path PERF_EVAL_DIR = new Path("performance_evaluation");

	/**
	 * Regex to parse lines in input file passed to mapreduce task.
	 */
	public static final Pattern LINE_PATTERN = Pattern
			.compile("startRow=(\\d+),\\s+"
					+ "perClientRunRows=(\\d+),\\s+totalRows=(\\d+),\\s+clients=(\\d+)");

	/**
	 * Enum for map metrics. Keep it out here rather than inside in the Map
	 * inner-class so we can find associated properties.
	 */
	protected static enum Counter {
		/** elapsed time */
		ELAPSED_TIME,
		/** number of rows */
		ROWS
	}

	/**
	 * Constructor
	 * 
	 * @param c
	 *            Configuration object
	 */
	public CCIndexTestCase(final HBaseConfiguration c) {
		this.conf = c;
	}

	/**
	 * Implementations can have their status set.
	 */
	static interface Status {
		/**
		 * Sets status
		 * 
		 * @param msg
		 *            status message
		 * @throws IOException
		 */
		void setStatus(final String msg) throws IOException;
	}

	/**
	 * MapReduce job that runs a performance evaluation client in each map task.
	 */
	@SuppressWarnings("unchecked")
	/*
	 * If table does not already exist, create. @param c Client to use checking.
	 * 
	 * @return True if we created the table. @throws IOException
	 */
	private boolean checkTable(CCIndexAdmin admin, String cmd)
			throws IOException {

		boolean tableExists = admin.tableExists(TABLE_DESCRIPTOR
				.getBaseTableDescriptor().getName());
		if (tableExists && cmd.equals(SEQUENTIAL_WRITE)) {
			try {
				admin.disableTable(this.TABLE_DESCRIPTOR.getBaseCCTName());
				admin.disableTable(TABLE_DESCRIPTOR.getBaseTableDescriptor()
						.getName());
				for (IndexSpecification spec : this.TABLE_DESCRIPTOR
						.getIndexes()) {
					admin.disableTable(spec.getCCITName());
					admin.disableTable(spec.getCCTName());
				}
				admin.deleteTable(this.TABLE_DESCRIPTOR.getBaseCCTName());
				admin.deleteTable(TABLE_DESCRIPTOR.getBaseTableDescriptor()
						.getName());
				for (IndexSpecification spec : this.TABLE_DESCRIPTOR
						.getIndexes()) {
					admin.deleteTable(spec.getCCITName());
					admin.deleteTable(spec.getCCTName());
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
			admin.createIndexedTable(TABLE_DESCRIPTOR);
			LOG.info("Table " + TABLE_DESCRIPTOR + " created");
		} else if (!tableExists) {
			// admin.deleteTable(TABLE_DESCRIPTOR.getBaseTableDescriptor().getName());
			admin.createIndexedTable(TABLE_DESCRIPTOR);
			// admin.createTable(TABLE_DESCRIPTOR);
			LOG.info("Table " + TABLE_DESCRIPTOR + " created");
		}
		return !tableExists;
	}

	@SuppressWarnings("unused")
	private void doMultipleClients(final String cmd) throws IOException {
		final List<Thread> threads = new ArrayList<Thread>(this.N);
		final long perClientRows = R / N;

		for (long i = 0; i < this.N; i++) {
			Thread t = new Thread(Long.toString(i)) {
				public void run() {
					super.run();
					CCIndexTestCase pe = new CCIndexTestCase(conf);
					long index = Integer.parseInt(getName());
					try {
						long elapsedTime = pe.runOneClient(cmd, (int) index
								* perClientRows, (int) perClientRows, R,
								new Status() {
									public void setStatus(final String msg)
											throws IOException {
										LOG.info("client-" + getName() + " "
												+ msg);
									}
								});
						LOG.info("Finished " + getName() + " in "
								+ elapsedTime++ + "ms writing " + perClientRows
								+ " rows");
						LOG.info("Finished " + getName() + " in " + elapsedTime
								+ "ms writing " + perClientRows + " rows"
								+ ", throughput " + R * 1000.0 / elapsedTime
								+ " Rec/s" + "  " + R * ROW_LENGTH
								/ elapsedTime + " KBytes/s");
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			};
			threads.add(t);
		}
		for (Thread t : threads) {
			t.start();
		}
		for (Thread t : threads) {
			while (t.isAlive()) {
				try {
					t.join();
				} catch (InterruptedException e) {
					LOG.debug("Interrupted, continuing" + e.toString());
				}
			}
		}
	}

	/*
	 * Run a mapreduce job. Run as many maps as asked-for clients. Before we
	 * start up the job, write out an input file with instruction per client
	 * regards which row they are to start on. @param cmd Command to run.
	 * 
	 * @throws IOException
	 */

	/*
	 * Write input file of offsets-per-client for the mapreduce job. @param c
	 * Configuration @return Directory that contains file written. @throws
	 * IOException
	 */
	private Path writeInputFile(final Configuration c) throws IOException {
		FileSystem fs = FileSystem.get(c);
		if (!fs.exists(PERF_EVAL_DIR)) {
			fs.mkdirs(PERF_EVAL_DIR);
		}
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
		Path subdir = new Path(PERF_EVAL_DIR, formatter.format(new Date()));
		fs.mkdirs(subdir);
		Path inputFile = new Path(subdir, "input.txt");
		PrintStream out = new PrintStream(fs.create(inputFile));
		try {
			for (long i = 0; i < (this.N * 10); i++) {
				// Write out start row, total number of rows per client run:
				// 1/10th of
				// (R/N).
				long perClientRows = (this.R / this.N);
				out.println("startRow=" + i * perClientRows
						+ ", perClientRunRows=" + (perClientRows / 10)
						+ ", totalRows=" + this.R + ", clients=" + this.N);
			}
		} finally {
			out.close();
		}
		return subdir;
	}

	/*
	 * A test. Subclass to particularize what happens per row.
	 */
	static abstract class Test {
		protected final Random rand = new Random(System.currentTimeMillis());
		static long callTimeRW = 0L;
		protected final long startRow;

		protected final long perClientRunRows;

		protected final long totalRows;

		protected final Status status;

		protected CCIndexAdmin admin;

		// protected IndexedTable table;
		protected int tableN = 5;
		protected IndexedTable tables[];
		protected HTable tableOrg;

		protected volatile HBaseConfiguration conf;

		Test(final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super();
			this.startRow = startRow;
			this.perClientRunRows = perClientRunRows;
			this.totalRows = totalRows;
			this.status = status;
			// this.table = null;
			this.conf = conf;
		}

		protected String generateStatus(final long sr, final long i,
				final long lr) {
			return sr + "/" + i + "/" + lr;
		}

		static int iflag = 0;

		public IndexedTable getTable() {

			IndexedTable ret = null;
			synchronized (this) {
				iflag++;
				if (iflag >= this.tables.length) {
					iflag = 0;
				}
				ret = this.tables[iflag];
			}
			return ret;
		}

		protected long getReportingPeriod() {
			return this.perClientRunRows / 10;
		}

		void testSetup() throws IOException {
			this.admin = new CCIndexAdmin(conf);
			this.tables = new IndexedTable[this.tableN];

			for (int i = 0; i < this.tableN; i++) {
				this.tables[i] = new IndexedTable(conf, TABLE_DESCRIPTOR
						.getBaseTableDescriptor().getName());
				this.tables[i].setAutoFlush(false);
				this.tables[i].setWriteBufferSize(1024 * 1024 * 10);
				this.tables[i].setScannerCaching(30);
			}

		}

		@SuppressWarnings("unused")
		void testTakedown() throws IOException {

			for (int i = 0; i < this.tableN; i++) {
				this.tables[i].flushCommits();
			}
		}

		/*
		 * Run test @return Elapsed time. @throws IOException
		 */
		long test() throws IOException {
			long elapsedTime;
			testSetup();
			try {
				Thread.currentThread().sleep(4000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			long startTime = System.currentTimeMillis();
			try {
				long lastRow = this.startRow + this.perClientRunRows;
				// Report on completion of 1/10th of total.

				for (long i = this.startRow; i < lastRow; i++) {
					long start = System.nanoTime();
					testRow(i);
					CCIndexTestCase.time.add(System.nanoTime() - start);
					if (status != null && i > 0
							&& (i % getReportingPeriod()) == 0) {
						status.setStatus(generateStatus(this.startRow, i,
								lastRow));
					}
				}
				elapsedTime = System.currentTimeMillis() - startTime;
			} finally {
				System.out.println("test take down successfully");
				testTakedown();
			}
			return elapsedTime;
		}

		/*
		 * Test for individual row. @param i Row index.
		 */
		abstract void testRow(final long i) throws IOException;

		/*
		 * @return Test name.
		 */
		abstract String getTestName();
	}

	/*
	 * Cluster
	 */
	class MultiDimensionRangeQueryTest extends Test {
		MultiDimensionRangeQueryTest(HBaseConfiguration conf, long startRow,
				long perClientRunRows, long totalRows, Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
			// TODO Auto-generated constructor stub
		}

		private ResultReader testScanner;

		void testSetup() throws IOException {
			super.testSetup();

			/**
			 * construct a query (A.a and A.b and A.c) or (B.a and B.b and B.c)
			 * 
			 */
			Range[] A = new Range[3];
			Range Aa = new Range();
			Aa
					.setBaseColumns(new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA });
			Aa.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME);
			Aa.setStart(format(100));
			Aa.setEnd(format(110));
			Range Ab = new Range();
			Ab
					.setBaseColumns(new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA });
			Ab.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME_2);
			Ab.setStart(format(100));
			Ab.setEnd(format(1000));
			Range Ac = new Range();
			Ac
					.setBaseColumns(new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA });
			Ac.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME_3);
			Ac.setStart(format(100));
			Ac.setEnd(null);
			A[0] = Aa;
			A[1] = Ab;
			A[2] = Ac;

			// A like 100<=q1<=110 and 1000<=q2<=100 and q3>=100

			Range[] B = new Range[3];
			Range Ba = new Range();
			Ba
					.setBaseColumns(new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA });
			Ba.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME);
			Ba.setStart(null);
			Ba.setEnd(format(5000));
			Range Bb = new Range();
			Bb
					.setBaseColumns(new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA });
			Bb.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME_2);
			Bb.setStart(format(2300));
			Bb.setEnd(format(2310));
			Range Bc = new Range();
			Bc
					.setBaseColumns(new byte[][] { FAMILY_NAME_AND_QUALIFIER_NAME_DATA });
			Bc.setColumn(FAMILY_NAME_AND_QUALIFIER_NAME_3);
			Bc.setStart(format(300));
			Bc.setEnd(null);
			B[0] = Ba;
			B[1] = Bc;
			B[2] = Bb;
			// B like q<=5000 and 2300<q2<=2310 and q3>300

			Range ranges[][] = { A, B };
			// ranges like A or B
			/**
			 * this.testScanner = table.getScanner(new byte [][] {COLUMN_NAME},
			 * format(this.startRow));
			 */
			this.testScanner = this.getTable().MDRQScan(ranges, 0);
			this.testScanner.init();
		}

		@Override
		String getTestName() {
			// TODO Auto-generated method stub
			return "ClusterScanTest";
		}

		@Override
		void testRow(long i) throws IOException {
			// TODO Auto-generated method stub
			Result r = this.testScanner.next();
			if (r != null)
				System.out.println(r.toString());
		}
	}

	class SequentialWriteTest extends Test {

		SequentialWriteTest(final HBaseConfiguration conf, final long startRow,
				final long perClientRunRows, final long totalRows,
				final Status status) {
			super(conf, startRow, perClientRunRows, totalRows, status);
		}

		void testRow(final long i) throws IOException {

			byte[] row = format(i);
			Put put = new Put(row);
			put.add(FAMILY_NAME_INDEX, QUALIFIER_NAME1, format(i));
			put.add(FAMILY_NAME_INDEX_2, QUALIFIER_NAME2, format(i));
			put.add(FAMILY_NAME_INDEX_3, QUALIFIER_NAME3, format(i));
			put.add(FAMILY_NAME, QUALIFIER_NAME_DATA, generateValue(this.rand));
			this.getTable().put(put);
		}

		String getTestName() {
			return "sequentialWrite";
		}
	}

	/*
	 * Format passed integer. @param number @return Returns zero-prefixed
	 * 10-byte wide decimal version of passed number (Does absolute in case
	 * number is negative).
	 */
	public static byte[] format(final long number) {
		byte[] b = new byte[10];
		long d = Math.abs(number);
		for (int i = b.length - 1; i >= 0; i--) {
			b[i] = (byte) ((d % 10) + '0');
			d /= 10;
		}
		return b;
	}

	/*
	 * This method takes some time and is done inline uploading data. For
	 * example, doing the mapfile test, generation of the key and value consumes
	 * about 30% of CPU time. @return Generated random value to insert into a
	 * table cell.
	 */
	static byte[] generateValue(final Random r) {
		byte[] b = new byte[ROW_LENGTH];
		// r.nextBytes(b);
		for (int i = 0; i < b.length; ++i) {
			int ch = r.nextInt() % 26;
			if (ch < 0)
				ch = ch * -1;
			b[i] = (byte) (ch + 65);
		}
		return b;
	}

	byte[] getRandomRow(final Random random, final long totalRows) {

	
		return format(random.nextInt(Integer.MAX_VALUE) % totalRows);

		/*
		 * Long rand = null; while (RAND_LIST.contains(rand = new Long(random
		 * .nextInt(Integer.MAX_VALUE) % (totalRows)))) ; RAND_LIST.add(rand);
		 * return format(rand);
		 */

	}

	byte[] getRandomRow(final Random random, final long totalRows,
			final long startRow) {

		return format((random.nextInt(Integer.MAX_VALUE) % totalRows)
				+ startRow);


		/*
		 * Long rand = null; while (RAND_LIST.contains(rand = new Long(random
		 * .nextInt(Integer.MAX_VALUE) % (totalRows)))) ; RAND_LIST.add(rand);
		 * return format(rand);
		 */

	}

	byte[] getRandomNumber(final Random random, final long totalRows) {


		/*
		 * Long rand = null; // two SETs while (RAND_NUMBER_LIST.contains(rand =
		 * new Long(random .nextInt(Integer.MAX_VALUE) % (totalRows)))) ;
		 * RAND_NUMBER_LIST.add(rand); return format(rand);
		 */

		return format(random.nextInt(Integer.MAX_VALUE) % totalRows);
	}

	byte[] getRandomNumber(final Random random, final long totalRows,
			long startRows) {

		/*
		 * Long rand = null; // two SETs while (RAND_NUMBER_LIST.contains(rand =
		 * new Long(random .nextInt(Integer.MAX_VALUE) % (totalRows)))) ;
		 * RAND_NUMBER_LIST.add(rand); return format(rand);
		 */

		return format(random.nextInt(Integer.MAX_VALUE) % totalRows + startRows);
	}

	long runOneClient(final String cmd, final long startRow,
			final long perClientRunRows, final long totalRows,
			final Status status) throws IOException {
		status.setStatus("Start " + cmd + " at offset " + startRow + " for "
				+ perClientRunRows + " rows");
		long totalElapsedTime = 0;
		if (cmd.equals(ClUSTER_SCAN)) {
			Test t = new MultiDimensionRangeQueryTest(this.conf, startRow,
					perClientRunRows, totalRows, status);
			totalElapsedTime = t.test();
		} else if (cmd.equals(SEQUENTIAL_WRITE)) {
			Test t = new SequentialWriteTest(this.conf, startRow,
					perClientRunRows, totalRows, status);
			totalElapsedTime = t.test();
		} else {
			new IllegalArgumentException("Invalid command value: " + cmd);
		}
		status.setStatus("Finished " + cmd + " in " + totalElapsedTime
				+ "ms at offset " + startRow + " for " + perClientRunRows
				+ " rows" + " throughput " + perClientRunRows * 1000.0
				/ (totalElapsedTime + 1) + " Rec/s" + "  " + perClientRunRows
				* ROW_LENGTH / (totalElapsedTime + 1) + " KBytes/s");

		return totalElapsedTime;
	}

	private void runNIsOne(final String cmd) {
		Status status = new Status() {
			@SuppressWarnings("unused")
			public void setStatus(String msg) throws IOException {
				LOG.info(msg);
			}
		};

		CCIndexAdmin admin = null;
		try {

			if (cmd.equals(this.SEQUENTIAL_WRITE))
				checkTable(new CCIndexAdmin(this.conf), cmd);
			runOneClient(cmd, 0, this.R, this.R, status);
		} catch (Exception e) {
			LOG.error("Failed", e);
		}
	}

	private void runTest(final String cmd) throws IOException {

		try {
			if (N == 1) {
				// If there is only one client and one HRegionServer, we assume
				// nothing
				// has been set up at all.
				runNIsOne(cmd);
			} else {
				// Else, run
				try {
					runNIsMoreThanOne(cmd);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} finally {
			java.io.File d = new java.io.File("/opt/result.txt");
			FileWriter w = new FileWriter(d);

			for (Long l : CCIndexTestCase.time)
				// LOG.debug(l+""+"ms");
				w.write(l + "" + "ms\n");
		}
	}

	private void runNIsMoreThanOne(final String cmd) throws IOException,
			InterruptedException, ClassNotFoundException {
		if (cmd.equals(this.SEQUENTIAL_WRITE))
			checkTable(new CCIndexAdmin(this.conf), cmd);
		doMultipleClients(cmd);
	}

	private void printUsage() {
		printUsage(null);
	}

	private void printUsage(final String message) {
		if (message != null && message.length() > 0) {
			System.err.println(message);
		}
		System.err.println("Usage: java " + this.getClass().getName()
				+ " [--master=HOST:PORT] \\");
		System.err.println("   <command> <nclients>");
		System.err.println();
		System.err.println("Options:");
		System.err.println(" master          Specify host and port of HBase "
				+ "cluster master. If not present,");
		System.err
				.println("                 address is read from configuration");
		System.err.println();
		System.err.println("Command:");
		System.err.println(" clusterScan  Run cluster table scan read test");
		System.err.println(" sequentialWrite Run sequential write test");
		System.err.println();
		System.err.println("Args:");
		System.err.println("Examples:");
		System.err.println(" To run a single evaluation client:");
		System.err
				.println(" $ bin/hbase "
						+ "org.apache.hadoop.hbase.PerformanceEvaluation sequentialWrite 1");
	}

	private void getArgs(final int start, final String[] args) {
		if (start + 1 > args.length) {
			throw new IllegalArgumentException(
					"must supply the number of clients");
		}

		N = Integer.parseInt(args[start]);
		if (N > 500 || N < 1) {
			throw new IllegalArgumentException(
					"Number of clients must be between " + "1 and 500.");
		}

		// Set total number of rows to write.
		// this.R = this.R * N;
	}

	private int doCommandLine(final String[] args) {
		// Process command-line args. TODO: Better cmd-line processing
		// (but hopefully something not as painful as cli options).
		int errCode = -1;
		if (args.length < 1) {
			printUsage();
			return errCode;
		}

		try {
			for (int i = 0; i < args.length; i++) {
				String cmd = args[i];
				if (cmd.equals("-h") || cmd.startsWith("--h")) {
					printUsage();
					errCode = 0;
					break;
				}

				if (COMMANDS.contains(cmd)) {
					getArgs(i + 1, args);
					runTest(cmd);
					errCode = 0;
					break;
				}

				printUsage();
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return errCode;
	}

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		HBaseConfiguration c = new HBaseConfiguration();
		System.exit(new CCIndexTestCase(c).doCommandLine(args));

	}
}
