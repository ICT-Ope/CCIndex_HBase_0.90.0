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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class CCIndexConstants {
	public final static String CCT_TAIL = "CCT";
	final static String CCIT_TAIL = "CCIT";
	public static final byte[] INDEX_COL_FAMILY_NAME = Bytes.toBytes("INDEX");
	public static final byte[] INDEX_COL_FAMILY = Bytes.add(
			INDEX_COL_FAMILY_NAME,
			new byte[] { KeyValue.COLUMN_FAMILY_DELIMITER });
	public static final byte[] INDEX_BASE_ROW = Bytes.toBytes("ROW");
	public static final byte[] INDEX_BASE_ROW_COLUMN = Bytes.add(
			INDEX_COL_FAMILY, INDEX_BASE_ROW);
	public static final byte[] maxRowKey = { (byte) 0xff, (byte) 0xff,
			(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
			(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
			(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
			(byte) 0xff, (byte) 0xff, (byte) 0xff, };
	static final byte[] INDEXES_KEY = Bytes.toBytes("INDEXES");
	final static byte[] CCT_KEY = Bytes.toBytes("CCT");
	final static byte[] CCIT_KEY = Bytes.toBytes("CCIT");
	final static byte[] ORGTABLE_COLUMN_DELIMITER = Bytes.toBytes(";");
	public static final byte[] BASE_KEY = Bytes.toBytes("BASE");
	public static final byte[] EMPYT_VALUE = Bytes.add(maxRowKey, Bytes
			.toBytes("EMPTY"));
	public static final byte[] indexID_Base = Bytes.toBytes("indexed-org");
	public static final String CheckNumNode="checknumber";

}
