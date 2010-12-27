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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.regionserver.ccindex.ByteUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Creates indexed keys for a single column....
 * 
 */
public class SimpleIndexKeyGenerator implements IndexKeyGenerator {

	public SimpleIndexKeyGenerator() {
		// For Writable
	}

	public static void main(String[] arg) {
		long a = 9999;
		("0000" + a).getBytes();
		byte[] b = "asdfasdf".getBytes();
		byte[] c = Bytes.add(b, ByteUtil.putLong(a));
		long d = ByteUtil.getLong(c, c.length - 8);
		// System.out.println(d);
		getLength(d);
		Integer.parseInt(new String(getLength(d)));

	}

	/** {@inheritDoc} */
	public byte[] createIndexKey(byte[] rowKey, byte[] indexedColumnValue) {
		// return Bytes.add(columns.get(column), rowKey);
		return CreateICTIndexKey(rowKey, indexedColumnValue);
	}

	public byte[] CreateICTIndexKey(byte[] rowKey, byte[] indexedColumnValue) {
		byte[] b = Bytes.add(indexedColumnValue, rowKey);
		// byte [] newKey=new byte[b.length+8];
		long columnLength = indexedColumnValue.length;
		return Bytes.add(b, getLength(columnLength));
	}

	static String len7 = "0000000";
	static String len6 = "000000";
	static String len5 = "00000";
	static String len4 = "0000";
	static String len3 = "000";
	static String len2 = "00";
	static String len1 = "0";

	public static byte[] getLength(long len) {
		String lens = len + "";
		StringBuffer s = new StringBuffer();

		switch (lens.length()) {
		case 1:
			s.append(len7);
			break;
		case 2:
			s.append(len6);
			break;
		case 3:
			s.append(len5);
			break;
		case 4:
			s.append(len4);
			break;
		case 5:
			s.append(len3);
			break;
		case 6:
			s.append(len2);
			break;
		case 7:
			s.append(len1);
			break;
		}
		s.append(lens);
		return s.toString().getBytes();
	}

	public static byte[] getOrgRowKey(byte[] rowKey) {
		long columnLength = Long.parseLong(new String(ByteUtil.subByte(rowKey,
				rowKey.length - 8, rowKey.length - 1)));
		return ByteUtil.subByte(rowKey, columnLength, rowKey.length - 9);
	}

	public static byte[] getOrgColumnValue(byte[] rowKey) {
		long columnLength = Long.parseLong(new String(ByteUtil.subByte(rowKey,
				rowKey.length - 8, rowKey.length - 1)));
		return ByteUtil.subByte(rowKey, 0, columnLength - 1);
	}

	/** {@inheritDoc} */
	public void readFields(DataInput in) throws IOException {

	}

	/** {@inheritDoc} */
	public void write(DataOutput out) throws IOException {

	}

}
