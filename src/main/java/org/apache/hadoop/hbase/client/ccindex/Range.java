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

public class Range {
	private byte[] column;
	private byte[] start;
	private byte[] end;
	private byte[][] baseColumns;
	public byte[][] getBaseColumns() {
		return baseColumns;
	}
	public void setBaseColumns(byte[][] baseColumns) {
		this.baseColumns = baseColumns;
	}
	public byte[] getColumn() {
		return column;
	}
	public void setColumn(byte[] column) {
		this.column = column;
	}
	public byte[] getStart() {
		return start;
	}
	public void setStart(byte[] start) {
		this.start = start;
	}
	public byte[] getEnd() {
		return end;
	}
	public void setEnd(byte[] end) {
		this.end = end;
	}
	public String toString()
	{
		StringBuffer b=new StringBuffer();
		b.append("column is:");
		b.append(new String(this.column));
		b.append("start is:");
		if(start!=null)
		b.append(new String(this.start));
		else
			b.append("null");
		b.append("end is:");
		if(end!=null)
			b.append(new String(this.end));
			else
				b.append("null");
		return b.toString();
		
	}

}
