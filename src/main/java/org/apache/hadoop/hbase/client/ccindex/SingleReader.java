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
import java.util.Vector;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * 
 * @author liu jia liujia09@software.ict.ac.cn
 *
 */
public class SingleReader {
	private ResultScanner scanner;
	private Range[] range;
	private int rowN=100;
	private boolean over=false;
	public SingleReader(ResultScanner scanner, Range[] range)
	{
		this.scanner=scanner;
		this.range=range;
	}

	public Vector<Result> next() throws IOException
	{
		Result[] rs=null;
	
		Vector<Result>ret=new Vector<Result>();
		while((rs=scanner.next(rowN))!=null&&rs.length!=0)
		{
			
			for(Result r:rs)
			{
				boolean discard=false;
				for(Range one:range)
				{
					
					byte [] value=Utilities.getValue(r,one.getColumn());

					if(value==null)
					{
						discard=true;
						break;
					}
					if(one.getStart()!=null)
					{
						if(Bytes.compareTo(value, one.getStart())<0)
						{
							discard=true;
							break;
						}
					}
					if(one.getEnd()!=null)
					{
						if(Bytes.compareTo(one.getEnd(), value)<0)
						{
							discard=true;
							break;
						}
					}
				}
				if(!discard)
					ret.add(r);
			}
			
			return ret;
			
		}
		this.over=true;
		return ret;
		
	}
	public boolean isOver()
	{
		return this.over;
	}

}
