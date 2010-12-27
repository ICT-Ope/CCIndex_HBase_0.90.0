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
import org.apache.hadoop.hbase.client.Result;

public class Utilities {
	
	public static long[] disRowKey(byte []ai,byte[] bi)
	 {
		 
		 long indexDis=0;
		 long numDis = 0;
		 int i=0,j=0,index = 0;
		 byte []a=null;
		 byte [] b=null;
		 if(ai!=null)
		 {
			 a=SimpleIndexKeyGenerator.getOrgColumnValue(ai);
		 }
		 if(bi!=null)
		 {
			 b=SimpleIndexKeyGenerator.getOrgColumnValue(bi);
		 }
		 if(a!=null&&b!=null)
		 {
			 if(a.length<b.length)
			 {
				 a=fillLength(a,b);
			 }
			 else if(a.length>b.length)
			 {
				 b=fillLength(a,b);
			 }
		 }
		 if(a==null||b==null)
		 {
			 if(a==null&&b!=null)
			 {
				 for(;j>=0;j--,index++)
				 {
					 int bn = (b[j] & 0xff);
					 if(bn!=0)
					 { indexDis=index;
					 	  numDis=Math.abs(bn);
					 }
				 }
				 return new long[]{indexDis,numDis};
				 
			 }
			 else if(a!=null&&b==null)
			 {
				 for(;i>=0;i--,index++)
				 {
					 int an = (a[i] & 0xff);
					 if(an!=0)
					 { 
						 indexDis=index;
						 numDis=Math.abs(an);
					 }
				 }
				 return new long[]{indexDis,numDis};
			 }
			 else
			 {
				 return new long[]{0,0};
			 }
		 }
		 for (i = a.length-1, j = b.length-1,index=0; i >=0&& j >=0; i--, j--,index++) {
		      int an = (a[i] & 0xff);
		      int bn = (b[j] & 0xff);
		      if (an != bn) {
		    	  	indexDis=index;
		    	  	numDis=Math.abs(an-bn);
		      }
		 }
		 if(i>=0)
		 { 
			 for(;i>=0;i--,index++)
			 {
				 int an = (a[i] & 0xff);
				 if(an!=0)
				 { 
					 indexDis=index;
					 numDis=Math.abs(an);
				 }
			 }
		 }
		 else if(j>=0)
		 {
			 for(;j>=0;j--,index++)
			 {
				 int bn = (b[j] & 0xff);
				 if(bn!=0)
				 { indexDis=index;
				 	 numDis=Math.abs(bn);
				 }
			 }
		 }
		 return new long[]{indexDis,numDis};
	 }
	 public static byte[] fillLength(byte []a,byte b[])
	  {
		 byte [] ret;
		 byte [] sh=a.length>b.length?b:a;
		 byte [] lo=a.length>b.length?a:b;
		 
			  ret=new byte[lo.length];
			  for(int i=0;i<ret.length;i++)
			  {
				  if(i<sh.length)
				  {
					  ret[i]=sh[i];
				  }
				  else
				  {
					  ret[i]=(byte)48;
				  }
			  }
			  return ret;
		 
	  }
	 public static byte[] getValue(Result r,byte[] columns)
	 {
		 byte[][] fq=KeyValue.parseColumn(columns);
		 return r.getValue(fq[0],fq[1]);
		 
	 }
	 public static long[] dis(byte []a1,byte[] b1)
	 {
		 
		 long indexDis=0;
		 long numDis = 0;
		 int i=0,j=0,index = 0;
		byte [] a=a1;
		byte [] b=b1;
		 if(a!=null&&b!=null)
		 {
			 if(a.length<b.length)
			 {
				 a=fillLength(a,b);
			 }
			 else if(a.length>b.length)
			 {
				 b=fillLength(a,b);
			 }
		 }
		
		 if(a==null||b==null)
		 {
			 if(a==null&&b!=null)
			 {
				 for(;j>=0;j--,index++)
				 {
					 int bn = (b[j] & 0xff);
					 if(bn!=0)
					 { indexDis=index;
					 	  numDis=Math.abs(bn);
					 }
				 }
				 return new long[]{indexDis,numDis};
				 
			 }
			 else if(a!=null&&b==null)
			 {
				 for(;i>=0;i--,index++)
				 {
					 int an = (a[i] & 0xff);
					 if(an!=0)
					 { 
						 indexDis=index;
						 numDis=Math.abs(an);
					 }
				 }
				 return new long[]{indexDis,numDis};
			 }
			 else
			 {
				 return new long[]{0,0};
			 }
		 }
		 for (i = a.length-1, j = b.length-1,index=0; i >=0&& j >=0; i--, j--,index++) {
		      int an = (a[i] & 0xff);
		      int bn = (b[j] & 0xff);
		      if (an != bn) {
		    	  	indexDis=index;
		    	  	numDis=Math.abs(an-bn);
		      }
		 }
		 if(i>=0)
		 { 
			 for(;i>=0;i--,index++)
			 {
				 int an = (a[i] & 0xff);
				 if(an!=0)
				 { 
					 indexDis=index;
					 numDis=Math.abs(an);
				 }
			 }
		 }
		 else if(j>=0)
		 {
			 for(;j>=0;j--,index++)
			 {
				 int bn = (b[j] & 0xff);
				 if(bn!=0)
				 { indexDis=index;
				 	 numDis=Math.abs(bn);
				 }
			 }
		 }
		 return new long[]{indexDis,numDis};
	 }


}
