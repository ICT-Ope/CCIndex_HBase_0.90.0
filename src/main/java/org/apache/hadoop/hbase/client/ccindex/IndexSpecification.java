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

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/** Holds the specification for a single CCIT. */

public class IndexSpecification implements Writable {

  // Columns that are indexed (part of the indexRowKey)

	private byte[] family;
	public byte[] getFamily() {
		return family;
	}

	public void setFamily(byte[] family) {
		this.family = family;
	}

	public byte[] getColumn() {
		return column;
	}

	public void setColumn(byte[] column) {
		this.column = column;
	}

	private byte[] column;
  private byte[][] indexedColumns;
  
  public void setIndexedColumns(byte[][] indexedColumns) {
	this.indexedColumns = indexedColumns;
}

private CCIndexDescriptor CCIndex;
  	public CCIndexDescriptor getCCIndex() {
	return CCIndex;
}

public void setCCIndex(CCIndexDescriptor cCIndex) {
	CCIndex = cCIndex;
	this.indexedColumns=cCIndex.getIndexedColumns();
}

	private  byte[] indexedColumn;
  public byte[] getIndexedColumn() {
		return indexedColumn;
	}

	public void setIndexedColumn(byte[] indexedColumn) {
		this.indexedColumn = indexedColumn;
		byte[][]fq=KeyValue.parseColumn(this.indexedColumn);
    	this.family=fq[0];
    	this.column=fq[1];
	}

// Constructs the
  private IndexKeyGenerator keyGenerator;

  // Additional columns mapped into the indexed row. These will be available for
  // filters when scanning the index.
  private byte[][] additionalColumns;

  private byte[][] allColumns;
  // Id of this index, unique within a table.
  private byte[] indexId;

  /** Construct an "simple" index spec for a single column. 
   * @param indexId 
   * @param indexedColumn
   */
  public IndexSpecification(byte[] indexId,byte[] indexedColumn, CCIndexDescriptor CCIndex) {
    
	  this(indexId,indexedColumn , null,
        new SimpleIndexKeyGenerator(), CCIndex);
    	
  }
  public IndexSpecification(byte[] indexId,byte[] indexedColumn,byte[][] additionalColumns, CCIndexDescriptor CCIndex) {
	    
	  this(indexId,indexedColumn ,additionalColumns,
        new SimpleIndexKeyGenerator(), CCIndex);
    	
  }


  /**
   * Construct an index spec by specifying everything.
   * 
   * @param indexId
   * @param indexedColumns
   * @param additionalColumns
   * @param keyGenerator
   */
  public IndexSpecification(byte[] indexId,byte[] indexedColumn,
      byte[][] additionalColumns, IndexKeyGenerator keyGenerator , CCIndexDescriptor CCIndex) {
    this.indexId = indexId;
    this.indexedColumn=indexedColumn;
    	byte[][]fq=KeyValue.parseColumn(this.indexedColumn);
    	this.family=fq[0];
    	this.column=fq[1];
    this.CCIndex=CCIndex;
    this.indexedColumns=CCIndex.getIndexedColumns();
    this.keyGenerator = keyGenerator;
    this.additionalColumns = (additionalColumns == null)? new byte[0][0] :
        additionalColumns;
    this.makeAllColumns(); 

    
  }

  public IndexSpecification() {
    // For writable
  }

  private void makeAllColumns() {
    this.allColumns = new byte[indexedColumns.length
        + (additionalColumns == null ? 0 : additionalColumns.length)][];
    System.arraycopy(indexedColumns, 0, allColumns, 0, indexedColumns.length);
    if (additionalColumns != null) {
      System.arraycopy(additionalColumns, 0, allColumns, indexedColumns.length,
          additionalColumns.length);
    }
  }
  
  /**
   * Get the indexedColumns.
   * 
   * @return Return the indexedColumns.
   */
  public byte[][] getIndexedColumns() {
    return indexedColumns;
  }

  /**
   * Get the keyGenerator.
   * 
   * @return Return the keyGenerator.
   */
  public IndexKeyGenerator getKeyGenerator() {
    return keyGenerator;
  }

  /**
   * Get the additionalColumns.
   * 
   * @return Return the additionalColumns.
   */
  public byte[][] getAdditionalColumns() {
    return additionalColumns;
  }

  /**
   * Get the indexId.
   * 
   * @return Return the indexId.
   */
  public byte[] getIndexId() {
    return indexId;
  }

  public byte[][] getAllColumns() {
    return allColumns;
  }

  public boolean containsColumn(byte[] column) {
    for (byte[] col : allColumns) {
      if (Bytes.equals(column, col)) {
        return true;
      }
    }
    return false;
  }

  public byte[] getIndexedTableName(byte[] baseTableName) {
    return Bytes.add(baseTableName, Bytes.toBytes("_" + indexId));
  }

  private static final HBaseConfiguration CONF = new HBaseConfiguration();
  
  /** {@inheritDoc} */
  	public static void printBytes(byte[]s)
  	{
//  		String out="";
//  		for(byte b:s)
//  		{
//  			out+=","+b;
//  			
//  			
//  		}
//  	System.out.println(out);
  	}
  public void readFields(DataInput in) throws IOException {
   
	  indexId = Bytes.readByteArray(in);
	  indexedColumn = Bytes.readByteArray(in);
		byte[][]fq=KeyValue.parseColumn(this.indexedColumn);
    	this.family=fq[0];
    	this.column=fq[1];
	  //System.out.println("read:");
	  //this.printBytes(indexedColumn);
	  //System.out.println(new String(indexedColumn));
    int numAdditionalCols = in.readInt();
    additionalColumns = new byte[numAdditionalCols][];
    for (int i = 0; i < numAdditionalCols; i++) {
      additionalColumns[i] = Bytes.readByteArray(in);
    }
    makeAllColumns();
    keyGenerator = (IndexKeyGenerator) ObjectWritable.readObject(in, CONF);
    
    // FIXME this is to read the deprecated comparator, in existing data
    ObjectWritable.readObject(in, CONF);
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
	  Bytes.writeByteArray(out, this.indexId);
	  Bytes.writeByteArray(out, this.indexedColumn);
	  //System.out.println("write:");
	  //System.out.println(indexedColumn);
	  //System.out.println(new String(indexedColumn));
    if (additionalColumns != null) {
      out.writeInt(additionalColumns.length);
      for (byte[] col : additionalColumns) {
        Bytes.writeByteArray(out, col);
      }
    } else {
      out.writeInt(0);
    }
    ObjectWritable
        .writeObject(out, keyGenerator, IndexKeyGenerator.class, CONF);
    
    // FIXME need to maintain this for exisitng data
    ObjectWritable.writeObject(out, null, WritableComparable.class,
        CONF);
  }

  /** {@inheritDoc} */
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ID => ");
    sb.append(indexId);
    return sb.toString();
  }

  public boolean equals(Object arg0)
  	{
	  	if(arg0 instanceof IndexSpecification)
	  	{
	  		IndexSpecification index=(IndexSpecification)arg0;
	  		if(index.CCIndex==null)
	  		{
	  			
	  			if(index.indexId.equals(this.indexId))
	  				return true;
	  		}
	  			if(index.indexId.equals(this.indexId)&&this.CCIndex.getBaseTableDescriptor().getName().equals(index.CCIndex.getBaseTableDescriptor().getName()))
	  				return true;
	  	}
	  	return false;
  	}

public byte[] getCCITName() {		
	return Bytes.add(this.CCIndex.getBaseTableDescriptor().getName(),Bytes.toBytes("-"),this.indexId);
	// TODO Auto-generated method stub

}	
public static byte[] getCCTNameBase(byte[] baseName) {
	
	return Bytes.add(baseName,Bytes.toBytes("-"),Bytes.toBytes(CCIndexConstants.CCT_TAIL));
	// TODO Auto-generated method stub

}	
public byte[] getCCTName() {
	
	return Bytes.add(this.getCCITName(),Bytes.toBytes("-"),Bytes.toBytes(CCIndexConstants.CCT_TAIL));
	// TODO Auto-generated method stub

}	
  
  
}
