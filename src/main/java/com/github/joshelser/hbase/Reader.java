/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.joshelser.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class Reader {
  private static final Logger log = LoggerFactory.getLogger(Reader.class);

  private static final byte[] FAMILY = Bytes.toBytes("f1");
  private static final byte[] QUAL1 = Bytes.toBytes("q1");
  private static final byte[] QUAL2 = Bytes.toBytes("q2");
  private static final byte[] VALUE1 = Bytes.toBytes("f1:q1");
  private static final byte[] VALUE2 = Bytes.toBytes("f1:q2");
  
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.rootdir", "hdfs://hw10447.local:8020/hbase-1.1.2.2.4.2");
    conf.set("zookeeper.znode.parent", "/hbase-1.1.2.2.4.2");

    Connection conn = ConnectionFactory.createConnection(conf);

    TableName name = TableName.valueOf("foo");

    if (!conn.getAdmin().tableExists(name)) {
      log.info("Table does not yet exist");

      // Create table
      log.info("Creating table");
      createTable(conn, name);

      // Write data
      log.info("Populating table");
      writeData(conn, name);
    } else {
      log.info("Table exists, not recreating");
    }

    log.info("Counting records in table");
    count(conn, name);
  }

  private static void createTable(Connection conn, TableName name) throws Exception {
    HTableDescriptor tableDesc = new HTableDescriptor(name);
    tableDesc.addFamily(new HColumnDescriptor(FAMILY));
    conn.getAdmin().createTable(tableDesc);
  }

  private static void writeData(Connection conn, TableName name) throws Exception {
    try (BufferedMutator writer = conn.getBufferedMutator(name)) {
      for (int i = 0; i < 1000 * 1000; i++) {
        Put p = new Put(Bytes.toBytes(i));
        for (int j = 0; j < 2; j++) {
          p.addColumn(FAMILY, QUAL1, VALUE1);
          p.addColumn(FAMILY, QUAL2, VALUE2);
        }
        writer.mutate(p);
      }
    }
  }

  private static void count(Connection conn, TableName name) throws Exception {
    try (Table table = conn.getTable(name)) {
      Scan scan = new Scan();
      scan.addFamily(FAMILY);
      scan.addColumn(FAMILY, QUAL1);
      scan.addColumn(FAMILY, QUAL2);
      scan.setCacheBlocks(true);
      scan.setCaching(1);
      scan.setBatch(1);
      ResultScanner scanner = table.getScanner(scan);
      Result result = null;
      long numCells = 0;
      long numRows = 0;
      while (null != (result = scanner.next())) {
        numRows++;
        numCells += result.listCells().size();
      }
      System.out.println("numRows=" + numRows + ", numCells=" + numCells + " for " + name);
    }
  }
}
