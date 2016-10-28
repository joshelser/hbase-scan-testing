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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

/**
 * 
 */
public class TombstoneTest implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(TombstoneTest.class);

  private static final HColumnDescriptor F1 = new HColumnDescriptor("f1").setCompressionType(Algorithm.SNAPPY);
  private static final HColumnDescriptor Q1 = new HColumnDescriptor("q1");
  private static final HColumnDescriptor Q2 = new HColumnDescriptor("q2");
  private static final HColumnDescriptor Q3 = new HColumnDescriptor("q3");
  private static final HColumnDescriptor Q4 = new HColumnDescriptor("q4");
  private final String[] args;
  private final Random r = new Random();

  public TombstoneTest(String[] args) {
    this.args = args;
  }

  @Override public void run() {
    try {
      doRun();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void doRun() throws Exception {
    final Configuration conf = new Configuration();
    conf.addResource(new Path("/usr/local/lib/hbase/conf/hbase-site.xml"));
    final Connection conn = ConnectionFactory.createConnection(conf);
    final TableName tableName = TableName.valueOf("tombstones");
    final Admin admin = conn.getAdmin();

    log.info("Creating table");
    createTable(admin, tableName);

    final Stopwatch sw = new Stopwatch();
    final BufferedMutator bm = conn.getBufferedMutator(tableName);
    final long rowsToWrite = 1000 * 1000 * 10;
    try {
      log.info("Writing data");
      sw.start();
      writeData(bm, rowsToWrite);
      sw.stop();
      log.info("Wrote {} rows in {}ms", rowsToWrite, sw.elapsedMillis());
    } finally {
      if (null != bm) {
        bm.close();
      }
    }

    final Table table = conn.getTable(tableName);
    final long recordsToRead = 10;
    final long recordsToDelete = 1000 * 1000;
    try {
      sw.reset();
      sw.start();
      readData(table, recordsToRead);
      sw.stop();

      log.info("Took {}ms to read {} records", sw.elapsedMillis(), recordsToRead);

      for (int i = 0; i < 3; i++) {
        sw.reset();
        sw.start();
        readAndDeleteData(table, recordsToDelete);
        sw.stop();
        log.info("Took {}ms to read and delete {} records", sw.elapsedMillis(), recordsToDelete);
  
        sw.reset();
        sw.start();
        readData(table, recordsToRead);
        sw.stop();
  
        log.info("Took {}ms to read {} records", sw.elapsedMillis(), recordsToRead);
      }
    } finally {
      if (null != table) {
        table.close();
      }
    }
  }

  void createTable(Admin admin, TableName tableName) throws Exception {
    if (admin.tableExists(tableName)) {
      try {
        admin.disableTable(tableName);
      } catch (IOException e) {
        log.error("Failed to disable table", e);
      }
      try {
        admin.deleteTable(tableName);
      } catch (IOException e) {
        log.error("Failed to delete table", e);
      }
    }

    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    tableDesc.addFamily(F1);
    admin.createTable(tableDesc);
  }

  void writeData(BufferedMutator bm, long numRecords) throws Exception {
    List<Put> puts = new ArrayList<>(50);
    for (long i = 0; i < numRecords; i++) {
      Put p = new Put(("row" + i).getBytes(UTF_8));
      p.addImmutable(F1.getName(), Q1.getName(), randomData(50));
      p.addImmutable(F1.getName(), Q2.getName(), randomData(50));
      p.addImmutable(F1.getName(), Q3.getName(), randomData(50));
      p.addImmutable(F1.getName(), Q4.getName(), randomData(50));
      puts.add(p);

      if (puts.size() == 50) {
        bm.mutate(puts);
        puts.clear();
      }
    }
    if (!puts.isEmpty()) {
      bm.mutate(puts);
    }
    bm.flush();
  }

  byte[] randomData(int numBytes) {
    byte[] b = new byte[numBytes];
    r.nextBytes(b);
    return b;
  }

  void readData(Table table, long numRecords) throws Exception {
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    try {
      for (long i = 0; i < numRecords; i++) {
        Result r = scanner.next();
      }
    } finally {
      scanner.close();
    }
  }

  void readAndDeleteData(Table table, long numRecords) throws Exception {
    Scan s = new Scan();
    ResultScanner scanner = table.getScanner(s);
    try {
      List<Delete> deletes = new ArrayList<>(50);
      for (long i = 0; i < numRecords; i++) {
        Result r = scanner.next();
        deletes.add(new Delete(r.getRow()));
        if (deletes.size() == 50) {
          table.delete(deletes);
          deletes.clear();
        }
      }
      if (!deletes.isEmpty()) {
        table.delete(deletes);
      }
    } finally {
      scanner.close();
    }
  }

  public static void main(String[] args) throws Exception {
    new TombstoneTest(args).run();
  }
}
