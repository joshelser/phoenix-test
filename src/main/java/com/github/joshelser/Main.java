package com.github.joshelser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

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

/**
 * 
 */
public class Main {
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int numThreads = 4;
    ExecutorService pool = Executors.newFixedThreadPool(1);
    for (int i = 0; i < numThreads; i++) {
      pool.submit(new PhxRunner());
    }
    pool.shutdown();
    pool.awaitTermination(300, TimeUnit.SECONDS);
    System.out.println("*****************************************");
    System.out.println("Waiting");
    System.out.println("*****************************************");
    Thread.sleep(500000000);
  }

  private static class PhxRunner implements Runnable {
    public void run() {
      try {
        System.out.println("Running thread");
        Connection cnxn = DriverManager.getConnection("jdbc:phoenix:hw10447.local:2181:/hbase-secure:jelserkeytab@EXAMPLE.COM:/usr/local/lib/hadoop-2.6.0/etc/secure/keytabs/jelserkeytab.service.keytab");
        Statement stmt = cnxn.createStatement();
        ResultSet results = stmt.executeQuery("SELECT * FROM SYSTEM.CATALOG");
        long numRows = 0;
        while (results.next()) {
          numRows++;
        }
        System.out.println(System.currentTimeMillis() + " Num rows: " + numRows);
        stmt.close();
        cnxn.close();
      } catch (Exception e) {
        System.out.println("Caught exception " + e.getClass() + " " + e.getMessage());
        for (StackTraceElement element : e.getStackTrace()) {
          System.out.println(element);
        }
      }
    }
  }
  
}
