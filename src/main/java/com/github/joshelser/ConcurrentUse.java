package com.github.joshelser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * 
 */
public class ConcurrentUse {
  public static final String userA = "jdbc:phoenix:hw10447.local:2181:/hbase-secure:jelserkeytab@EXAMPLE.COM:/usr/local/lib/hadoop-2.6.0/etc/secure/keytabs/jelserkeytab.service.keytab";
  public static final String userB = "jdbc:phoenix:hw10447.local:2181:/hbase-secure:jelser2@EXAMPLE.COM:/usr/local/lib/hadoop-2.6.0/etc/secure/keytabs/jelser2.headless.keytab";

  public static final CountDownLatch latchA  = new CountDownLatch(1);
  public static final CountDownLatch latchB  = new CountDownLatch(1);

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    pool.submit(new PhxRunner());
    pool.submit(new PhxRunnerBlockThenRepeat());
    pool.shutdown();
    pool.awaitTermination(5, TimeUnit.SECONDS);
  }

  private static class PhxRunner implements Runnable {
    public void run() {
      try {
        latchA.await();
        String url = userB;
        System.out.println("****** USER B *********** Running thread with " + url);
        Connection cnxn = DriverManager.getConnection(url);
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
      latchB.countDown();
    }
  }

  private static class PhxRunnerBlockThenRepeat implements Runnable {
    public void run() {
      try {
        String url = userA;
        System.out.println("********** USER A ************ Running thread with " + url);
        Connection cnxn = DriverManager.getConnection(url);
        Statement stmt = cnxn.createStatement();
        ResultSet results = stmt.executeQuery("SELECT * FROM SYSTEM.CATALOG");
        long numRows = 0;
        while (results.next()) {
          numRows++;
        }
        System.out.println(System.currentTimeMillis() + " Num rows: " + numRows);
        stmt.close();

        latchA.countDown();
        latchB.await();

        // Try to reuse the connection
        System.out.println("************* USER A ************** Running thread with " + url);
        stmt = cnxn.createStatement();
        results = stmt.executeQuery("SELECT * FROM SYSTEM.CATALOG");
        numRows = 0;
        while (results.next()) {
          numRows++;
        }
        System.out.println(System.currentTimeMillis() + " Num rows: " + numRows);
        stmt.close();
      } catch (Exception e) {
        System.out.println("Caught exception " + e.getClass() + " " + e.getMessage());
        for (StackTraceElement element : e.getStackTrace()) {
          System.out.println(element);
        }
      }
    }
  }
}
