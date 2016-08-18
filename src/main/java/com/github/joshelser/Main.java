package com.github.joshelser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * 
 */
public class Main {
  public static final List<String> JDBC_URLS = Arrays.asList("jdbc:phoenix:hw10447.local:2181:/hbase-secure:jelserkeytab@EXAMPLE.COM:/usr/local/lib/hadoop-2.6.0/etc/secure/keytabs/jelserkeytab.service.keytab",
      "jdbc:phoenix:hw10447.local:2181:/hbase-secure:jelser2@EXAMPLE.COM:/usr/local/lib/hadoop-2.6.0/etc/secure/keytabs/jelser2.headless.keytab");
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int numThreads = 16;
//    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
//    System.out.println(ugi);
//    System.out.println(ugi.isFromKeytab());
//    System.out.println(UserGroupInformation.isLoginKeytabBased());
//    System.out.println(UserGroupInformation.isLoginTicketBased());
    ExecutorService pool = Executors.newFixedThreadPool(1);
    for (int i = 0; i < numThreads; i++) {
      pool.submit(new PhxRunner());
    }
    pool.shutdown();
    pool.awaitTermination(300, TimeUnit.SECONDS);
//    System.out.println("*****************************************");
//    System.out.println("*****************************************");
//    ugi = UserGroupInformation.getLoginUser();
//    System.out.println(ugi);
//    System.out.println(ugi.isFromKeytab());
//    System.out.println(UserGroupInformation.isLoginKeytabBased());
//    System.out.println(UserGroupInformation.isLoginTicketBased());
    System.out.println("*****************************************");
    System.out.println("Waiting");
    System.out.println("*****************************************");
    Thread.sleep(500000000);
  }

  private static class PhxRunner implements Runnable {
    private static final Random rand = new Random();
    public void run() {
      try {
        String url = JDBC_URLS.get(rand.nextInt(JDBC_URLS.size()));
        System.out.println("Running thread with " + url);
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
    }
  }
  
}
