package com.cloudera.hbase.client.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

public class HBaseMOBTest {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    if (args[0].equals("insert")) {
      insertMob(args);
    } else if (args[0].equals("insertText")) {
      for (int i = 0; i < 100; i++) {
        insertTextMob(args, i);
      }
    } else {
      readMOB(args);
    }

  }

  public static void readMOB(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    HTable table = new HTable(config, args[2]);
    Scan scan = new Scan();
    ResultScanner result = table.getScanner(scan);
    for (Result r : result) {
      byte[] b = r.getValue(Bytes.toBytes("mob"), Bytes.toBytes("c1"));
      System.out.println("--------->printing results");
      System.out.println(b);
      String out = new String(b);
      System.out.println("--------->printing results to string");
      System.out.println(out);

    }

  }
  
  public static void insertMob(String[] args) throws IOException {

    FileInputStream fis = new FileInputStream(new File(args[1]));

    byte[] b = new byte[102400];

    int read = fis.read(b);

    fis.close();

    Configuration config = HBaseConfiguration.create();

    // while (true) {
    HTable table = new HTable(config, args[2]);
    // Scan scan = new Scan();
    // System.out.println("2");;
    // ResultScanner result = table.getScanner(scan);
    // System.out.println("3");
    // for (Result r : result) {
    // System.out.println(Bytes.toString(r.getRow()));
    // }
    // try {
    // Thread.sleep(2000);
    // } catch (InterruptedException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // result.close();

    Put put = new Put(Bytes.toBytes(args[3]));
    put.add(Bytes.toBytes("mob"), Bytes.toBytes("c1"), b);
    table.put(put);
    table.flushCommits();
    table.close();

    System.out.println("completed binary put");

  }

  public static void insertTextMob(String[] args, Integer rowId) throws IOException {

    FileInputStream fis = new FileInputStream(new File(args[1]));
    
    BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
    
    String line = reader.readLine();
    
    StringBuilder builder = new StringBuilder();

    while (line != null) {
      
      builder.append(line);

      line = reader.readLine();
      
    }

    fis.close();

    reader.close();

    Configuration config = HBaseConfiguration.create();

      // while (true) {
    HTable table = new HTable(config, args[2]);
      // Scan scan = new Scan();
      // System.out.println("2");;
      // ResultScanner result = table.getScanner(scan);
      // System.out.println("3");
      // for (Result r : result) {
      // System.out.println(Bytes.toString(r.getRow()));
      // }
      // try {
      // Thread.sleep(2000);
      // } catch (InterruptedException e) {
      // // TODO Auto-generated catch block
      // e.printStackTrace();
      // }
      // result.close();
    System.out.println(">>>>>>>putting row: " + rowId);
    Put put = new Put(Bytes.toBytes(rowId));
    put.add(Bytes.toBytes("mob"), Bytes.toBytes("c1"), Bytes.toBytes(builder.toString()));
      table.put(put);
      table.flushCommits();
      table.close();

      System.out.println("completed put");
    
  }

}
