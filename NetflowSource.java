package edu.cu.boulder.cs.flink.triangles;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;


import java.time.Instant;
import java.util.Random;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
/**
 * This creates netflows from a pool of IPs.
 */
public class NetflowSource extends RichParallelSourceFunction<Netflow> {
  private String filename;
  public long unix_secs; //0
  public String srcaddr; //10
  public String dstaddr; //11

  public NetflowSource(String filename)
  {
    this.filename = filename;
  /*  try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
      String line;
      String delimnator = ",";
      while ((line = br.readLine()) != null) {
        String[] liny = line.split(delimnator);
        Netflow netflow = new Netflow(
        Long.parseLong(liny[0]),
        liny[10],
        liny[11]
        );
      }
        br.close();
} catch (FileNotFoundException e) {
          e.printStackTrace();
      } catch (IOException e) {
          e.printStackTrace();
      }
  */
  }
  @Override
  public void run(SourceContext<Netflow> out) throws Exception
  {
    try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
      String line;
      String delimnator = ",";
      while ((line = br.readLine()) != null) {
        String[] liny = line.split(delimnator);
        Netflow netflow = new Netflow(
        Long.parseLong(liny[0]),
        liny[10],
        liny[11]
        );
      
       // br.close();
      
        out.collectWithTimestamp(netflow, unix_secs);
        out.emitWatermark(new Watermark(unix_secs));
      }
      }
    }
      @Override
  public void cancel()
  {
      //;
  }
}