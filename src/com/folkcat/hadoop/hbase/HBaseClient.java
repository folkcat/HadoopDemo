package com.folkcat.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * Hbase Java接口
 */
public class HBaseClient {
	public static void main(String[] args)throws IOException{
		Configuration config=HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "localhost");//使用eclipse时必须添加这个，否则无法定位
		config.set("hbase.rootdir","hdfs://localhost:9000/hbase");
		
		//创建表
		HBaseAdmin admin=new HBaseAdmin(config);
		HTableDescriptor htd=new HTableDescriptor("htd");
		HColumnDescriptor hcd=new HColumnDescriptor("hcd");
		htd.addFamily(hcd);
		admin.createTable(htd);
		byte[] tableName=htd.getName();
		HTableDescriptor[] tables=admin.listTables();
		if(tables.length!=1&&Bytes.equals(tableName, tables[0].getName())){
			throw new IOException("Failed created of table");
		}
		//运行操作，
		HTable table=new HTable(config,tableName);
		byte[] row1=Bytes.toBytes("row1");
		Put p1=new Put(row1);
		byte[] dataBytes=Bytes.toBytes("data");
		p1.add(dataBytes,Bytes.toBytes("1"),Bytes.toBytes("value1"));
		table.put(p1);
		Get g=new Get(row1);
		Result result=table.get(g);
		System.out.println("Get:"+result);
	}
}
