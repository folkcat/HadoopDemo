package com.folkcat.hadoop.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

public class HBaseInvoke {

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		System.out.println("HBase 操作开始...");
		// 1.初始化HBaseOperation
		Configuration conf = new Configuration();
		System.out.println("HBase 操作开始...2");
		// 与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同
		//conf.set("hbase.zookeeper.quorum", "hadoop");
		// 与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同
		//conf.set("hbase.zookeeper.property.clientPort", "2181");
		HBaseHelper hbase = new HBaseHelper(conf);
		System.out.println("HBase 操作开始...3");
		// 2.测试相应操作
		// 2.1创建表
		String tableName = "blog";
		hbase.deleteTable(tableName);
		String colFamilies[] = { "article", "author" };
		hbase.createTable(tableName, colFamilies);
		// 2.2插入一条记录
		hbase.insertRecord(tableName, "1", "article", "title",
				"Hadoop学习资料");
		hbase.insertRecord(tableName, "1", "author", "name",
				"hugengyong");
		hbase.insertRecord(tableName, "1", "article", "content",
				"Hadoop学习，HBase学习-http://blog.csdn.net/hugengyong");

		// 2.3查询一条记录
		Result rs1 = hbase.getOneRecord(tableName, "1");
		for (KeyValue kv : rs1.raw()) {
			System.out.println(new String(kv.getRow()));
			System.out.println(new String(kv.getFamily()));
			System.out.println(new String(kv.getQualifier()));
			System.out.println(new String(kv.getValue()));
		}
		// 2.4查询整个Table
		List<Result> list = null;
		list = hbase.getAllRecord(tableName);
		Iterator<Result> it = list.iterator();
		while (it.hasNext()) {
			Result rs2 = it.next();
			for (KeyValue kv : rs2.raw()) {
				System.out.print("row key is : "
						+ new String(kv.getRow()));
				System.out.print("family is  : "
						+ new String(kv.getFamily()));
				System.out.print("qualifier is:"
						+ new String(kv.getQualifier()));
				System.out.print("timestamp is:"
						+ kv.getTimestamp());
				System.out.println("Value  is  : "
						+ new String(kv.getValue()));
			}

		}

	}

}