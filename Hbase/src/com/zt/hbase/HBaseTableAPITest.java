package com.zt.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableAPITest {
	private final static String TABLE_NAME = "employee";
	public static Configuration configuration = null;
	public static HBaseAdmin hBaseAdmin = null;

	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		// isTableExists("test");
		// createTable("employee", "info");
//		isTableExists("employee");
//		printConfiguration();
		// addFamilyToTable("Carrer");
		// deleteFamilyToTable("Carrer");
		// insert("employee", "002");
//		 get("employee", "002");
		// delete("employee","001");
		// query("employee", "002");
		// insertInBatch(tableName);
		// deleteInBatch(tableName);
		// get(tableName, "001");
		// getInBatch(tableName);
		// filterWithFamily("rk");
		// scanTable(tableName);
		isAutoFlush();
		HTable table = new HTable(configuration, TABLE_NAME.getBytes());
		HTableDescriptor des = table.getTableDescriptor();
		System.out.println(des.getNameAsString());
//		des.addFamily(new HColumnDescriptor("test".getBytes()));
//		hBaseAdmin.enableTable(TABLE_NAME.getBytes());
//		hBaseAdmin.modifyTable(TABLE_NAME.getBytes(), des);
//		HColumnDescriptor[] dess = des.getColumnFamilies();
//		for (HColumnDescriptor hColumnDescriptor : dess) {
//			System.out.println(hColumnDescriptor);
//		}
//		System.out.println(des.getFamily("info".getBytes()));
//		System.out.println(des.hasFamily("test".getBytes()));
//		des.setReadOnly(false);
		System.out.println(des.isReadOnly());
		 insert("employee", "032");
		 System.out.println(des.getMemStoreFlushSize());
		closeConnecton();
		
	}

	public static void init() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		configuration = HBaseConfiguration.create();
		hBaseAdmin = new HBaseAdmin(configuration);

	}

	public static void closeConnecton() throws IOException {
		hBaseAdmin.close();
	}

	public static void createTable(String tableName, String familyName)
			throws IOException {
		init();
		HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(
				familyName.getBytes());
		hTableDescriptor.addFamily(columnDescriptor);
		boolean flag = hBaseAdmin.tableExists(tableName.getBytes());
		if (flag) {
			System.out.println(tableName + "is exists");
		} else {
			hBaseAdmin.createTable(hTableDescriptor);
		}

		closeConnecton();

	}

	public static void deleteTable(String tableName)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		init();
		if (hBaseAdmin.tableExists(Bytes.toBytes(tableName))) {
			hBaseAdmin.disableTable(Bytes.toBytes(tableName));
			hBaseAdmin.deleteTable(tableName);

		} else {
			System.out.println(tableName + "does not exist");
		}

	}

	public static void addFamilyToTable(String columnFamilyName)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		init();
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(
				columnFamilyName.getBytes());
		hBaseAdmin.addColumn(TABLE_NAME, columnDescriptor);
		closeConnecton();

	}

	public static void deleteFamilyToTable(String columnFamilyName)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {
		init();
		HTableDescriptor tableDescriptor = hBaseAdmin
				.getTableDescriptor(TABLE_NAME.getBytes());
		tableDescriptor.removeFamily(columnFamilyName.getBytes());
		hBaseAdmin.modifyTable(TABLE_NAME.getBytes(), tableDescriptor);
		closeConnecton();

	}

	public static HTable getTable(String tableName) throws IOException {
		init();
		HTable hTable = new HTable(configuration, Bytes.toBytes(tableName));
		return hTable;

	}

	public static void isTableExists(String tableName) throws IOException {
		init();
		boolean flag = hBaseAdmin.tableExists(tableName.getBytes());
		if (flag) {
			System.out.println(tableName + " exists");
		} else {
			System.out.println(tableName + " does not  exists");
		}

	}

	public static void insert(String tableName, String rowkey)
			throws IOException {
		HTable hTable = getTable(tableName);
		Put put = new Put(rowkey.getBytes());
		put.addColumn("info".getBytes(), "name".getBytes(), "zt".getBytes());
		put.addColumn("info".getBytes(), "age".getBytes(), "18".getBytes());
		put.addColumn("info".getBytes(), "addr".getBytes(),
				"ChaoYang District".getBytes());
		put.addColumn("info".getBytes(), "gender".getBytes(), "Male".getBytes());
		put.addColumn("info".getBytes(), "salary".getBytes(),
				"10000".getBytes());
		hTable.put(put);
		hTable.close();
	}

	public static void insertInBatch(String tableName) throws IOException {
		HTable hTable = getTable(tableName);
		List<Put> lists = new ArrayList<Put>();
		Put put1 = new Put("rk001".getBytes());
		put1.addColumn("info".getBytes(), "name".getBytes(), "zt".getBytes());
		put1.addColumn("info".getBytes(), "age".getBytes(), "18".getBytes());
		put1.addColumn("info".getBytes(), "addr".getBytes(),
				"ChaoYang District".getBytes());
		put1.addColumn("info".getBytes(), "gender".getBytes(),
				"Male".getBytes());
		put1.addColumn("info".getBytes(), "salary".getBytes(),
				"10000".getBytes());
		lists.add(put1);
		Put put2 = new Put("rk002".getBytes());
		put2.addColumn("info".getBytes(), "name".getBytes(), "godie".getBytes());
		put2.addColumn("info".getBytes(), "age".getBytes(), "18".getBytes());
		put2.addColumn("info".getBytes(), "addr".getBytes(),
				"ChaoYang District".getBytes());
		put2.addColumn("info".getBytes(), "gender".getBytes(),
				"FeMale".getBytes());
		put2.addColumn("info".getBytes(), "salary".getBytes(), "100".getBytes());
		lists.add(put2);

		hTable.put(lists);

		hTable.close();
	}

	public static void delete(String tableName, String rowkey)
			throws IOException {
		HTable hTable = getTable(tableName);
		Delete delete = new Delete(rowkey.getBytes());
		delete.addColumn("info".getBytes(), "name".getBytes());
		hTable.delete(delete);
		hTable.close();

	}

	public static void deleteInBatch(String tableName) throws IOException {
		HTable hTable = getTable(tableName);
		List<Delete> listsDeletes = new ArrayList<Delete>();
		Delete delete1 = new Delete("rk001".getBytes());
		Delete delete2 = new Delete("rk002".getBytes());

		listsDeletes.add(delete1);
		listsDeletes.add(delete2);

		hTable.delete(listsDeletes);
		hTable.close();
		for (Delete delete : listsDeletes) {
			byte[] row = delete.getRow();
			System.out.println("delete: " + new String(row));
		}

	}

	@SuppressWarnings("deprecation")
	public static void get(String tableName, String rowkey) throws IOException {
		HTable hTable = getTable(tableName);
		Get get = new Get(rowkey.getBytes());
		// get.addColumn("info".getBytes(), "age".getBytes());
		get.addFamily("info".getBytes());
		// get.addColumn("info".getBytes(), "name".getBytes());
		// get.addColumn("info".getBytes(), "age".getBytes());
		Result result = hTable.get(get);
		printResult(result);
		hTable.close();
	}

	public static void getInBatch(String tableName) throws IOException {
		HTable hTable = getTable(tableName);
		List<Get> listsGets = new ArrayList<Get>();
		Get get1 = new Get("rk001".getBytes());
		Get get2 = new Get("rk002".getBytes());
		listsGets.add(get1);
		listsGets.add(get2);

		Result[] results = hTable.get(listsGets);
		for (Result result : results) {
			printResult(result);
		}

		hTable.close();
	}

	public static void scanTable(String tableName) throws IOException {
		HTable hTable = getTable(tableName);
		Scan scan = new Scan();
		scan.addFamily("info".getBytes());
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}

		hTable.close();
	}

	public static void filterByRowKey(String filterName) throws IOException {
		HTable table = getTable(TABLE_NAME);
		Scan scan = new Scan();
		Filter filter = new PrefixFilter(filterName.getBytes());
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}

	}

	public static void filterWithFamily(String filterName) throws IOException {
		HTable table = getTable(TABLE_NAME);
		Scan scan = new Scan();
		// Filter filter = new Fam(filterName.getBytes());
		// scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}

	}

	public static void filterByRo2wKey(String filterName) throws IOException {
		HTable table = getTable(TABLE_NAME);
		Scan scan = new Scan();
		Filter filter = new PrefixFilter(filterName.getBytes());
		scan.setFilter(filter);

		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}

	}

	public static void printResult(Result rs) {
		System.out.println("rowkey is :" + new String(rs.getRow()));
		System.out.println("rowkey is :" + rs.size());
		System.out.println(new String(rs.getValue("info".getBytes(), "age".getBytes())));
		List<Cell> lists = rs.listCells();
		for (Cell cell : lists) {
			System.out.println(new String(cell.getRowArray()));
			System.out.println(new String(cell.getValueArray()));
		}
		
//		System.out.println("rowkey" + "\t\t" + "family:column"
//				+ "\t " + "value");
//		
//		for (Cell cell : rs.rawCells()) {
//			System.out.println(new String(CellUtil.cloneRow(cell)) + "\t\t"
//					+ new String(CellUtil.cloneFamily(cell)) + ":"
//					+ new String(CellUtil.cloneQualifier(cell)) + "\t"
//					+ new String(CellUtil.cloneValue(cell)));
//		}

	}

	public static void printConfiguration() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		init();
		System.out.println(configuration.get("hbase.master.maxclockskew"));
		

	}
	
	private static void isAutoFlush() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		init();
		HTable table = getTable(TABLE_NAME);
		table.setAutoFlush(false);
		System.out.println(table.isAutoFlush());
		
	}
}
