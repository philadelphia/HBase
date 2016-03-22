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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTableAPITest {
	private final static String TABLE_NAME = "employee";
	private static Connection connection;
	private static Configuration configuration = null;
	private static HBaseAdmin hBaseAdmin = null;
	private static HTable table = null;
	

	public static void main(String[] args) throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		init();
		table = new HTable(configuration, TABLE_NAME.getBytes());
//		createTable(TABLE_NAME, "info");
//		initTable();
//		scanTable(TABLE_NAME);
		
//		scanTable(TABLE_NAME,"1001", "1009");
//		scanTable(TABLE_NAME,"1090");
//		scanTableByFamily(TABLE_NAME,"info");
//		scanTableByQualifier(TABLE_NAME,"info","age");
//		Filter filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("1004")));
//		Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("102")));
		Filter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("age")));
		
		scanTableByFilter(TABLE_NAME, filter);
		
		
//		get(TABLE_NAME, "1010");
		closeConnecton();
		
	}

	public static void init() throws MasterNotRunningException,
			ZooKeeperConnectionException, IOException {
		connection = ConnectionFactory.createConnection();
		configuration = connection.getConfiguration();
		hBaseAdmin = (HBaseAdmin) connection.getAdmin();

	}

	public static void closeConnecton() throws IOException {
		table.close();
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

	//init a table,insert some test data to this table
	public static void initTable( ) throws IOException{
		
		List<Put> listPuts = new ArrayList<Put>();
		for (int i = 0; i < 100; i++) {
			Put put = new Put(String.valueOf(1000+i).getBytes());
			put.addColumn("info".getBytes(), "name".getBytes(), String.valueOf(i).getBytes());
			put.addColumn("info".getBytes(), "age".getBytes(), String.valueOf(i).getBytes());
			put.addColumn("info".getBytes(), "addr".getBytes(),
					("ChaoYang District"+String.valueOf(i)).getBytes());
			if(i % 2 ==0){
				put.addColumn("info".getBytes(), "gender".getBytes(), "Male".getBytes());
			}else {
				put.addColumn("info".getBytes(), "gender".getBytes(), "FeMale".getBytes());
			}
		
			put.addColumn("info".getBytes(), "salary".getBytes(),
					String.valueOf(10000+i).getBytes());
			listPuts.add(put);
		}
		
		table.put(listPuts);
		
	
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

	}

	
	public static void scanTable(String tableName) throws IOException {
		Scan scan = new Scan();
		scan.addFamily("info".getBytes());
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}
	}

	public static void scanTable(String tableName, String startKey) throws IOException {
		Scan scan = new Scan(Bytes.toBytes(startKey));
		scan.addFamily("info".getBytes());
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}
	}

	public static void scanTable(String tableName, String startKey, String endKey) throws IOException {
		Scan scan = new Scan(Bytes.toBytes(startKey), Bytes.toBytes(endKey));
		scan.addFamily("info".getBytes());
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}
	}
	
	public static void scanTableByFamily(String tableName, String family) throws IOException {
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}
	}
	
	public static void scanTableByQualifier(String tableName, String family, String qualifier) throws IOException {
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}
	}
	public static void scanTableByFilter(String tableName, Filter filter) throws IOException {
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			printResult(result);
		}
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
		List<Cell> lists = rs.listCells();
//		for (Cell cell : lists) {
//			System.out.println(new String(cell.getRowArray()));
//			System.out.println(new String(cell.getValueArray()));
//		}
		
		System.out.println("rowkey" + "\t\t" + "family:column"
				+ "\t " + "value");
		
		for (Cell cell : rs.rawCells()) {
			System.out.println(new String(CellUtil.cloneRow(cell)) + "\t\t"
					+ new String(CellUtil.cloneFamily(cell)) + ":"
					+ new String(CellUtil.cloneQualifier(cell)) + "\t"
					+ new String(CellUtil.cloneValue(cell)));
		}

	}


}
