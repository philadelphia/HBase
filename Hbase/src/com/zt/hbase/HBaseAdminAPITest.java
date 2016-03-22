package com.zt.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.stream.events.EndDocument;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.ServiceException;

/**
 * hadoop this Class is setup to test API to HBaseAdmin Class
 * @author tao.zhang  
 * 
 */
public class HBaseAdminAPITest {

	private static final String TABLENAME = "test";

	private static Connection connection;
	private static Configuration configuration;
	private static HBaseAdmin admin;

	/**
	 * @param args
	 * @throws IOException
	 * @throws ServiceException
	 */
	public static void main(String[] args) throws IOException, ServiceException {
		// TODO Auto-generated method stub
		init();
		// deleteTable(TABLENAME);
		// createTablewithPreRegions(TABLENAME);
		// printTableRegions(TABLENAME);
		checkCluster();
		printClusterStatus();

	}

	public static void init() throws IOException {
		connection = ConnectionFactory.createConnection();
		configuration = connection.getConfiguration();
		admin = (HBaseAdmin) connection.getAdmin();

	}

	public static void createTable(String tableName) throws IOException {
		HTableDescriptor descriptor = new HTableDescriptor(TABLENAME.getBytes());
		descriptor.addFamily(new HColumnDescriptor("info".getBytes()));
		admin.createTable(descriptor);
		System.out.println(admin.isTableAvailable(TABLENAME.getBytes()));
	}

	public static void createTablewithPreRegions(String tableName)
			throws IOException {
		HTableDescriptor descriptor1 = new HTableDescriptor(
				tableName.getBytes());
		descriptor1.addFamily(new HColumnDescriptor("info".getBytes()));
		admin.createTable(descriptor1, Bytes.toBytes(1L), Bytes.toBytes(100L),
				10);
	}

	public static void deleteTable(String tableName) throws IOException {
		System.out.println("deleting table " + tableName);
		admin.disableTable(tableName.getBytes());
		admin.deleteTable(tableName.getBytes());
		System.out.println(tableName + " is available"
				+ admin.isTableAvailable(tableName.getBytes()));

	}

	public static void printTableRegions(String tableName) throws IOException {
		HTable table = new HTable(configuration, tableName.getBytes());
		Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
		for (int i = 0; i < startEndKeys.getFirst().length; i++) {
			byte[] first = startEndKeys.getFirst()[i];
			byte[] second = startEndKeys.getSecond()[i];
			System.out.println("["
					+ (i + 1)
					+ "]"
					+ "StartKey:\t"
					+ (first.length == 8 ? Bytes.toLong(first) : Bytes
							.toStringBinary(first))
					+ "\t endKey:\t"
					+ (second.length == 8 ? Bytes.toLong(second) : Bytes
							.toStringBinary(second)));
		}
	}

	public static void checkCluster() throws MasterNotRunningException,
			ZooKeeperConnectionException, ServiceException, IOException {
		HBaseAdmin.checkHBaseAvailable(configuration);
	}

	public static void printClusterStatus() throws IOException {
		ClusterStatus status = admin.getClusterStatus();
		System.out.println("AverageLoad is : " + status.getAverageLoad());
		System.out.println("ClusterID is : " + status.getClusterId());
		System.out.println("Master is : " + status.getMaster());
		System.out.println("Server Number is : " + status.getServersSize());
		System.out.println("Dead Servers Number is : "
				+ status.getDeadServers());
		System.out.println("HBaseVersion is : " + status.getHBaseVersion());
		System.out.println("Region Count is : " + status.getRegionsCount());
		Collection<ServerName> servers = status.getServers();
		for (ServerName serverName : servers) {
			System.out.println("*****************************");
			printServerInfo(serverName);
			ServerLoad serverLoad = status.getLoad(serverName);
			printServerLoadInfo(serverLoad);

		}

	}

	public static void printServerInfo(ServerName serverName) {
		System.out.println("HostName is: " + serverName.getHostname());
		System.out.println("Server Port is: " + serverName.getPort());
		System.out.println("Host and Port is: " + serverName.getHostAndPort());
		System.out.println("startCode is : " + serverName.getStartcode());

	}

	public static void printServerLoadInfo(ServerLoad serverLoad) {
		System.out
				.println("ServerLoad start:-------------------------------------------------------");
		System.out.println("MemStoreSize is: "
				+ serverLoad.getMemstoreSizeInMB());
		System.out.println("MaxHeapSize is: " + serverLoad.getMaxHeapMB());
		System.out.println("UsedHeapSize is: " + serverLoad.getUsedHeapMB());
		System.out.println("Region Number is: "
				+ serverLoad.getNumberOfRegions());
		System.out.println("Load  is: " + serverLoad.getLoad());
		System.out.println("Store files is: " + serverLoad.getStorefiles());
		Map<byte[], RegionLoad> regionsLoad = serverLoad.getRegionsLoad();
		for (Entry<byte[], RegionLoad> entry : regionsLoad.entrySet()) {
			System.out.println("Region: "
					+ Bytes.toStringBinary(entry.getKey()));
			RegionLoad regionLoad = entry.getValue();
			printRegionLoadInfo(regionLoad);

		}
		System.out
				.println("ServerLoad end :-------------------------------------------------------");
	}

	public static void printRegionLoadInfo(RegionLoad regionLoad) {

		System.out.println("RegionName "
				+ Bytes.toStringBinary(regionLoad.getName()));
		System.out.println("RegionName " + regionLoad.getNameAsString());
		System.out.println("Family Number:" + regionLoad.getStores());
		System.out.println("Stores Files Number:" + regionLoad.getStorefiles());
		System.out.println("Stores files Size is:"
				+ regionLoad.getStorefileIndexSizeMB());
		System.out.println("MemStores Size is:"
				+ regionLoad.getMemStoreSizeMB());
	}
}
