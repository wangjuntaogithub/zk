package com.itcast.zkclient;

import java.util.List;

import org.I0Itec.zkclient.DataUpdater;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestZKclient {
	private ZkClient zkClient = null;

	/**
	 * 创建zookeeper连接
	 */
	@Before
	public void connection() {
		// zookeeper地址和超时时间
		zkClient = new ZkClient("slave1:2181,slave2:2181,slave3:2181", 2000);
	}

	/**
	 * 关闭zookeeper连接
	 */
	@After
	public void close() {
		zkClient.close();
	}

	/**
	 * 创建节点
	 */
	@Test
	public void testCreateZnode() {
		zkClient.create("/test/test", "abc", CreateMode.PERSISTENT_SEQUENTIAL);
	}

	/**
	 * 删除节点
	 */
	@Test
	public void testDeleteZnode() {
		zkClient.delete("/test");
	}

	/**
	 * 更新节点
	 */
	@Test
	public void testUpdateZnode() {
		zkClient.updateDataSerialized("/test", new DataUpdater<String>() {
			@Override
			public String update(String currentData) {
				// 返回之前znode的data
				System.out.println(currentData);
				// 设置新的data
				return "bbbbbbb";
			}
		});
	}

	/**
	 * 查询创建时间
	 */
	@Test
	public void testCreationTime() {
		long creationTime = zkClient.getCreationTime("/test");
		System.out.println(creationTime);
	}

	/**
	 * 查询节点内容
	 */
	@Test
	public void testGetData() {
		String readData = zkClient.readData("/test", true);
		System.out.println(readData);
	}

	/**
	 * 查询子节点 统计子节点个数
	 */
	@Test
	public void testChild() {
		int countChildren = zkClient.countChildren("/test");
		System.out.println("/test共有" + countChildren + "个子节点！");
		List<String> children = zkClient.getChildren("/test");
		for (String string : children) {
			System.out.println(string);
		}
	}

	/**
	 * 注册节点更改监听
	 */
	@Test
	public void testDataChangesListener() {
		zkClient.subscribeDataChanges("/test", new IZkDataListener() {
			@Override
			public void handleDataDeleted(String dataPath) throws Exception {
				System.out.println("节点被删除！");

			}

			@Override
			public void handleDataChange(String dataPath, Object data)
					throws Exception {
				System.out.println("节点被更改！");
			}
		});
		for (int i = 0; i < 5; i++) {
			zkClient.updateDataSerialized("/test", new DataUpdater<String>() {
				@Override
				public String update(String currentData) {
					return "aaaaaaaaaaaa";
				}
			});
		}
	}

	/**
	 * 注册子节点改变监听
	 * 
	 * @throws InterruptedException
	 */
	@Test
	public void testChildChangesListener() throws InterruptedException {
		zkClient.subscribeChildChanges("/test", new IZkChildListener() {
			@Override
			public void handleChildChange(String parentPath,
					List<String> currentChilds) throws Exception {
				System.out.println(parentPath + "子节点被修改！");
				for (String string : currentChilds) {
					System.out.println("现在为" + string);
				}
			}
		});
		for (int i = 0; i < 5; i++) {
			zkClient.create("/test/test", "abc",
					CreateMode.EPHEMERAL_SEQUENTIAL);
			Thread.sleep(100);
		}
	}
}
