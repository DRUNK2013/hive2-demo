package com.ccssoft.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	/**
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		// ��������
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://134.175.5.79:10000/default", "zsf", "");
		// impala
		// Connection con =
		// DriverManager.getConnection("jdbc:hive2://yun10.ccssoft.com.cn:21050/;auth=noSasl");
		Statement stmt = con.createStatement();
		String tableName = "student";
		String sql = "";
		ResultSet res = null;
		// DDL����
//		 stmt.execute("drop table if exists " + tableName);
//		 stmt.execute("create table student(id string,name string) row format delimited fields terminated by ','");
//		 // �鿴���б�
		 sql = "show tables";
		 System.out.println("Running: " + sql);
		 res = stmt.executeQuery(sql);
		 if (res.next()) {
		 System.out.println(res.getString(1));
		 }
		// // �鿴����Ϣ
		// sql = "describe " + tableName;
		// System.out.println("Running: " + sql);
		// res = stmt.executeQuery(sql);
		// while (res.next()) {
		// System.out.println(res.getString(1) + "\t" + res.getString(2));
		// }

		// //�������ݣ����
//		 String filepath = "C:\\Users\\Administrator\\Desktop\\tmp\\student.txt";
//		 sql = "load data local inpath '" + filepath + "' into table " +
//		 tableName;
//		 System.out.println("Running: " + sql);
//		 stmt.execute(sql);

		// DML����
		sql = "select id from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t"
					+ res.getString(2));
		}

		// �ر�����
		stmt.close();
		con.close();
	}
}
