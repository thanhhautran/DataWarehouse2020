package loadFromStagingToWarehouse;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.collections4.map.HashedMap;
import connectionDatabase.BaseConnection;
import updateLogAndConfig.UpdateLog;

public class StagingToWarehouse {
	public void part3() throws SQLException {
//		Map<String, String> map = getSTReadyToWarehouse(1);
//		LoadToWarehouse(map);
	}
	public Map<String,String> getSTReadyToWarehouse(int id) {
		//phuong thuc nay nhan vao mot id can lay ra mot config lưu vào map ,phuong thuc tra ve mot map
		Map<String, String> mapResult = new HashedMap<String, String>();
		//tao map dung de luu tru cac config theo ten
        int idConfig =0;//1
        String username = "";//2
        String password ="";//3
//        String remoteDir="";//4
//        String port ="";//5
        String filepattern ="";//6
        String des_config ="";//7
		String fileType = "";//8
		String delimiter = "";//9
		String listColumn = "";//10
		String listWarehouseRequireCol = "";//11
		String Staging_tabName = "";//12
		int colNum = 0;//13
		String warehouseTabName ="";//14
		String warehouseColumn ="";//15
		String staging_naturalKey ="";//16
		String warehouse_naturalKey ="";//17
		String warehouse_des ="";//18
		String procedureName="";//19
		String localdir ="";
		//cac bien duoc khai dai dien cho cai column trong config
		try {
			Connection connection_user = BaseConnection.getMySQLConnection();
			connection_user.setAutoCommit(false);
			PreparedStatement stat = connection_user.prepareStatement("select * from configtable c, logtab l where c.idConfig = l.idConfig and l.status_file = 'success_to_staging' and l.idconfig="+id+" limit 1;");
			stat.execute();
			ResultSet rs =  stat.getResultSet();
			while(rs.next()) {
				idConfig =rs.getInt("idConfig");
		        username = rs.getString("user");
		        password =rs.getString("password");
//		        remoteDir = rs.getString("remote_Dir");
//		        port = rs.getString("port");
		        filepattern =rs.getString("file_pattern");
		        des_config =rs.getString("des_config");
				fileType = rs.getString("file_type");
				delimiter = rs.getString("delimiter");
				listColumn = rs.getString("list_column");
				listWarehouseRequireCol = rs.getString("list_WarehouseRequiredColumn");
				Staging_tabName = rs.getString("StagingTabName");
				colNum = rs.getInt("numCol");
				warehouseTabName =rs.getString("warehouseTabName");
				warehouseColumn =rs.getString("warehouseColumn");
				staging_naturalKey =rs.getString("staging_naturalKey");
				warehouse_naturalKey =rs.getString("warehouse_naturalKey");
				warehouse_des = rs.getString("warehouse_des");
				procedureName = rs.getString("procedureName");
				localdir = rs.getString("filePathLocal");
				// dung cac bien luu cac column lay tu resultset
				mapResult.put("idConfig", String.valueOf(idConfig));mapResult.put("username", username);
				mapResult.put("password", password);mapResult.put("filepattern", filepattern);
				mapResult.put("des_config", des_config);mapResult.put("fileType",fileType);
				mapResult.put("delimiter",delimiter);mapResult.put("listColumn",listColumn);
				mapResult.put("listWarehouseRequireCol", listWarehouseRequireCol);mapResult.put("Staging_tabName",Staging_tabName);
				mapResult.put("warehouse_des",warehouse_des);mapResult.put("colNum",String.valueOf(colNum));
				mapResult.put("warehouseTabName",warehouseTabName);mapResult.put("warehouseColumn",warehouseColumn);
				mapResult.put("staging_naturalKey",staging_naturalKey);mapResult.put("warehouse_naturalKey",warehouse_naturalKey);
				mapResult.put("procedureName", procedureName);mapResult.put("localdir", localdir);
				//luu cac bien vao ben trong mot map
			}
			connection_user.close();
			//dong ket noi toi database
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return mapResult;
		//tra ve mot map da luu cac bien
	}
	public void LoadToWarehouse(Map<String,String> map) throws SQLException, ClassNotFoundException {
		//phuong thuc nhan vao map va load cac row trong staging vao warehouse
		Connection connection_user = null;
		//khoi tao mot connection
		UpdateLog ul = new UpdateLog();
		//new ra mot doi tuong cua class UpdateLog de goi phuong thuc ben trong
		if(checkExistsOfProceDure(map.get("warehouse_des"), map.get("username"), map.get("password"), map.get("procedureName")) == true) {
			//kiem tra procedure dung de insert vao warehouse co ton tai hay khong
			try {
				connection_user = DriverManager.getConnection(map.get("warehouse_des"), map.get("username"), map.get("password"));
				//lay ket noi dua tren config
				connection_user.setAutoCommit(false);
				//set chuc nang tu dong commit la false 
				PreparedStatement stat = connection_user.prepareStatement("select "+map.get("listColumn") +" from staging."+map.get("Staging_tabName"));
				//tao cau query luu vao preparedStatement
				System.out.println("select "+map.get("listColumn") +" from "+map.get("Staging_tabName"));
				//test query
				ResultSet rs= stat.executeQuery();
				//execute cau query lay ra ResultSet
				while(rs.next()) {
					//lay ra resultset 
					String value = handleValue(rs,map.get("listColumn"));
					// thuc hien phuong thuc handleValue nhan vao resultSet va listColumn tra ve cac gia tri cho cau query
					insertOneRecord(value,map.get("procedureName"),map.get("warehouse_des"),map.get("username"),map.get("password"));
					//thuc hien phuong thuc insertRecord nhan vao procdureName, connection ,user ,password de goi mot procedure
				}
				ul.updateLogWhenSuccessLoadWarehouse(Integer.parseInt(map.get("idConfig")));
				//updateLog
				connection_user.commit();
				//thuc hien commit data len database
				connection_user.close();
				//thuc hien dong ket noi
				truncateTable(map.get("Staging_tabName"));
				//thuc hien truncate table o staging
			} catch (SQLException e) {
				e.printStackTrace();
				connection_user.rollback();
				//neu trong qua trinh insert co mot ngoai le se roll back transaction
				ul.updateLogWhenFailLoadWarehouse(Integer.parseInt(map.get("idConfig")));
				//update log la that bai
			}
		}
	}
	public boolean truncateTable(String tabname) throws ClassNotFoundException {
		//phuong thuc nhan vao ten table staging va truncate no
		try {
			Connection connection_user = BaseConnection.getMySQLConnection();
			//mo connection co san
			PreparedStatement stat = connection_user.prepareStatement("truncate table staging."+tabname+";");
			// build cau query
			stat.execute();
			//thuc thi cau query
			connection_user.close();
			//dong ket noi 
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return true;
	}
	public String handleValue(ResultSet rs,String listCol) throws SQLException {
		String values = "";
		StringTokenizer stoken = new StringTokenizer(listCol, ",");
		while(stoken.hasMoreElements()) {
			values+="'";
			values+=rs.getString(stoken.nextToken());
			values+="',";
		}
		values = values.substring(0, values.length()-1);
		return values.toString();
	}
	public boolean checkExistsOfProceDure(String destination,String username,String password,String procedureName) {
		//phuong thuc checkExistsOfProceDure nhan vao procdureName, connection ,user ,password de kiem tra co ton tai procedure
		try {
			Connection connection_user = DriverManager.getConnection(destination, username, password);
			// dung des ,user,password tao mot ket noi
			connection_user.setAutoCommit(false);
			//set chuc nang tu dong commit la false 
			PreparedStatement stat = connection_user.prepareStatement("SHOW CREATE PROCEDURE "+procedureName+";");
			// build cau query
			stat.execute();
			//thuc thi cau query
			ResultSet rs = stat.getResultSet();
			//tra ve mot resultset
			if(rs.next()) {
				//neu co result chung to la co procedure
				return true;
				// tra ve ket qua dung la co procedure
			}
			connection_user.commit();
			connection_user.close();
			//dong ket noi
		} catch (SQLException e) {
			return false;
		}
		return false;
		// tra ve ket qua ko co procedure
	}
	public void insertOneRecord(String value,String procedureName,String des,String username,String password) {
		//thuc hien phuong thuc insertRecord nhan vao procdureName, connection ,user ,password de goi mot procedure
		Connection connection_user = null;
		// khoi tao mot connection
		try {
			connection_user = DriverManager.getConnection("jdbc:mysql://localhost:3306/warehouse?useSSL=false&characterEncoding=utf8","root", "1234");
			//tao mot connection
			connection_user.setAutoCommit(false);
			//set chuc nang tu dong commit la false 
			CallableStatement call = connection_user.prepareCall("{call "+procedureName+"("+value+")}");
			//tao cau lenh call procedure
			System.out.println("{call "+procedureName+"("+value+")}");
			//test query
			call.execute();
			//thuc thi cau call procedure
			connection_user.commit();
			// thuc hien commit
			connection_user.close();
			// dong ket noi
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws SQLException {
		StagingToWarehouse stw = new StagingToWarehouse();
//		stw.createProcedure("insertClassroomdim", "MaSV varchar(20),Holot varchar(20),Ten varchar(20),Ngaysinh varchar(20),Malop varchar(50),lop varchar(20),sodienthoai varchar(10),email varchar(50),quequan varchar(20),ghichu varchar(20)", "studentdim", "", "MaSV");
//		Map<String,String> list = stw.getSTReadyToWarehouse(1);
//		System.out.println(list.get("localdir"));
	}
}
