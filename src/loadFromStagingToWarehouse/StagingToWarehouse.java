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
import org.apache.poi.ss.formula.functions.Vlookup;

import com.mysql.jdbc.Statement;

import connectionDatabase.BaseConnection;

public class StagingToWarehouse {
	public void part3() {
		Map<String, String> map = getSTReadyToWarehouse();
		LoadToWarehouse(map);
	}
	public Map<String,String> getSTReadyToWarehouse() {
		Map<String, String> mapResult = new HashedMap<String, String>();
        int idConfig =0;
        String username = "";
        String password ="";
        String localdir ="";
        String des_config ="";
		String fileType = "";
		String delimiter = "";
		String listColumn = "";
		String listWarehouseRequireCol = "";
		String Staging_tabName = "";
		int colNum = 0;
		String warehouseTabName ="";
		String warehouseColumn ="";
		String staging_naturalKey ="";
		String warehouse_naturalKey ="";
		String warehouse_des ="";
		try {
			Connection connection_user = BaseConnection.getMySQLConnection();
			connection_user.setAutoCommit(false);
			PreparedStatement stat = connection_user.prepareStatement("select * from configtable left join logtab on logtab.idconfig = configtable.idconfig where logtab.status_file = 'success';");
			stat.execute();
			ResultSet rs =  stat.getResultSet();
			while(rs.next()) {
				idConfig =rs.getInt(1);
		        username = rs.getString(2);
		        password =rs.getString(3);
		        localdir =rs.getString(4);
		        des_config =rs.getString(5);
				fileType = rs.getString(6);
				delimiter = rs.getString(7);
				listColumn = rs.getString(8);
				listWarehouseRequireCol = rs.getString(9);
				Staging_tabName = rs.getString(10);
				colNum = rs.getInt(11);
				warehouseTabName =rs.getString(12);
				warehouseColumn =rs.getString(13);
				staging_naturalKey =rs.getString(14);
				warehouse_naturalKey =rs.getString(15);
				warehouse_des = rs.getString(16);
				mapResult.put("idConfig", String.valueOf(idConfig));mapResult.put("username", username);
				mapResult.put("password", password);mapResult.put("localdir", localdir);
				mapResult.put("des_config", des_config);mapResult.put("fileType",fileType);
				mapResult.put("delimiter",delimiter);mapResult.put("listColumn",listColumn);
				mapResult.put("listWarehouseRequireCol", listWarehouseRequireCol);mapResult.put("Staging_tabName",Staging_tabName);
				mapResult.put("warehouse_des",warehouse_des);mapResult.put("colNum",String.valueOf(colNum));
				mapResult.put("warehouseTabName",warehouseTabName);mapResult.put("warehouseColumn",warehouseColumn);
				mapResult.put("staging_naturalKey",staging_naturalKey);mapResult.put("warehouse_naturalKey",warehouse_naturalKey);
				break;
			}
			connection_user.close();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return mapResult;
	}
	public void LoadToWarehouse(Map<String,String> map) {
		try {
			Connection connection_user = DriverManager.getConnection(map.get("warehouse_des"), map.get("username"), map.get("password"));
			connection_user.setAutoCommit(false);
			PreparedStatement stat = connection_user.prepareStatement("select"+map.get("") +"from"+map.get(""));
			ResultSet rs= stat.executeQuery();
			while(rs.next()) {
				String value = handleValue(rs, map.get(""));
				insertOneRecord(value, "insertProcedure");
			}
			//ghi log
			//truncate table
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public void truncateTable(String destination,String username,String password,String tableName) {
		try {
			Connection connection_user = DriverManager.getConnection(destination, username, password);
			connection_user.setAutoCommit(false);
			
			PreparedStatement stat = connection_user.prepareStatement("TRUNCATE TABLE"+tableName+";");
			stat.execute();
			
			connection_user.commit();
			connection_user.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
	
		try {
			Connection connection_user = DriverManager.getConnection(destination, username, password);
			connection_user.setAutoCommit(false);
			
			PreparedStatement stat = connection_user.prepareStatement("SHOW CREATE PROCEDURE "+procedureName+";");
			stat.execute();
			ResultSet rs = stat.getResultSet();
			if(rs.next()) {
				return true;
			}
			connection_user.commit();
			connection_user.close();
		} catch (SQLException e) {
			return false;
		}
		
		return false;
	}
	public void insertOneRecord(String value,String procedureName) {
		Connection connection_user;
		try {
			connection_user = DriverManager.getConnection("jdbc:mysql://localhost:3306/staging?useSSL=false&characterEncoding=utf8","root", "1234");
			connection_user.setAutoCommit(false);
			CallableStatement call = connection_user.prepareCall("{call "+procedureName+"("+value+")}");
			System.out.println("{call "+procedureName+"("+value+")}");
			call.execute();
			connection_user.commit();
			connection_user.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String splitAndProcess(String listColumnwarehouse) {
		String[] split = listColumnwarehouse.split(",");
		String valueInsert ="";
		String input ="";
		for (int i = 0; i < split.length; i++) {
			valueInsert += "in " + split[i]+",";
			String[] nameCol = split[i].split(" ");
			input += nameCol[0]+",";
		}
		valueInsert = valueInsert.substring(0, valueInsert.length()-1);
		input = input.substring(0,input.length()-1);
		valueInsert +="\n"+ input; 
		return valueInsert;
	}
	public void createProcedure(String procedureName,String listWarehouseRequired,String tableName,String listCol,String listValue,String naturalKey,String naturalKeyValue) {
		String infor = splitAndProcess(listWarehouseRequired);
		String[] part = infor.split("\n");
		String cmd = "CREATE DEFINER=`root`@`localhost` PROCEDURE `"+tableName+"`("+part[0]+")\r\n" + 
				"If EXISTS (SELECT * FROM "+tableName+" WHERE "+tableName+"."+naturalKey+"= MaSV and "+tableName+".dt_expired='9999-01-01') then\r\n" + 
				"	update studentdim set "+tableName+".dt_expired=curdate() where "+tableName+"."+naturalKey+"= MaSV and "+tableName+".dt_expired='9999-01-01';\r\n" + 
				"	insert into "+tableName+"("+listCol+") values("+part[1]+",'9999-01-01','9999-01-01);\r\n" + 
				"ELSE\r\n" + 
				"   insert into "+tableName+"("+listCol+") values("+part[1]+",'9999-01-01','9999-01-01);\r\n" + 
				"end if";
		System.out.println(cmd);
	}
	public static void main(String[] args) throws SQLException {
		StagingToWarehouse stw = new StagingToWarehouse();
//		stw.createProcedure("insertProduct","MaSV varchar(10),hoVaTen varchar(30)", "studentdim","MaSV,hoVaTen,dt_expired,dt_hasChange", "'17130026','yeye','9999-01-01','9999-01-01", "Masv", "17130026");
//		System.out.println(stw.checkExistsOfProceDure("jdbc:mysql://localhost:3306/staging?useSSL=false&characterEncoding=utf8", "root", "1234", "insertProdure"));
//		Connection connection_user = DriverManager.getConnection("jdbc:mysql://localhost:3306/staging?useSSL=false&characterEncoding=utf8","root", "1234");
//		connection_user.setAutoCommit(false);
//		PreparedStatement stat = connection_user.prepareStatement("select MSSV,HoLot from staging.data1");
//		ResultSet rs= stat.executeQuery();
//		int i = 0;
//		while(i<1) {
//			rs.next();
//			String value = stw.handleValue(rs,"MSSV,Holot");
//			stw.insertOneRecord(value, "insertProdure");
//			System.out.println(rs.getString("MSSV"));
//			i++;
//		}
	}
}
