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
	public void part3() throws SQLException {
//		Map<String, String> map = getSTReadyToWarehouse(1);
//		LoadToWarehouse(map);
	}
	public Map<String,String> getSTReadyToWarehouse(int id) {
		Map<String, String> mapResult = new HashedMap<String, String>();
        int idConfig =0;//1
        String username = "";//2
        String password ="";//3
        String remoteDir="";//4
        String port ="";//5
        String localdir ="";//6
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
		try {
			Connection connection_user = BaseConnection.getMySQLConnection();
			connection_user.setAutoCommit(false);
			PreparedStatement stat = connection_user.prepareStatement("select * from configtable left join logtab on logtab.idconfig = configtable.idconfig where logtab.status_file = 'success_to_staging' and logtab.idconfig="+id+";");
			stat.execute();
			ResultSet rs =  stat.getResultSet();
			while(rs.next()) {
				idConfig =rs.getInt(1);
		        username = rs.getString(2);
		        password =rs.getString(3);
		        remoteDir = rs.getString(4);
		        port = rs.getString(5);
		        localdir =rs.getString(6);
		        des_config =rs.getString(7);
				fileType = rs.getString(8);
				delimiter = rs.getString(9);
				listColumn = rs.getString(10);
				listWarehouseRequireCol = rs.getString(11);
				Staging_tabName = rs.getString(12);
				colNum = rs.getInt(13);
				warehouseTabName =rs.getString(14);
				warehouseColumn =rs.getString(15);
				staging_naturalKey =rs.getString(16);
				warehouse_naturalKey =rs.getString(17);
				warehouse_des = rs.getString(18);
				procedureName = rs.getString(19);
				mapResult.put("idConfig", String.valueOf(idConfig));mapResult.put("username", username);
				mapResult.put("password", password);mapResult.put("localdir", localdir);
				mapResult.put("des_config", des_config);mapResult.put("fileType",fileType);
				mapResult.put("delimiter",delimiter);mapResult.put("listColumn",listColumn);
				mapResult.put("listWarehouseRequireCol", listWarehouseRequireCol);mapResult.put("Staging_tabName",Staging_tabName);
				mapResult.put("warehouse_des",warehouse_des);mapResult.put("colNum",String.valueOf(colNum));
				mapResult.put("warehouseTabName",warehouseTabName);mapResult.put("warehouseColumn",warehouseColumn);
				mapResult.put("staging_naturalKey",staging_naturalKey);mapResult.put("warehouse_naturalKey",warehouse_naturalKey);
				mapResult.put("procedureName", procedureName);
				break;
			}
			connection_user.close();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return mapResult;
	}
	public void LoadToWarehouse(Map<String,String> map) throws SQLException {
		Connection connection_user = null;
		if(checkExistsOfProceDure(map.get("warehouse_des"), map.get("username"), map.get("password"), map.get("procedureName")) == true) {
			try {
				connection_user = DriverManager.getConnection(map.get("warehouse_des"), map.get("username"), map.get("password"));
				connection_user.setAutoCommit(false);
				String col = map.get("listWarehouseRequireCol");
				String haveHandle = splitAndProcess(col);
				String[] attribute = haveHandle.split("\n");
				PreparedStatement stat = connection_user.prepareStatement("select "+map.get("listColumn") +" from staging."+map.get("Staging_tabName"));
				System.out.println("select "+map.get("listColumn") +" from "+map.get("Staging_tabName"));
				ResultSet rs= stat.executeQuery();
				while(rs.next()) {
					String value = handleValue(rs,map.get("listColumn"));
					insertOneRecord(value,map.get("procedureName"),map.get("warehouse_des"),map.get("username"),map.get("password"));
				}
				//ghi log
				//truncate table
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				connection_user.rollback();
				e.printStackTrace();
			}
		}else {
			createProcedure(map.get("procedureName"), map.get("listWarehouseRequireCol"), map.get("warehouseTabName"), map.get("warehouseColumn"), map.get("warehouse_naturalKey"));
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
	public void insertOneRecord(String value,String procedureName,String des,String username,String password) {
		Connection connection_user;
		try {
			connection_user = DriverManager.getConnection("jdbc:mysql://localhost:3306/warehouse?useSSL=false&characterEncoding=utf8","root", "1234");
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
	public void createProcedure(String procedureName,String listWarehouseRequired,String tableName,String listCol,String naturalKey) {
		String infor = splitAndProcess(listWarehouseRequired);
		String[] part = infor.split("\n");
		String cmd = "CREATE DEFINER=`root`@`localhost` PROCEDURE `"+procedureName+"`("+part[0]+")\r\n" + 
				"If EXISTS (SELECT * FROM "+tableName+" WHERE "+tableName+"."+naturalKey+"= "+naturalKey+" and "+tableName+".dt_expired='9999-01-01') then\r\n" + 
				"	update studentdim set "+tableName+".dt_expired=curdate() where "+tableName+"."+naturalKey+"= "+naturalKey+" and "+tableName+".dt_expired='9999-01-01';\r\n" + 
				"	insert into "+tableName+"("+part[1]+",dt_expired,dt_haschange) values("+part[1]+",'9999-01-01','9999-01-01');\r\n" + 
				"ELSE\r\n" + 
				"   insert into "+tableName+"("+part[1]+",dt_expired,dt_haschange) values("+part[1]+",'9999-01-01','9999-01-01');\r\n" + 
				"end if";
		System.out.println(cmd);
	}
	public static void main(String[] args) throws SQLException {
		StagingToWarehouse stw = new StagingToWarehouse();
		stw.createProcedure("insertClassroomdim", "MaLH int,MaMH varchar(10),Ca int,Nhom int,Nam varchar(10)", "classroomdim", "", "MaLH");
	}
}
