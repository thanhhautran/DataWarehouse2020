package loadFromStagingToWarehouse;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;

import connectionDatabase.BaseConnection;

public class StagingToWarehouse {
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
				mapResult.put("idConfig", String.valueOf(idConfig));mapResult.put("username", username);
				mapResult.put("password", password);mapResult.put("localdir", localdir);
				mapResult.put("des_config", des_config);mapResult.put("fileType",fileType);
				mapResult.put("delimiter",delimiter);mapResult.put("listColumn",listColumn);
				mapResult.put("listWarehouseRequireCol", listWarehouseRequireCol);mapResult.put("Staging_tabName",Staging_tabName);
				mapResult.put("Staging_tabName",Staging_tabName);mapResult.put("colNum",String.valueOf(colNum));
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
		
	}
	public void insertOneRecord() {
		Connection connection_user;
		try {
			connection_user = BaseConnection.getMySQLConnection();
			connection_user.setAutoCommit(false);
			CallableStatement call = connection_user.prepareCall("{call demoSp(?, ?)}");
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
