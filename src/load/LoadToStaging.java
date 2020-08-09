package load;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import connectionDatabase.BaseConnection;
import preprocessingInStaging.Preprocessing;
import sendMail.SendMail;
import updateLogAndConfig.UpdateLog;


public class LoadToStaging {
		public boolean fileIsExsist(Map<String, String> map) {
			File file = new File(map.get("localdir"));
			if(file.exists()) {
				return true;
			}else {
			return false;
			}
		}
		public void callRun() throws ClassNotFoundException, SQLException {
			Connection connection_user = null;
			// khoi tao mot connection
			try {
				connection_user = BaseConnection.getMySQLConnection();
				//tao mot connection
				connection_user.setAutoCommit(false);
				//set chuc nang tu dong commit la false 
				CallableStatement call = connection_user.prepareCall("{call checkrunprocess() }");
				//test query
				call.execute();
				//thuc thi cau call procedure
				connection_user.commit();
				// thuc hien commit
				connection_user.close();
				// dong ket noi
			} catch (SQLException e) {
				connection_user.rollback();
				e.printStackTrace();
			}
		}
		public int getNumColOfTable(String tableName,String desConfig,String user,String password) throws SQLException {
			Connection con = DriverManager.getConnection(desConfig, user, password);
			int numcol =0;
			String text = "SELECT COUNT(*) FROM "+tableName;
			PreparedStatement ps = con.prepareStatement(text);
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				numcol = rs.getInt(1);
			}
			return numcol;
		}
		public boolean isEqualColNum(Map<String, String> map) {
			try {
				int colNumExecute = getNumColOfTable(map.get("Staging_tabName"),map.get("des_config"),map.get("username"),map.get("password"));
				if(colNumExecute == Integer.parseInt(map.get("colNum"))) {
					return true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			return false;
		}
		public Map<String,String> getFileToLoadStagingById(int id){
			Map<String, String> mapResult = new HashedMap<String, String>();
	        int idConfig =0;//1
	        String username = "";//2
	        String password ="";//3
	        String remoteDir="";//4
	        String port ="";//5
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
			int idlogtab = 0 ;//20
			String localdir ="";
			
			try {
				Connection connection_user = BaseConnection.getMySQLConnection();
				connection_user.setAutoCommit(false);
				PreparedStatement stat = connection_user.prepareStatement("select * from configtable c, logtab l where "
					+ "l.idconfig = c.idconfig and l.status_file = 'ready_to_staging' "
					+ "and c.idconfig = "+id+" limit 1;");
				stat.execute();
				ResultSet rs =  stat.getResultSet();
				while(rs.next()) {
					idConfig =rs.getInt("idConfig");
			        username = rs.getString("user");
			        password =rs.getString("password");
			        remoteDir = rs.getString("remote_Dir");
			        port = rs.getString("port");
			        filepattern =rs.getString("file_pattern");
			        des_config =rs.getString("des_config");
					fileType = rs.getString("file_type");
					delimiter = rs.getString("delimiter");
					listColumn = rs.getString("list_column");
					listWarehouseRequireCol = rs.getString("list_warehouseRequiredColumn");
					Staging_tabName = rs.getString("stagingTabName");
					colNum = rs.getInt("numcol_of_file");
					warehouseTabName =rs.getString("warehouseTabName");
					warehouseColumn =rs.getString("warehouseColumn");
					staging_naturalKey =rs.getString("staging_naturalKey");
					warehouse_naturalKey =rs.getString("warehouse_naturalKey");
					warehouse_des = rs.getString("warehouse_des");
					procedureName = rs.getString("procedureName");
					idlogtab = rs.getInt("idlogTab");
					localdir = rs.getString("filePathLocal");
					mapResult.put("idConfig", String.valueOf(idConfig));mapResult.put("username", username);
					mapResult.put("password", password);mapResult.put("localdir", localdir);
					mapResult.put("des_config", des_config);mapResult.put("fileType",fileType);
					mapResult.put("delimiter",delimiter);mapResult.put("listColumn",listColumn);
					mapResult.put("listWarehouseRequireCol", listWarehouseRequireCol);mapResult.put("Staging_tabName",Staging_tabName);
					mapResult.put("warehouse_des",warehouse_des);mapResult.put("colNum",String.valueOf(colNum));
					mapResult.put("warehouseTabName",warehouseTabName);mapResult.put("warehouseColumn",warehouseColumn);
					mapResult.put("staging_naturalKey",staging_naturalKey);mapResult.put("warehouse_naturalKey",warehouse_naturalKey);
					mapResult.put("procedureName", procedureName);mapResult.put("idlogtab",String.valueOf(idlogtab));mapResult.put("filepattern", filepattern);
				}
				connection_user.close();
			} catch (ClassNotFoundException | SQLException e) {
				e.printStackTrace();
			}
			return mapResult;
		}
		public Map<String,String> getFileToLoadStaging() {
			Map<String, String> mapResult = new HashedMap<String, String>();
	        int idConfig =0;//1
	        String username = "";//2
	        String password ="";//3
	        String remoteDir="";//4
	        String port ="";//5
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
			int idlogtab = 0 ;//20
			String localdir ="";
			try {
				Connection connection_user = BaseConnection.getMySQLConnection();
				connection_user.setAutoCommit(false);
				PreparedStatement stat = connection_user.prepareStatement("select * from configtable "
						+ "left join logtab on logtab.idconfig = configtable.idconfig "
						+ "l.idconfig = c.idconfig and c.flag = 'run' and l.status_file = 'ready_to_staging' limit 1;");
				stat.execute();
				ResultSet rs =  stat.getResultSet();
				while(rs.next()) {
					idConfig =rs.getInt("idConfig");
			        username = rs.getString("user");
			        password =rs.getString("password");
			        remoteDir = rs.getString("remoteDir");
			        port = rs.getString("port");
			        filepattern =rs.getString("file_pattern");
			        des_config =rs.getString("des_config");
					fileType = rs.getString("file_type");
					delimiter = rs.getString("delimiter");
					listColumn = rs.getString("list_column");
					listWarehouseRequireCol = rs.getString("list_warehouseRequiredColumn");
					Staging_tabName = rs.getString("stagingTabName");
					colNum = rs.getInt("numcol_of_file");
					warehouseTabName =rs.getString("warehouseTabName");
					warehouseColumn =rs.getString("warehouseColumn");
					staging_naturalKey =rs.getString("staging_naturalKey");
					warehouse_naturalKey =rs.getString("warehouse_naturalKey");
					warehouse_des = rs.getString("warehouse_des");
					procedureName = rs.getString("procedureName");
					idlogtab = rs.getInt("idlogTab");
					localdir = rs.getString("filePathLocal");
					mapResult.put("idConfig", String.valueOf(idConfig));mapResult.put("username", username);
					mapResult.put("password", password);mapResult.put("localdir", localdir);
					mapResult.put("des_config", des_config);mapResult.put("fileType",fileType);
					mapResult.put("delimiter",delimiter);mapResult.put("listColumn",listColumn);
					mapResult.put("listWarehouseRequireCol", listWarehouseRequireCol);mapResult.put("Staging_tabName",Staging_tabName);
					mapResult.put("warehouse_des",warehouse_des);mapResult.put("colNum",String.valueOf(colNum));
					mapResult.put("warehouseTabName",warehouseTabName);mapResult.put("warehouseColumn",warehouseColumn);
					mapResult.put("staging_naturalKey",staging_naturalKey);mapResult.put("warehouse_naturalKey",warehouse_naturalKey);
					mapResult.put("procedureName", procedureName);mapResult.put("filepattern", filepattern);
					break;
				}
				connection_user.close();
			} catch (ClassNotFoundException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return mapResult;
		}
		public void loadToLocal(Map<String, String> mapConfig) throws EncryptedDocumentException, NumberFormatException, IOException, SQLException {
			Map<String, String> mapLog = getLogOf(Integer.parseInt(mapConfig.get("idConfig")));
			UpdateLog ul = new UpdateLog();
			switch (mapConfig.get("fileType")) {
			case ".xlsx": {
				try {
				loadFileExcel(Integer.parseInt(mapConfig.get("idConfig")),Integer.parseInt(mapLog.get("id_logtab")),mapConfig.get("localdir"),mapConfig.get("delimiter"), mapConfig.get("des_config"),mapConfig.get("username"), mapConfig.get("password"), mapConfig.get("Staging_tabName"));
//					int numcol = getNumColOfTable(mapConfig.get("Staging_tabName"),mapConfig.get("des_config"),mapConfig.get("username"),mapConfig.get("password"));
//					ul.updateLogWhenSuccess(Integer.parseInt(mapLog.get("id_logtab")), numcol);
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
			case ".txt": {
				try {
				loadFileTxt(Integer.parseInt(mapConfig.get("idConfig")),Integer.parseInt(mapLog.get("id_logtab")),mapConfig.get("localdir"),mapConfig.get("delimiter"), mapConfig.get("des_config"),mapConfig.get("username"), mapConfig.get("password"), mapConfig.get("Staging_tabName"));
//					int numcol = getNumColOfTable(mapConfig.get("Staging_tabName"),mapConfig.get("des_config"),mapConfig.get("username"),mapConfig.get("password"));
//					ul.updateLogWhenSuccess(Integer.parseInt(mapLog.get("id_logtab")), numcol);
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
			case ".csv": {	
				try {
				loadFileCsv(Integer.parseInt(mapConfig.get("idConfig")),Integer.parseInt(mapLog.get("id_logtab")),mapConfig.get("localdir"),mapConfig.get("delimiter"), mapConfig.get("des_config"),mapConfig.get("username"), mapConfig.get("password"), mapConfig.get("Staging_tabName"));
//					int numcol = getNumColOfTable(mapConfig.get("Staging_tabName"),mapConfig.get("des_config"),mapConfig.get("username"),mapConfig.get("password"));
//					ul.updateLogWhenSuccess(Integer.parseInt(mapLog.get("id_logtab")), numcol);
				}catch (Exception e) {
					e.printStackTrace();
					}
				}
			}
		}
		
		public static Map<String,String> getLogOf(int idConfig) {
			Map<String, String> result = new HashedMap<String, String>();
	        int id_logtab =0;
	        int idconfig = 0;
	        String statusFile ="";
	        String detail_information="";
	        String timeLoadStaging ="";
	        String timeLoadWarehouse="";
	        String numColHaveLoad ="";
	        try {
				Connection connection_user = BaseConnection.getMySQLConnection();
				connection_user.setAutoCommit(false);
				PreparedStatement stat = connection_user.prepareStatement("select * from logtab left join configtable on logtab.idconfig = configtable.idconfig where logtab.idconfig="+idConfig+";");
				  stat.execute();
				ResultSet rs =  stat.getResultSet();
				while(rs.next()) {
					id_logtab =rs.getInt(1);
					idconfig = rs.getInt(2);
			        statusFile =rs.getString(3);
			        detail_information = rs.getString(4);
			        timeLoadStaging =rs.getString(5);
			        timeLoadWarehouse=rs.getString(6);
			        numColHaveLoad =rs.getString(7);
			        result.put("id_logtab", String.valueOf(id_logtab));
			        result.put("idconfig",String.valueOf(idconfig));result.put("statusFile", statusFile);
			        result.put("detail_information", detail_information);
			        result.put("timeLoadStaging", timeLoadStaging);result.put("timeLoadWarehouse", timeLoadWarehouse);
			        result.put("numColHaveLoad", numColHaveLoad);
				}
				connection_user.close();
				
			} catch (SQLException | ClassNotFoundException e1) {
				e1.printStackTrace();
			}
			return result;
		}
		public static String convertToCsv(String path) throws EncryptedDocumentException, IOException {
			File file = new File(path);
			InputStream is = new FileInputStream(file);
			Workbook wb = WorkbookFactory.create(is);
			
	        Sheet sheet = wb.getSheetAt(0);
	        Iterator<Row> rowIterator = sheet.iterator();
	        String str = "";
	        int line = 0 ;
	       int colNum = 0;
	        while(rowIterator.hasNext()) {
	        	line++;
	        	Row fRow = rowIterator.next();
	        	if(line == 1) {
	        	colNum = fRow.getLastCellNum();
	        	}
	        	for (int i=0; i<colNum; i++) {
	        		DataFormatter df = new DataFormatter();
	        		Cell cell = fRow.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
	        		String s ="";
	        		if(cell.getCellType() == CellType.FORMULA) {
	        			if(cell.getCachedFormulaResultType() == CellType.NUMERIC) {
	        				s = ""+cell.getNumericCellValue();
	        				str += s+",";
	        			}
	        			else if(cell.getCachedFormulaResultType() == CellType.STRING){
	        				s = cell.getStringCellValue();
	        				str += s+",";
	                	}
	        		}
	        		else if(cell.getCellType() == CellType.BLANK || cell == null){
	        				str += ",";
	        		}
	        		else if(cell.getCellType() != CellType.BLANK){
	        			 s = df.formatCellValue(cell);
	        			 str += s+",";
	        		}
	        	}
	        	str += "\n";
	        }
	        System.out.println(str);
	        File fileout = new File(file.getParent()+file.separator+file.getName().substring(0,file.getName().length()-5)+".csv");
	        FileOutputStream fos = new FileOutputStream(fileout);
	        fos.write(str.getBytes(StandardCharsets.UTF_8));
			return fileout.getAbsolutePath();
		}
		public static void loadFileCsv(int idconfig,int id_log,String sourceConfig,String delimiter,String desConfig,String user,String password,String tableName) {
	        try {
	        	if(delimiter.equals("comma")) {
					delimiter =",";
				}
				Connection connection_user = DriverManager.getConnection(desConfig, user, password);
				connection_user.setAutoCommit(false);
				String command =" load data infile "+"'"+sourceConfig+"'"+" into table staging." +tableName+"  FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES;";
				PreparedStatement stat = connection_user.prepareStatement(command);
				stat.execute();
				connection_user.commit();
				connection_user.close();		
			} catch (SQLException e1) {
			}
	        
		}
		public static void loadFileTxt(int idconfig,int id_log,String sourceConfig,String delimiter,String desConfig,String user,String password,String tableName) {
			try {
				Connection connection_user = DriverManager.getConnection(desConfig, user, password);
				connection_user.setAutoCommit(false);
				if(delimiter.equals("comma")) {
					delimiter =",";
				}
				String command =" load data infile "+"'"+sourceConfig+"'"+" into table staging." +tableName+" FIELDS TERMINATED BY '"+delimiter+"' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES;";
				PreparedStatement stat = connection_user.prepareStatement(command);
				stat.execute();
				connection_user.commit();
				connection_user.close();
			} catch (SQLException e1) {
			}
		}
		public static void loadFileExcel(int idconfig,int id_log,String sourceConfig,String delimiter,String desConfig,String user,String password,String tableName) throws EncryptedDocumentException, IOException{
			try {
				if(delimiter.equals("comma")) {
					delimiter =",";
				}
				String afterConvert = convertToCsv(sourceConfig);
				File file = new File(afterConvert);
				String source = file.getParent()+"\\\\"+file.getName();
				Connection connection_user = DriverManager.getConnection(desConfig, user, password);
				connection_user.setAutoCommit(false);
				String command = " load data infile "+"'"+source+"'"+" into table staging." +tableName+" FIELDS TERMINATED BY '"+delimiter+"' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES;";
				PreparedStatement stat = connection_user.prepareStatement(command);
				System.out.println(command);
				stat.execute();
				connection_user.commit();
				connection_user.close();
				
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		public static void main(String[] args) throws EncryptedDocumentException, IOException {
			LoadToStaging ltd = new LoadToStaging();
//			ltd.convertToCsv("D:\\localpaging\\\\LopHoc_S_03.xlsx");
		}
}
