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

public class LoadToStaging {
		public boolean fileIsExsist(Map<String, String> map) {
			//phuong thuc kiem tra file co ton tai hay khong
			File file = new File(map.get("localdir"));
			//tao ra mot doi tuong file voi duong dan local trong config
			if(file.exists()) {
				//neu ton tai tra ve true
				return true;
			}else {
				//neu ton tai tra ve fail
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
				//tao cau call procedure
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
			//phuong thuc tra ve so fong trong mot bang staging
			Connection con = DriverManager.getConnection(desConfig, user, password);
			//tao ket noi voi connection, user,password
			int numcol =0;
			//tao bien ket qua tra ve la so dong load
			String text = "SELECT COUNT(*) FROM "+tableName;
			// tao bien text la cau query 
			PreparedStatement ps = con.prepareStatement(text);
			// tao preparedStatement thuc hien cau query
			ResultSet rs = ps.executeQuery();
			// thuc thi cau query
			while(rs.next()) {
				//rs tra ve ket qua 
				numcol = rs.getInt(1);
				//set bien tra ve la cot 1 cua rs
			}
			return numcol;
		}
		public boolean isEqualColNum(Map<String, String> map) {
			//phuong thuc kiem tra xem so dong load co bang so dong trong file hay khong
			try {
				int colNumExecute = getNumColOfTable(map.get("Staging_tabName"),map.get("des_config"),map.get("username"),map.get("password"));
				//lay ve so dong load duoc cua bang vua load set vao bien colNumExcecute
				if(colNumExecute == Integer.parseInt(map.get("colNum"))) {
					//neu dong da load bang so dong trong file local thi tra ve true
					return true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			//neu dong da load khong bang so dong trong file local thi tra ve fail
			return false;
		}
		public Map<String,String> getFileToLoadStagingById(int id){
			Map<String, String> mapResult = new HashedMap<String, String>();
			//tao ra mot map luu cac config vao
	        int idConfig =0;//1
	        String username = "";//2
	        String password ="";//3
//	        String remoteDir="";//4
//	        String port ="";//5
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
			// khoi tao cac bien tuong ung voi config
			try {
				Connection connection_user = BaseConnection.getMySQLConnection();
				//lay ra connection toi control_database - 1.Mở kết nối tới Database control
				connection_user.setAutoCommit(false);
				//set auto commit bang false
				PreparedStatement stat = connection_user.prepareStatement("select * from configtable c, logtab l where "
					+ "l.idconfig = c.idconfig and l.status_file = 'ready_to_staging' "
					+ "and c.idconfig = "+id+" limit 1;");
				//preparedStatement cau query - 2.Lấy ra 1list config có thuộc tính statusfile trong bảng log là 'ready_to_staging'
				stat.execute();
				//thuc thi cau query
				ResultSet rs =  stat.getResultSet();
				//tra ve ket qua cua cau query
				while(rs.next()) {
					idConfig =rs.getInt("idConfig");
			        username = rs.getString("user");
			        password =rs.getString("password");
//			        remoteDir = rs.getString("remote_Dir");
//			        port = rs.getString("port");
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
					//set cac column tra ve cho cac bien
					mapResult.put("idConfig", String.valueOf(idConfig));mapResult.put("username", username);
					mapResult.put("password", password);mapResult.put("localdir", localdir);
					mapResult.put("des_config", des_config);mapResult.put("fileType",fileType);
					mapResult.put("delimiter",delimiter);mapResult.put("listColumn",listColumn);
					mapResult.put("listWarehouseRequireCol", listWarehouseRequireCol);mapResult.put("Staging_tabName",Staging_tabName);
					mapResult.put("warehouse_des",warehouse_des);mapResult.put("colNum",String.valueOf(colNum));
					mapResult.put("warehouseTabName",warehouseTabName);mapResult.put("warehouseColumn",warehouseColumn);
					mapResult.put("staging_naturalKey",staging_naturalKey);mapResult.put("warehouse_naturalKey",warehouse_naturalKey);
					mapResult.put("procedureName", procedureName);mapResult.put("idlogtab",String.valueOf(idlogtab));mapResult.put("filepattern", filepattern);
					//add bien vao trong map
				}
				connection_user.close();
				//dong connection
			} catch (ClassNotFoundException | SQLException e) {
				e.printStackTrace();
			}
			return mapResult;
		}
		public Map<String,String> getFileToLoadStaging() {
			//tra ve list config khong can id de chay auto 
			Map<String, String> mapResult = new HashedMap<String, String>();
			//khoi tao map de luu config
	        int idConfig =0;//1
	        String username = "";//2
	        String password ="";//3
//	        String remoteDir="";//4
//	        String port ="";//5
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
			// khoi tao cac bien tuong ung voi config
			try {
				Connection connection_user = BaseConnection.getMySQLConnection();
				//lay ra connection toi control_database - 1.Mở kết nối tới Database control
				connection_user.setAutoCommit(false);
				//set auto commit la false
				PreparedStatement stat = connection_user.prepareStatement("select * from configtable "
						+ "left join logtab on logtab.idconfig = configtable.idconfig "
						+ "l.idconfig = c.idconfig and c.flag = 'run' and l.status_file = 'ready_to_staging' limit 1;");
				//preparestatement cau query - 2.Lấy ra 1 list config có thuộc tính statusfile trong bảng log là 'ready_to_staging'
				stat.execute();
				// thuc thi cau query
				ResultSet rs =  stat.getResultSet();
				// tra ve ket qua resultset
				while(rs.next()) {
					//tra ve dong result set tiep theo
					idConfig =rs.getInt("idConfig");
			        username = rs.getString("user");
			        password =rs.getString("password");
//			        remoteDir = rs.getString("remoteDir");
//			        port = rs.getString("port");
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
					//gan bien bang mot column tuong ung trong resultset
					mapResult.put("idConfig", String.valueOf(idConfig));mapResult.put("username", username);
					mapResult.put("password", password);mapResult.put("localdir", localdir);
					mapResult.put("des_config", des_config);mapResult.put("fileType",fileType);
					mapResult.put("delimiter",delimiter);mapResult.put("listColumn",listColumn);
					mapResult.put("listWarehouseRequireCol", listWarehouseRequireCol);mapResult.put("Staging_tabName",Staging_tabName);
					mapResult.put("warehouse_des",warehouse_des);mapResult.put("colNum",String.valueOf(colNum));
					mapResult.put("warehouseTabName",warehouseTabName);mapResult.put("warehouseColumn",warehouseColumn);
					mapResult.put("staging_naturalKey",staging_naturalKey);mapResult.put("warehouse_naturalKey",warehouse_naturalKey);
					mapResult.put("procedureName", procedureName);mapResult.put("filepattern", filepattern);
					//them bien vao map
				}
				connection_user.close();
				//dong ket noi
			} catch (ClassNotFoundException | SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return mapResult;
		}
		public void loadToLocal(Map<String, String> mapConfig) throws EncryptedDocumentException, NumberFormatException, IOException, SQLException {
			//phuong thuc load toan bo file dua vao config
			Map<String, String> mapLog = getLogOf(Integer.parseInt(mapConfig.get("idConfig")));
			//lay ra map chua list cac thuoc tinh trong log
			switch (mapConfig.get("fileType")) {
			//kiem tra file type cua file can load
			case ".xlsx": {
				//neu la file excel thi load kieu excel
				try {
				loadFileExcel(Integer.parseInt(mapConfig.get("idConfig")),Integer.parseInt(mapLog.get("id_logtab")),mapConfig.get("localdir"),mapConfig.get("delimiter"), mapConfig.get("des_config"),mapConfig.get("username"), mapConfig.get("password"), mapConfig.get("Staging_tabName"));
				//goi phuong thuc load excel
				//-5.Lấy các thuộc tính cần thiết trong bảng config đưa vào phương thức load data vào staging
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
			case ".txt": {
				//neu la file text thi load kieu text
				try {
				loadFileTxt(Integer.parseInt(mapConfig.get("idConfig")),Integer.parseInt(mapLog.get("id_logtab")),mapConfig.get("localdir"),mapConfig.get("delimiter"), mapConfig.get("des_config"),mapConfig.get("username"), mapConfig.get("password"), mapConfig.get("Staging_tabName"));
				//goi phuong thuc load text
				//-5.Lấy các thuộc tính cần thiết trong bảng config đưa vào phương thức load data vào staging
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
			case ".csv": {	
				//neu la file csv thi load kieu csv
				try {
				loadFileCsv(Integer.parseInt(mapConfig.get("idConfig")),Integer.parseInt(mapLog.get("id_logtab")),mapConfig.get("localdir"),mapConfig.get("delimiter"), mapConfig.get("des_config"),mapConfig.get("username"), mapConfig.get("password"), mapConfig.get("Staging_tabName"));
				//goi phuong thuc load csv
				//-5.Lấy các thuộc tính cần thiết trong bảng config đưa vào phương thức load data vào staging
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
			//phuong thuc doi file excel sang csv
			File file = new File(path);
			//tao mot doi tuong file voi duong dan
			InputStream is = new FileInputStream(file);
			//tao mot doi tuong InputStream bao file
			Workbook wb = WorkbookFactory.create(is);
			//tao mot doi tuong workbook nhan vao inputStream
			// cau truc workbook > sheet > row > cell
	        Sheet sheet = wb.getSheetAt(0);
	        //lay sheet thu 0 trong file excel
	        Iterator<Row> rowIterator = sheet.iterator();
	        //tao mang iterator row tu sheet
	        String str = "";
	        //tao mang string 
	        int line = 0 ;
	        //tao bien line lay ra so cot max cua sheet
	       int colNum = 0;
	     //tao bien colNum lay ra so cot max cua sheet
	        while(rowIterator.hasNext()) {
	        	//duyet tung dong trong sheet
	        	line++;
	        	//dem bien line tang 1 don vi
	        	Row fRow = rowIterator.next();
	        	//lay ra row tiep theo  
	        	if(line == 1) {
	        	//neu la dong title dau thi lay ra so column cua no
	        	colNum = fRow.getLastCellNum();
	        	//set bien colNum bang so col cuoi cung
	        	}
	        	for (int i=0; i<colNum; i++) {
	        		//chay tu cell 0 den cell max
	        		DataFormatter df = new DataFormatter();
	        		//
	        		Cell cell = fRow.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
	        		// neu dong i la null thi sua them no la blank
	        		String s ="";
	        		//tao bien s tam de nhan gia tri cua tung cell doc ra
	        		if(cell.getCellType() == CellType.FORMULA) {
	        			//neu gia tri cua cell la mot ham excel
	        			if(cell.getCachedFormulaResultType() == CellType.NUMERIC) {
	        				//neu gia tri cua ham la numeric thi tra ve numeric
	        				s = ""+cell.getNumericCellValue();
	        				str += s+",";
	        			}
	        			else if(cell.getCachedFormulaResultType() == CellType.STRING){
	        				//neu gia tri cua ham la string thi tra ve string
	        				s = cell.getStringCellValue();
	        				str += s+",";
	                	}
	        		}
	        		else if(cell.getCellType() == CellType.BLANK || cell == null){
	        			//neu gia tri cua cell la mot o trong thi them mot dau phay
	        				str += ",";
	        		}
	        		else if(cell.getCellType() != CellType.BLANK){
	        			//neu cell co gia tri tra ve gia tri cua no da duoc format
	        			 s = df.formatCellValue(cell);
	        			// lay ra format chuan cua file
	        			 str += s+",";
	        		}
	        	}
	        	str += "\n";
	        	//them dau cach dong cho moi file
	        }
	        File fileout = new File(file.getParent()+file.separator+file.getName().substring(0,file.getName().length()-5)+".csv");
	        //tao file tra ve 
	        FileOutputStream fos = new FileOutputStream(fileout);
	        //tao doi tuong FileOutputStream bao doi tuong fileout
	        fos.write(str.getBytes(StandardCharsets.UTF_8));
	        //ghi str xuong file out theo chuan UTF 8
			return fileout.getAbsolutePath();
			//tra ve duong dan tuyet doi 
		}
		public static void loadFileCsv(int idconfig,int id_log,String sourceConfig,String delimiter,String desConfig,String user,String password,String tableName) {
			//phuong thuc load file theo csv
			try {
	        	if(delimiter.equals("comma")) {
					delimiter =",";
				}
	        	//neu delimiter la comma thi doi sang dau phay
				Connection connection_user = DriverManager.getConnection(desConfig, user, password);
				// mo connection
				connection_user.setAutoCommit(false);
				//set auto commit la false
				String command =" load data infile "+"'"+sourceConfig+"'"+" into table staging." +tableName+"  FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES;";
				// khoi tao cau lenh load ca file len
				PreparedStatement stat = connection_user.prepareStatement(command);
				//tao preparedStatement xu ly command
				stat.execute();
				// xu ly cau len 
				connection_user.commit();
				// commit
				connection_user.close();	
				//dong ket noi
			} catch (SQLException e1) {
				
			}
	        
		}
		public static void loadFileTxt(int idconfig,int id_log,String sourceConfig,String delimiter,String desConfig,String user,String password,String tableName) {
			try {
				Connection connection_user = DriverManager.getConnection(desConfig, user, password);
				// mo connection - 6.Thực hiện kết nối tới staging
				connection_user.setAutoCommit(false);
				//set auto commit la false
				if(delimiter.equals("comma")) {
					delimiter =",";
				}
				//neu delimiter la comma thi doi sang dau phay
				String command =" load data infile "+"'"+sourceConfig+"'"+" into table staging." +tableName+" FIELDS TERMINATED BY '"+delimiter+"' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES;";
				// khoi tao cau lenh load ca file len
				PreparedStatement stat = connection_user.prepareStatement(command);
				//tao preparedStatement xu ly command
				stat.execute();
				// xu ly cau len - 7b.Load cả file từ local lên bảng staging
				connection_user.commit();
				// commit
				connection_user.close();
				//dong ket noi
			} catch (SQLException e1) {
			}
		}
		public static void loadFileExcel(int idconfig,int id_log,String sourceConfig,String delimiter,String desConfig,String user,String password,String tableName) throws EncryptedDocumentException, IOException{
			try {
				if(delimiter.equals("comma")) {
					delimiter =",";
				}
				//neu delimiter la comma thi doi sang dau phay
				String afterConvert = convertToCsv(sourceConfig);
				//convert file excel sang csv va tra ve duong dan toi file csv
				File file = new File(afterConvert);
				// tao doi tuong file tu duong dan file csv
				String source = file.getParent()+"\\\\"+file.getName();
				// lay ra duong dan toi source them 2 dau slash
				Connection connection_user = null;
				try {
				connection_user = DriverManager.getConnection(desConfig, user, password);
				//mo connection - 6.Thực hiện kết nối tới staging
				}catch (Exception e) {
					//7a.Gửi mail thông báo kết nối tới staging bị lỗi
				}
				connection_user.setAutoCommit(false);
				// set autocommit false
				String command = " load data infile "+"'"+source+"'"+" into table staging." +tableName+" FIELDS TERMINATED BY '"+delimiter+"' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES;";
				//tao cau lenh load ca file vao staging
				PreparedStatement stat = connection_user.prepareStatement(command);
				//tao preparedStatement nhan vao query
				System.out.println(command);
				stat.execute();
				//thuc thi cau query - 7b.Load cả file từ local lên bảng staging
				connection_user.commit();
				//commit - 7b.Load cả file từ local lên bảng staging
				connection_user.close();
				//dong ket noi
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		public static void main(String[] args) throws EncryptedDocumentException, IOException {
//			LoadToStaging ltd = new LoadToStaging();
//			ltd.convertToCsv("D:\\localpaging\\\\LopHoc_S_03.xlsx");
		}
}
