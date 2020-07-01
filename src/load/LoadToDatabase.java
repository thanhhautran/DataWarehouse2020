package load;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import connectionDatabase.BaseConnection;
import preprocessingInStaging.Preprocessing;
import sendMail.SendMail;
import updateLogAndConfig.UpdateLog;


public class LoadToDatabase {
		public static void main(String[] args) throws EncryptedDocumentException, IOException {
			LoadToDatabase ltd = new LoadToDatabase();
		}
		public boolean fileIsExsist(String config) {
			String[] part = config.split("\t");
			File file = new File(part[3]);
			if(file.exists()) {
				return true;
			}else {
			return false;
			}
		}
		public static int getNumColOfTable(String tableName,String desConfig,String user,String password) throws SQLException {
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
		public static String getListFileLoad() {
			String list ="";
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
	        try {
				Connection connection_user = BaseConnection.getMySQLConnection();
				connection_user.setAutoCommit(false);
				PreparedStatement stat = connection_user.prepareStatement("select * from configtable left join logtab on logtab.idconfig = configtable.idconfig where logtab.status_file = 'ok_toStaging';");
				  stat.execute();
				ResultSet rs =  stat.getResultSet();
				while(rs.next()) {
					idConfig = rs.getInt(1);//0
					username = rs.getString(2);//1
					password = rs.getString(3);//2
					localdir = rs.getString(6);//3
					des_config = rs.getString(7);//4
					fileType = rs.getString(8);//5
					delimiter = rs.getString(9);//6
					listColumn = rs.getString(10);//7
					listWarehouseRequireCol = rs.getString(11);//8
					Staging_tabName = rs.getString(12);//9
				list = idConfig+"\t"+username+"\t"+password+"\t"+localdir+"\t"+des_config+"\t"+fileType+"\t"+delimiter+"\t"+listColumn+"\t"+listWarehouseRequireCol+"\t"+Staging_tabName+"\n"; 
				}
				connection_user.close();
			} catch (SQLException | ClassNotFoundException e1) {
				e1.printStackTrace();
			}
			return list;
		}
		public static void loadToLocal(String config) throws EncryptedDocumentException, NumberFormatException, IOException, SQLException {
			UpdateLog ul = new UpdateLog();
			String[] fields = config.split("\t");
			String log = getLogOf(1);
			String[] f_log = log.split(",");
			switch (fields[5]) {
			case ".csv": {	
				try {
				loadFileCsv(Integer.parseInt(fields[0]),Integer.parseInt(f_log[0]),fields[3],fields[6], fields[4],fields[1], fields[2], fields[9]);
					int numcol = getNumColOfTable(fields[9], fields[7], fields[1],fields[2]);
					ul.updateLogWhenSuccess(Integer.parseInt(f_log[0]),numcol);
					SendMail.sendMailToVertify("", "load file thanh cong", "");
				}catch (Exception e) {
					ul.updateLogWhenFail(Integer.parseInt(f_log[0]));
				}
			}
			case ".xlsx": {
				try {
				loadFileExcel(Integer.parseInt(fields[0]),Integer.parseInt(f_log[0]),fields[3],fields[6], fields[4],fields[1], fields[2], fields[9]);
					int numcol = getNumColOfTable(fields[9], fields[7], fields[1],fields[2]);
					ul.updateLogWhenSuccess(Integer.parseInt(f_log[0]), numcol);
					SendMail.sendMailToVertify("", "load file thanh cong", "");
				}catch (Exception e) {
					ul.updateLogWhenFail(Integer.parseInt(f_log[0]));
				}
			}
			case ".txt": {
				try {
				loadFileTxt(Integer.parseInt(fields[0]),Integer.parseInt(f_log[0]),fields[3],fields[6], fields[4],fields[1], fields[2], fields[9]);
					int numcol = getNumColOfTable(fields[9], fields[7], fields[1],fields[2]);
					ul.updateLogWhenSuccess(Integer.parseInt(f_log[0]), numcol);
					SendMail.sendMailToVertify("", "load file thanh cong", "");
				}catch (Exception e) {
					ul.updateLogWhenFail(Integer.parseInt(f_log[0]));
				}
			}
				}
		}
		
		public static String getLogOf(int idConfig) {
			String log =null;
	        int id_logtab =0;
	        int idconfig = 0;
	        String statusFile ="";
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
			        timeLoadStaging =rs.getString(4);
			        timeLoadWarehouse=rs.getString(5);
			        numColHaveLoad =rs.getString(6);
			        log =id_logtab+","+idconfig+","+statusFile+","+timeLoadStaging+","+timeLoadWarehouse+","+numColHaveLoad+"\n";
				}
				connection_user.close();
				
			} catch (SQLException | ClassNotFoundException e1) {
				e1.printStackTrace();
			}
			return log;
		}
		public static String convertToCsv(String path) throws EncryptedDocumentException, IOException {
			File file = new File(path);
			InputStream is = new FileInputStream(file);
			Workbook wb = WorkbookFactory.create(is);

	        Sheet sheet = wb.getSheetAt(0);
	        Iterator<Row> rowIterator = sheet.iterator();
	        String str = "";
	        while(rowIterator.hasNext()) {
	        	Row fRow = rowIterator.next();
	        	Iterator<Cell> cellIterator = fRow.iterator();
	        	while(cellIterator.hasNext()) {
	        		Cell cell = cellIterator.next();
	        		str += cell+",";
	        	}
	        	str += "\n";
	        }
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
				PreparedStatement stat = connection_user.prepareStatement("load data infile "+"'"+sourceConfig+"'"+" into table " +tableName+" CHARACTER SET latin1 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (STT,MSSV,HoLot,Ten,Ngaysinh,Malop,lop,sodienthoai,email,Quequan,ghichu);");
				stat.execute();
				connection_user.commit();
				connection_user.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
	        
		}
		public static void loadFileTxt(int idconfig,int id_log,String sourceConfig,String delimiter,String desConfig,String user,String password,String tableName) {
			try {
				Connection connection_user = DriverManager.getConnection(desConfig, user, password);
				connection_user.setAutoCommit(false);
				if(delimiter.equals("comma")) {
					delimiter =",";
				}
				PreparedStatement stat = connection_user.prepareStatement("load data infile "+"'"+sourceConfig+"'"+" into table " +tableName+" CHARACTER SET latin1 FIELDS TERMINATED BY '"+delimiter+"' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n' IGNORE 1 LINES (STT,MSSV,HoLot,Ten,Ngaysinh,Malop,lop,sodienthoai,email,Quequan,ghichu);");
				stat.execute();
				connection_user.commit();
				connection_user.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
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
				System.out.println(desConfig+" "+user+" "+password);
				Connection connection_user = DriverManager.getConnection(desConfig, user, password);
				connection_user.setAutoCommit(false);
				System.out.println("load data infile "+"'"+source+"'"+" into table " +tableName+" CHARACTER SET latin1 FIELDS TERMINATED BY '"+delimiter+"' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (STT,MSSV,HoLot,Ten,Ngaysinh,Malop,lop,sodienthoai,email,Quequan,ghichu);");
				PreparedStatement stat = connection_user.prepareStatement("load data infile "+"'"+source+"'"+" into table " +tableName+" CHARACTER SET latin1 FIELDS TERMINATED BY '"+delimiter+"' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES (STT,MSSV,HoLot,Ten,Ngaysinh,Malop,lop,sodienthoai,email,Quequan,ghichu);");
				stat.execute();
				connection_user.commit();
				connection_user.close();
				
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		public void cleanData() {
			Preprocessing ppc = new Preprocessing();
			ppc.replaceMissingValue();
		}
		public void combineData(String config) {
			Preprocessing ppc = new Preprocessing();
			ppc.combineValue(config);
		}
}
