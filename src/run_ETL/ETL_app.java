package run_ETL;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import javax.mail.MessagingException;

import load.LoadToStaging;
import loadFromStagingToWarehouse.StagingToWarehouse;
import sendMail.SendMail;
import updateLogAndConfig.UpdateLog;

public class ETL_app {
	public int localToStaging() {
		LoadToStaging ltd = new LoadToStaging();
		UpdateLog ul = new UpdateLog();
		Map<String, String> listConfig = ltd.getFileToLoadStaging();
			if(ltd.fileIsExsist(listConfig)) {
				try {
					ltd.loadToLocal(listConfig);
					if(ltd.isEqualColNum(listConfig)) {
					ul.updateLogWhenSuccess(1, 58);
					}else {
						SendMail.sendMailToVertify("17130059", "Load Khong du dong ", "");
					}
				}catch (Exception e) {
					ul.updateLogWhenFail(1);
				}
			}else {
				try {
					SendMail.sendMailToVertify("17130059", "Loi load file tu local len staging", "");
				} catch (MessagingException e) {
					ul.updateLogWhenFail(1);
				}
			}
		return Integer.parseInt(listConfig.get("idConfig"));	
	}
	public void loadStagingToWarehouse(int id) throws SQLException, ClassNotFoundException {
		StagingToWarehouse stw = new StagingToWarehouse();
		Map<String, String> map = stw.getSTReadyToWarehouse(id);
		stw.LoadToWarehouse(map);
	}
	public void truncateTable(String destination,String username,String password,String tableName) {
		try {
			Connection connection_user = DriverManager.getConnection(destination, username, password);
			connection_user.setAutoCommit(false);
			
			PreparedStatement stat = connection_user.prepareStatement("TRUNCATE TABLE "+tableName+";");
			stat.execute();
			
			connection_user.commit();
			connection_user.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public void ETL() throws SQLException, ClassNotFoundException {
		int idGenerate = localToStaging();
		loadStagingToWarehouse(idGenerate);
	}
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		ETL_app ea = new ETL_app();
		ea.ETL();
	}
}
