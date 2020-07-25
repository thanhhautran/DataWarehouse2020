package run_ETL;



import java.sql.SQLException;
import java.util.Map;

import javax.mail.MessagingException;

import load.LoadToDatabase;
import loadFromStagingToWarehouse.StagingToWarehouse;
import sendMail.SendMail;
import updateLogAndConfig.UpdateLog;

public class ETL_app {
	public int localToStaging() {
		LoadToDatabase ltd = new LoadToDatabase();
		UpdateLog ul = new UpdateLog();
		Map<String, String> listConfig = ltd.getFileToLoadStaging();
		for(int i=0;i<1;i++) {
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
		}
		return Integer.parseInt(listConfig.get("idConfig"));	
	}
	public void loadStagingToWarehouse(int id) throws SQLException {
		StagingToWarehouse stw = new StagingToWarehouse();
		Map<String, String> map = stw.getSTReadyToWarehouse(id);
		stw.LoadToWarehouse(map);
	}
	public void ETL() throws SQLException {
		int id = localToStaging();
		loadStagingToWarehouse(id);
	}
	public static void main(String[] args) throws SQLException {
		ETL_app ea = new ETL_app();
		ea.ETL();
	}
}
