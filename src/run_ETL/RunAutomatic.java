package run_ETL;

import java.sql.SQLException;
import java.util.Map;

import javax.mail.MessagingException;

import load.LoadToStaging;
import loadFromStagingToWarehouse.StagingToWarehouse;
import sendMail.SendMail;
import updateLogAndConfig.UpdateLog;

public class RunAutomatic {
	public int localToStaging() {
		//phuong thuc localToStaging load tu local sang staging tra ve 1 configid 
		LoadToStaging ltd = new LoadToStaging();
		//new ra mot doi tuong cua class LoadToStaging de goi phuong thuc ben trong
		UpdateLog ul = new UpdateLog();
		//new ra mot doi tuong cua class UpdateLog de goi phuong thuc ben trong
		try {
			ltd.callRun();
		} catch (ClassNotFoundException | SQLException e1) {
			e1.printStackTrace();
		}
		//phuong thuc callrun() set flag run cho nhung process chua chay ETL 
		Map<String, String> listConfig = ltd.getFileToLoadStaging();
		// lay ra mot file tu configlog
			if(ltd.fileIsExsist(listConfig)) {
				//kiem tra xem file co ton tai ko
				try {
					ltd.loadToLocal(listConfig);
					//loadfile tu local len staging
					if(ltd.isEqualColNum(listConfig)) {
						//kiem tra so dong xem co load du dong hay khong
					ul.updateLogWhenSuccess(1, 58);
					//neu thanh cong ghi log lai so dong va thong bao thanh cong
					}else {
						SendMail.sendMailToVertify("17130059", "Load Khong du dong ", "");
						//neu that bai ghi log lai so dong bang 0 va thong bao that bai
					}
				}catch (Exception e) {
					ul.updateLogWhenFail(1);
					//update log that bai khi tim thay loi trong ngoai le
				}
			}else {
				try {
					SendMail.sendMailToVertify("17130059", "Loi load file tu local len staging", "");
				} catch (MessagingException e) {
					ul.updateLogWhenFail(1);
				}
			}
		return Integer.parseInt(listConfig.get("idConfig"));
		//tra ve mot so int chinh la id cua config
	}
	public void loadStagingToWarehouse(int id) throws SQLException, ClassNotFoundException {
		StagingToWarehouse stw = new StagingToWarehouse();
		//new ra mot doi tuong cua class StagingToWarehouse de goi phuong thuc ben trong
		Map<String, String> map = stw.getSTReadyToWarehouse(id);
		//lay ra mot dong config trong control database
		stw.LoadToWarehouse(map);
		//load data tu bang staging sang warehouse
	}
	public void ETL() throws SQLException, ClassNotFoundException {
		//phuong thuc ETL de thuc hien qua trinh extract,transform,load 
		int idGenerate = localToStaging();
		//lay ket qua tra ve sau khi thuc thi phuong thuc localToStaging()
		loadStagingToWarehouse(idGenerate);
		//phuong thuc loadStagingToWarehouse nhan vao ket qua tra ve cua phuong thuc localToStaging sau do
		// thuc hien tung column tu staging sang warehouse
	}
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		RunAutomatic ea = new RunAutomatic();
		//goi the hien tu class RunManualETL de goi phuong thuc ben trong
		ea.ETL();
		//thuc thi phuong thuc ETL
	}
}
