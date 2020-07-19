package run_ETL;



import javax.mail.MessagingException;

import load.LoadToDatabase;
import sendMail.SendMail;
import updateLogAndConfig.UpdateLog;

public class ETL_app {
	public void localToStaging() {
		LoadToDatabase ltd = new LoadToDatabase();
		UpdateLog ul = new UpdateLog();
		String listfile = ltd.getListFileLoad();
		String[] fileSplit = listfile.split("\n");
		for(int i=0;i<1;i++) {
			if(ltd.fileIsExsist(fileSplit[i])) {
				try {
					ltd.loadToLocal(fileSplit[i]);
					if(ltd.isEqualColNum(fileSplit[i])) {
//					ltd.addAndInsert(fileSplit[i]);
//						ltd.cleanData();
					}else {
						SendMail.sendMailToVertify("17130059", "Load Khong du dong ", "");
					}
				}catch (Exception e) {
				}
			}else {
				try {
					SendMail.sendMailToVertify("17130059", "Loi load file tu local len staging", "");
				} catch (MessagingException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	public static void main(String[] args) {
		ETL_app ea = new ETL_app();
		ea.localToStaging();
	}
}
