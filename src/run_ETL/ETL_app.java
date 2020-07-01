package run_ETL;

import java.util.List;

import javax.mail.MessagingException;

import load.LoadToDatabase;
import sendMail.SendMail;
import updateLogAndConfig.UpdateLog;

public class ETL_app {
	public void localToStaging() {
		LoadToDatabase ltd = new LoadToDatabase();
		String listfile = ltd.getListFileLoad();
		String[] fileSplit = listfile.split("\n");
		for(int i=0;i<1;i++) {
			if(ltd.fileIsExsist(fileSplit[i])) {
				try {
					System.out.println(fileSplit[i]);
					ltd.loadToLocal(fileSplit[i]);
//					ltd.combineData(fileSplit[i]);
//					ltd.cleanData();
				}catch (Exception e) {
				}
			}else {
				try {
					SendMail.sendMailToVertify("", "Loi load file tu local len staging", "");
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
