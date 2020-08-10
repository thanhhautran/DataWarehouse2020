package run_ETL;

import java.sql.SQLException;
import java.util.Map;
import java.util.Scanner;

import javax.mail.MessagingException;


import load.LoadToStaging;
import loadFromStagingToWarehouse.StagingToWarehouse;
import sendMail.SendMail;
import updateLogAndConfig.UpdateLog;

public class RunManualETL {
	public int localToStaging(int id) {
		//phuong thuc localToStaging load tu local sang staging nhan vao configid va tra ve 1 configid 
		LoadToStaging ltd = new LoadToStaging();
		//new ra mot doi tuong cua class LoadToStaging de goi phuong thuc ben trong
		UpdateLog ul = new UpdateLog();
		//new ra mot doi tuong cua class UpdateLog de goi phuong thuc ben trong
		Map<String, String> listConfig = ltd.getFileToLoadStagingById(id); //1.Mở kết nối tới Database control + 2.Lấy ra 1list config có thuộc tính statusfile trong bảng log là 'ready_to_staging'
		// lay ra mot file tu configlog
			if(ltd.fileIsExsist(listConfig)) {
				//kiem tra xem file co ton tai ko - 3.Kiểm tra đường dẫn lưu trong thuộc tính filePathLocal trong bảng log có nằm trong thư  mục local hay không
				try {
					ltd.loadToLocal(listConfig);//4b.Kiểm tra loại file và delimiter dựa vào các thuộc tính trên bảng config 
					//+ 5.Lấy các thuộc tính cần thiết trong bảng config đưa vào phương thức load data vào staging
					//loadfile tu local len staging + 6.Thực hiện kết nối tới staging + 7b.Load cả file từ local lên bảng staging
					if(ltd.isEqualColNum(listConfig)) {
						//kiem tra so dong xem co load du dong hay khong - 8.Kiểm tra số dòng đã load vào bảng tạm trong staging
					int colNum = ltd.getNumColOfTable(listConfig.get("Staging_tabName"),listConfig.get("des_config"),listConfig.get("username"),listConfig.get("password"));
					ul.updateLogWhenSuccess(Integer.parseInt(listConfig.get("idlogtab")), colNum);
						//neu thanh cong ghi log lai so dong va thong bao thanh cong - 9b.Gửi mail thông báo thành công & cập nhật lại log của file, số dòng load thành công
					}else {
						SendMail.sendMailToVertify("17130059", "Load Khong du dong ", "");
						//neu that bai ghi log lai so dong bang 0 va thong bao that bai - 9.aGửi mail thông báo load không đủ dòng
					}
				}
				catch (Exception e) {
					e.printStackTrace();
					ul.updateLogWhenFail(Integer.parseInt(listConfig.get("idlogtab")));
					//update log that bai khi tim thay loi trong ngoai le - 9.aGửi mail thông báo load không đủ dòng
				}
			}else {
				try {
					SendMail.sendMailToVertify("17130059", "Loi load file tu local len staging", "");
					//4a.Gửi mail thông báo source không có trong local
				} catch (MessagingException e) {
					e.printStackTrace();
				}
			}
			//tra ve mot so int chinh la id cua config
		return Integer.parseInt(listConfig.get("idConfig"));	
	}
	public void loadStagingToWarehouse(int id) throws SQLException, ClassNotFoundException {
		StagingToWarehouse stw = new StagingToWarehouse();
		//new ra mot doi tuong cua class StagingToWarehouse de goi phuong thuc ben trong
		Map<String, String> map = stw.getSTReadyToWarehouse(id);
		//1.Mở kết nối tới Database control 
		//+ 2.Lấy ra 1list config thuộc tính flag trong bảng config là run và thuộc tính statusfile trong bảng log là 'ready_to_warehouse'
		//lay ra mot dong config trong control database
		stw.LoadToWarehouse(map);
		//+3.Lấy ra kết nối, user, password, tên procedure trong bảng config kiểm tra xem đã tồn tại hay chưa
		//+4.thực hiện truy vấn tất cả các dòng trong bảng tạm staging trả về result set
		//+5.Gọi phương thức để insert từng dòng trong table staging vào trong bảng dim ở warehouse với tham số là giá trị đầu vào , tên procedure, connection, user, password
		//+7. lấy ra connection, user,password, tên table staging trong bảng config để lấy ra connection tới warehouse
		//+8.gọi một procedure trong database với tham số đầu vào là  từng dòng trong ResultSet ở step 4
		//+9.Update log thành công.
		//load data tu bang staging sang warehouse
	}
	public void ETL(int id) throws SQLException, ClassNotFoundException {
		//phuong thuc ETL de thuc hien qua trinh extract,transform,load 
		int idGenerate = localToStaging(id);
		//lay ket qua tra ve sau khi thuc thi phuong thuc localToStaging()
		loadStagingToWarehouse(idGenerate);
		//phuong thuc loadStagingToWarehouse nhan vao ket qua tra ve cua phuong thuc localToStaging sau do
		// thuc hien tung column tu staging sang warehouse
	}
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		RunManualETL ea = new RunManualETL();
		//goi the hien tu class RunManualETL de goi phuong thuc ben trong
		System.out.println("Nhap theo cu phan ETL_id");
		//dua ra huong dan ten command line huong dan nguoi dung thuc hien
		Scanner scn = new Scanner(System.in);
		//tao scanner nhan ket qua nhap tu ban phim
		String command = scn.nextLine();
		//bien command lay ra dong ma nguoi dung vua nhap vao
		String[] split = command.split("\t");
		//mang array split chia bien command theo ky tu '\t' lam 2 thanh phan split[0] = ETL va split[1] = idconfig
		int id = Integer.parseInt(split[1]);
		// tao bien id co gia tri la split[1]
		ea.ETL(id);
		//thuc thi phuong thuc ETL voi tham so dau vao la bien id
	}
}
