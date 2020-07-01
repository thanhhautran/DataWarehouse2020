package load;

import java.util.Arrays;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class DowloadFromServer {
	public void downloadFtp(String userName, String password, String host, int port, String path) {


        Session session = null;
        Channel channel = null;
        try {
            JSch ssh = new JSch();
            JSch.setConfig("StrictHostKeyChecking", "no");
            session = ssh.getSession(userName, host, port);
            session.setPassword(password);
            session.connect();
            channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftp = (ChannelSftp) channel;
            sftp.get(path, "D:\\localpaging");
        } catch (JSchException e) {
            System.out.println(userName);
            e.printStackTrace();
        } catch (SftpException e) {
            System.out.println(userName);
            e.printStackTrace();
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
	}
	public void scpFile(String host, String username, String password, String src, String dest) throws Exception {

	    String[] scpCmd = new String[]{"expect", "-c", String.format("spawn scp -r %s %s@%s:%s\n", src, username, host, dest)  +
	            "expect \"?assword:\"\n" +
	            String.format("send \"%s\\r\"\n", password) +
	            "expect eof"};

	    ProcessBuilder pb = new ProcessBuilder(scpCmd);
	    System.out.println("Run shell command: " + Arrays.toString(scpCmd));
	    Process process = pb.start();
	    int errCode = process.waitFor();
	    System.out.println("Echo command executed, any errors? " + (errCode == 0 ? "No" : "Yes"));
//	    System.out.println("Echo Output:\n" + output(process.getInputStream()));
	    if(errCode != 0) throw new Exception();
	}
	public static void main(String[] args) {
		DowloadFromServer dfs = new DowloadFromServer();
		dfs.downloadFtp("guest_access","123456", "115.78.8.83", 2227, "http://drive.ecepvn.org:5000/d/f/558307832617766929//sinhvien_chieu_nhom3.xlsx");
	}
}
