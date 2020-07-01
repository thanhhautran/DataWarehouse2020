package load;


import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.jcraft.jsch.*;

public class DowloadFile {
		public static void checkSum(InputStream fis) throws IOException, NoSuchAlgorithmException {
//			String filepath = "D:\\localpaging\\test.xlsx";
//			 
	        MessageDigest messageDigest = MessageDigest.getInstance("SHA1");
//	         
//	        FileInputStream fileInput = new FileInputStream(filepath);
	        byte[] dataBytes = new byte[1024];
	 
	        int bytesRead = 0;
	 
	        while ((bytesRead = fis.read(dataBytes)) != -1) {
	            messageDigest.update(dataBytes, 0, bytesRead);
	        }
	         
	 
	        byte[] digestBytes = messageDigest.digest();
	 
	        StringBuffer sb = new StringBuffer("");
	         
	        for (int i = 0; i < digestBytes.length; i++) {
	            sb.append(Integer.toString((digestBytes[i] & 0xff) + 0x100, 16).substring(1));
	        }
	 
	        System.out.println("Checksum for the File: " + sb.toString());
	         
	        fis.close();
	 
		}
		 public void dowloadFromServer(){  
			    FileOutputStream fos=null;
			    try{
			      String user= "guest_access";
			      String host="drive.ecepvn.org";
			      String rfile="drive.ecepvn.org:/volume1/ECEP/song.nguyen/DW_2020/data/17130106_sang_nhom1.txt";
			      String lfile="D:\\localpaging\\17130106_sang_nhom1.txt";
			      System.out.println(user);

			      String prefix=null;
			      if(new File(lfile).isDirectory()){
			        prefix=lfile+File.separator;
			      }
			      
			      JSch jsch=new JSch();
			      Session session=jsch.getSession(user, host, 2227);
			      java.util.Properties config = new java.util.Properties();
                  config.put("StrictHostKeyChecking", "no");
                  session.setConfig(config);
			      session.setPassword("123456");
			      session.connect();

//			      rfile="'"+rfile+"'";
			      String command="scp -P "+rfile;
			      Channel channel=session.openChannel("exec");
			      ((ChannelExec)channel).setCommand(command);

			      // get I/O streams for remote scp
			      OutputStream out=channel.getOutputStream();
			      InputStream in=channel.getInputStream();
			      channel.connect();

			      byte[] buf=new byte[1024];

			      // send '\0'
			      buf[0]=0; out.write(buf, 0, 1); 
			      out.flush();

			      while(true){
				int c=checkAck(in);
			        if(c!='C'){
				  break;
				}

			        // read '0644 '
			        in.read(buf, 0, 5);

			        long filesize=0L;
			        while(true){
			          if(in.read(buf, 0, 1)<0){
			            // error
			            break; 
			          }
			          if(buf[0]==' ')break;
			          filesize=filesize*10L+(long)(buf[0]-'0');
			        }

			        String file=null;
			        for(int i=0;;i++){
			          in.read(buf, i, 1);
			          if(buf[i]==(byte)0x0a){
			            file=new String(buf, 0, i);
			            break;
			  	  }
			        }

				//System.out.println("filesize="+filesize+", file="+file);

			        // send '\0'
			        buf[0]=0; out.write(buf, 0, 1); out.flush();

			        // read a content of lfile
			        fos=new FileOutputStream(prefix==null ? lfile : prefix+file);
			        int foo;
			        while(true){
			          if(buf.length<filesize) foo=buf.length;
				  else foo=(int)filesize;
			          foo=in.read(buf, 0, foo);
			          if(foo<0){
			            // error 
			            break;
			          }
			          fos.write(buf, 0, foo);
			          filesize-=foo;
			          if(filesize==0L) break;
			        }
			        fos.close();
			        fos=null;

				if(checkAck(in)!=0){
				  System.exit(0);
				}

			        // send '\0'
			        buf[0]=0; out.write(buf, 0, 1); out.flush();
			      }

			      session.disconnect();

			      System.exit(0);
			    }
			    catch(Exception e){
			      System.out.println(e);
			      try{if(fos!=null)fos.close();}catch(Exception ee){}
			    }
			  }

			  static int checkAck(InputStream in) throws IOException{
			    int b=in.read();
			    // b may be 0 for success,
			    //          1 for error,
			    //          2 for fatal error,
			    //          -1
			    if(b==0) return b;
			    if(b==-1) return b;

			    if(b==1 || b==2){
			      StringBuffer sb=new StringBuffer();
			      int c;
			      do {
				c=in.read();
				sb.append((char)c);
			      }
			      while(c!='\n');
			      if(b==1){ // error
				System.out.print(sb.toString());
			      }
			      if(b==2){ // fatal error
				System.out.print(sb.toString());
			      }
			    }
			    return b;
			  }
			public static void main(String[] args) throws NoSuchAlgorithmException, IOException {
				DowloadFile df = new DowloadFile();
//				df.dowloadFromServer();
				String filepath = "D:\\localpaging\\test.txt";
				FileInputStream fis = new FileInputStream(filepath);
				df.checkSum(fis);
//				0003a1e86ff393c217a157d30e66879cf81d83fa
			}
}
