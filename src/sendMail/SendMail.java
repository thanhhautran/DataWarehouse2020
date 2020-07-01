package sendMail;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class SendMail {
public static boolean sendMailToVertify(String to, String subject, String text) throws AddressException, MessagingException {
		
		//tao mail session
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "25");
        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("datawarehousenhom11chieu@gmail.com", "123456warehouse");
            }
        });
       
        	//tao mail
            Message message = new MimeMessage(session);//tao kieu file gui
            message.setSubject(subject);//tao tieu de
            message.setText(text);//tao than
            //tao nguoi gui nguoi nhan
            message.setHeader("Content-Type", "text/plain; charset=UTF-8");
            message.setFrom(new InternetAddress("jackgreenbee@gmail.com"));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            //gui
            Transport.send(message);
       
        return true;
		}
}
