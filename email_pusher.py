import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config_cred import RECIPIENT_LIST
from config_cred import EMAIL_SERVER_CONFIG


def send_email(msg, recipient_list=RECIPIENT_LIST):
    try:
        # set up the SMTP server
        smtp_server = smtplib.SMTP(host=EMAIL_SERVER_CONFIG['host'], port=EMAIL_SERVER_CONFIG['port'])
        smtp_server.starttls()
        smtp_server.login(EMAIL_SERVER_CONFIG['user-name'], EMAIL_SERVER_CONFIG['password'])
        print("Successfully connected to the SMTP server at %s:%s !!"
              % (EMAIL_SERVER_CONFIG['host'], EMAIL_SERVER_CONFIG['port']))

        # create a multi-part email message
        email_message = MIMEMultipart()
        # setup the parameters of the message
        email_message['From'] = EMAIL_SERVER_CONFIG['user-name']
        email_message['To'] = ", ".join(recipient_list)
        email_message['Subject'] = "CUrW Weather Station inconsistency Alert"
        print(email_message['Subject'])
        # add in the message body
        email_message.attach(MIMEText(msg, 'plain'))

        # send the message via the server set up earlier.
        smtp_server.send_message(email_message)
        #smtp_server.sendmail(email_message)
        print("Successfully sent the email notifications!!")
        smtp_server.quit()

    except Exception as ex:
        print("Error while sending email notifications: ", ex)

