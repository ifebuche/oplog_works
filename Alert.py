import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv

load_dotenv()

import requests


class Alert:

    @staticmethod
    def slack(message):

        print("Sending Slack alert")
        slack_hook = os.getenv('slack_hook')
        print(slack_hook)
        slack_message = {'text': message}

        response = requests.post(
            slack_hook,
            data=json.dumps(slack_message),
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code != 200:
            print(f"Failed to send Slack alert. Response: {response.text}")

    @staticmethod
    def email(email_addresses: dict, message: str, subject='Success'):
        """

        Args:
            email_addresses (dict): dictionary contiaining email address and name
            message (str): email body
            subject (str, optional): email body. Defaults to 'Success'.
        """

        print("Sending email alert")
        from_address = os.getenv('from_email')
        password = os.getenv('mail_passwod')
        print('Retrieved email details')
        
        for item, name in email_addresses.item():
            from_addr = from_address
            to_addr = item

            msg = MIMEMultipart()
            msg['From'] = "MI-ETL"
            msg['To'] = to_addr

            if subject != 'Success':
                subject = 'Failure'

            msg['Subject'] = f"Oplog Pipeline Cron: {subject}"
            body = message
            msg.attach(MIMEText(body, 'plain'))

            try:
                with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                    server.login(from_addr, password)
                    server.send_message(msg)
                print(f"Email sent to {name} - {item}")
            except Exception as e:
                print(f"Failed to send email to {name} - {item}. Error: {str(e)}")

    @staticmethod
    def sms(message):
        print("Sending SMS alert")

