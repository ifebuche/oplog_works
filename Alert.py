import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests


class Alert:

    @staticmethod
    def slack(message):

        print("Sending Slack alert")
        slack_hook = os.getenv('slack_hook')
    
        slack_message = {'text': message}

        response = requests.post(
            slack_hook,
            data=json.dumps(slack_message),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code != 200:
            print(f"Failed to send Slack alert. Response: {response.text}")

    @staticmethod
    def email(email_addresses: dict, data: dict, message: str, from_add:str,
               password:str, time_of_run, total_extract_seconds,
               total_collections_extracted, total_collections, subject='Success'):
        

        """

        Args:
            email_addresses (dict): dictionary contiaining email address and name
            message (str): email body
            subject (str, optional): email body. Defaults to 'Success'.
        """

        print("Sending email alert")
        from_address = from_add
        password = password
        print('Retrieved email details')

        # Making some modifications to accomadate the email format.
        formatted_message = f'''\
        <html>
        <body>
            <h2>Run Summary</h2>
            <p><strong>Time of Run:</strong> {time_of_run}</p>
            <p><strong>Total Time of Run (s):</strong> {total_extract_seconds}</p>
            <p><strong>Total Collections Extracted:</strong> {total_collections_extracted}</p>
            <p><strong>Collections per Document:</strong></p>
            <ul>
        '''
        for collection, item in data.items():
            formatted_message += f'        <li>{collection} ({len(item)})</li>\n'

        formatted_message += '''\
            </ul>
        </body>
        </html>
        '''
        
        for name, item in email_addresses.items():
            from_addr = from_address
            to_addr = item

            msg = MIMEMultipart()
            msg['From'] = "MI-ETL"
            msg['To'] = to_addr

            if subject != 'Success':
                subject = 'Failure'

            msg['Subject'] = f"Oplog Pipeline Cron: {subject}"
            # body = message
            msg.attach(MIMEText(formatted_message, 'html'))

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

