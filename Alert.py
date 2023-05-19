class Alert:
    @staticmethod
    def slack(message):
        print("Sending Slack alert")

    @staticmethod
    def email(message):
        print("Sending email alert")
    
    def sms(message):
        print("Sending SMS alert")
