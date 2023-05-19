class Alert:
    @staticmethod
    def slack(message):
        print("Sending Slack alert")

    @staticmethod
    def email(message):
        print("Sending email alert")
    @staticmethod
    def sms(message):
        print("Sending SMS alert")
