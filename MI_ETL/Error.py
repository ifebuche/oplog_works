class OplogWorksError(Exception):
    def __init__(self, func_name, issue):
        self.func_name = func_name
        self.issue = issue

    def __str__(self):
        return f"Error at {self.func_name}: \nIssue => {self.issue}"
