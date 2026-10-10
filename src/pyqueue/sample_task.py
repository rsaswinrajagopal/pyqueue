from pyqueue.task import task


@task
def send_email():
    pass


send_email.delay("user@domain.com", subject="Welcome!")
