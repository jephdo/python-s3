class Config:
    TIMEZONE = None

    AWS_SECRET_ACCESS_KEY = None
    AWS_ACCESS_KEY_ID = None


config = Config()


def set_aws_keys(aws_access_key_id, aws_secret_access_key):
    config.AWS_ACCESS_KEY_ID = aws_access_key_id
    config.AWS_SECRET_ACCESS_KEY = aws_secret_access_key


def set_timezone(timezone):
    import pytz
    config.TIMEZONE = pytz.timezone(timezone)