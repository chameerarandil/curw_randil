
DATABASE_CONFIG = {
    'MYSQL_HOST': '35.197.98.125',
    'MYSQL_USER': 'routine_user',
    'MYSQL_PASSWORD': 'aquaroutine',
    'MYSQL_DB': 'curw_obs'
}

EMAIL_SERVER_CONFIG = {
    'host': 'smtp.gmail.com',
    'port': 587,
    'user-name': 'curwalerts@gmail.com',
    'password': 'eyfwpozounllwhhq'
}

RECIPIENT_LIST = ['chameerarandil@gmail.com']

EMAIL_ALERT_TEMPLATE_1 = """
Dear All,

Please note that excessive rainfalls have been recorded at the following weather stations. 

%s 

Thanks,
curwalerts. """

EMAIL_ALERT_TEMPLATE_2 = """
Dear All,

Please kindly note that,

%s has started to respond at : %s


Thanks,
curwalerts. """
