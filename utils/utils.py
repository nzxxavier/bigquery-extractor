import json
from datetime import datetime
from dingtalkchatbot.chatbot import DingtalkChatbot


def alarm(message):
    # webhook
    webhook = "https://oapi.dingtalk.com/robot/send?access_token=0a6039f7b4c69593a1dfca9d8d9900b483a7867a678e42a2b6813979b82f912b"
    # bot
    bot = DingtalkChatbot(webhook)
    # send
    bot.send_text(message, is_at_all=False, at_mobiles=["15927400635"])


def format_data(raw_data):
    data = []
    values = raw_data.values()
    index = 0
    for value in values:
        if isinstance(value, dict) or isinstance(value, list):
            data.append(json.dumps(value))
        elif value is None:
            if index in [1, 4, 5, 6, 7, 12]:
                data.append(0)
            else:
                data.append('')
        else:
            data.append(value)
        index += 1
    data.append(datetime.now())
    return data
