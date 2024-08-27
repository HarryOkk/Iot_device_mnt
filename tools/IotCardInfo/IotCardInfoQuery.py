import remes_aliyun_openapi.dyiotapi.query as IotCardTools
import pandas as pd
import datetime
import requests
import json
from time import sleep


def send_msg_sucess(msg):
    url = 'https://oapi.dingtalk.com/robot/send?access_token=245dcf595bf6719a407f1fd196c316293c013f0c3d2a55414e6430c5e62c11e4'
    headers = {'Content-Type': 'application/json;charset=utf-8'}
    data = {
        "msgtype": "text",
        "text": {'content': msg},
        "at": {
            "atMobiles": ['17855320298'],
            "atUserIds": ["2bm-nqjf0yafb"],
            "isAtAll": False  # 此处 为是否@所有人
        }
    }
    r = requests.post(url, data=json.dumps(data), headers=headers)
    return r.text


def Process1():
    df = pd.read_excel('info_list.xlsx')

    result = IotCardTools.query_card_history_flow_info(iccid=df['ICCID'][0], start_time='202408', end_time='202409')

    if result['body']['Success']:
        print(result['body']['Data'])
        d_flow_query_info = result['body']['Data'][0]
        # 接口返回处理
        l_msg_output = "ICCID：" + str(df['ICCID'][0]) + "\n"
        l_msg_output += "DTU：" + str(df['DTU'][0]) + "\n"
        l_msg_output += "MNT_BUID_NAME：" + str(df['MNT_BUID_NAME'][0]) + "\n"
        l_msg_output += "当前日期：" + datetime.date.today().strftime('%Y-%m-%d') + "\n"
        l_msg_output += "当月流量已使用：" + str(d_flow_query_info['CurValue'] / (1024 * 1024)) + " GB" + "\n"
        # 查询当前系统日期
        l_msg_output += "本日流量已使用：" + str(
            d_flow_query_info['DayUsageList'][datetime.date.today().day - 1]['Value'] / 1024) + " MB" + "\n"
        print(l_msg_output)
        r = send_msg_sucess(l_msg_output)
        sleep(3)
        print(r)
    else:
        print(result['body']['Message'])
        print(result['body']['Code'])


if __name__ == '__main__':
    # 创建一个每日三点执行的定时任务
    Process1()
    while (1):
        if datetime.datetime.now().hour == 15:
            Process1()
            sleep(3600)


