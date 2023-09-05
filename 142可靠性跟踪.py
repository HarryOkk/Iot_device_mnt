from remes_aliyun_openapi.iot.device_manage import query_device_by_sql, batch_query_device_detail
from remes_mysql.db_config import AliyunBizDb

from iot_device_mnt import query_devices_thing_status, SMEC_CPD, iot_instance_id, SMEC_GW
import pandas as pd
import time
from datetime import datetime
from remes_aliyun_openapi.iot.thing_model_use import query_device_property_data, query_device_property_status

existing_excel_file1 = r'D:\my_documents\Tableau_python_data\142可靠性跟踪装查询结果.xlsx'
existing_excel_file2 = r'D:\my_documents\Tableau_python_data\控制柜装置142固件物模型查询.xlsx'

Sheet = 'Sheet1'
timestamp_seconds = time.time()
current_timestamp_milliseconds = int(timestamp_seconds * 1000)
start_timestamp_milliseconds = (current_timestamp_milliseconds - 604800000)


# 从装置
def extract_colum_cpdid() -> list:
    xlsx = pd.ExcelFile(existing_excel_file1)
    # 该函数导出xlsx文件中所有sheet
    df = pd.read_excel(xlsx, Sheet)
    try:
        l_devices = df.iloc[:, 1].tolist()
    except:
        l_devices = []
        print(f"导出CPDID列表失败")
    print(f"从文件拿到的所有控制柜装置如下：{l_devices}")
    print(f'装置的一共有{len(l_devices)}台')
    return l_devices


# 查询列表cpdid的物模型属性并返回字典
def query_devices_thing_status_to_dict(function_block_id: str,
                                       thing_lable_name: str,
                                       l_devices: list[str],
                                       ) -> dict:
    d_response = {}
    for device_name in l_devices:
        if function_block_id == 'default':
            d_func_block = query_device_property_status(iot_instance_id=iot_instance_id,
                                                        product_key=SMEC_GW,
                                                        device_name=device_name)
        else:
            d_func_block = query_device_property_status(iot_instance_id=iot_instance_id,
                                                        product_key=SMEC_GW,
                                                        device_name=device_name,
                                                        function_block_id=function_block_id)
        if d_func_block['body']['Success']:
            l_thing_of_device = d_func_block['body']['Data']['List']['PropertyStatusInfo']
            for d_single_func_thing in l_thing_of_device:
                try:
                    if d_single_func_thing['Identifier'] == thing_lable_name:
                        d_response[device_name] = d_single_func_thing['Value']
                except KeyError:
                    d_response[device_name] = None
        else:
            print(f'设备{device_name}物模型{function_block_id}模块{thing_lable_name}查询结果为：', d_func_block['body']['Success'], d_func_block['body']['ErrorMessage'])
    return d_response


# 将毫秒级时间戳转换成标准时间格式（用于查询物模型履历）
def trans_milliseconds_to_formattedtime(timestamp_milliseconds: int) -> str:
    # 将毫秒级时间戳转换为日期时间对象
    timestamp_seconds = timestamp_milliseconds / 1000
    dt_object = datetime.datetime.fromtimestamp(timestamp_seconds)

    # 格式化日期时间对象为2023-07-24 12:37:15形式
    formatted_time = dt_object.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time


# 查询装置列表中每一个装置的ota固件版本，具体查询需求可以通过修改sql语句来指定
def query_device_module_version_by_SQL(device_ota_module: str,
                                       l_devices: list[str]) -> dict:
    d_devices_ota_module_version = {}
    for device in l_devices:
        # 查询的产品类型为SMEC_CPD
        device = str(device)
        sql = f"""
            select
                ota_module.version
            from
                device 
            where         
                ota_module.name = '{device_ota_module}'
            and
                name = '{device}' 
            and
                product_key = '{SMEC_GW}'
                """
        d_ota_module_version = query_device_by_sql(sql=sql, iot_instance_id=iot_instance_id)
        # print(d_ota_module_version)
        if d_ota_module_version['body']['Data']:
            l_ota_module_version = d_ota_module_version['body']['Data'][0]['OTAModules']
            for d_FirmwareVersion in l_ota_module_version:
                if not d_FirmwareVersion['FirmwareVersion'].find('js') or not d_FirmwareVersion['FirmwareVersion'].find(
                        'JS'):
                    d_devices_ota_module_version[device] = d_FirmwareVersion['FirmwareVersion']

        else:
            print(f'{device}设备OTA模块版本查询为空')

            d_devices_ota_module_version[device] = None
    return d_devices_ota_module_version


# 查询装置列表的在线状态，并将在线状态填写到表格对应的位置
def query_devices_status(l_devices: list[str]) ->dict:
    d_devices_status = {}
    chunk_size = 100  # 分片大小,因该接口每次调用最多查询100台装置，所以需要对list进行分片
    for i in range(0, len(l_devices), chunk_size):
        chunk = l_devices[i:i + chunk_size]
        # 在这里对每个分片进行处理
        response_dev_mnt = batch_query_device_detail(SMEC_GW, chunk, iot_instance_id)
        if response_dev_mnt['body']['Success']:
            # 从返回值中拿到设备详细信息的列表
            l_device_detail = response_dev_mnt['body']['Data']['Data']
            for dict_dev_item in l_device_detail:
                d_devices_status[dict_dev_item['DeviceName']] = dict_dev_item['Status']
        else:
            print('装置在线状态查询接口调用失败，详细信息为：', response_dev_mnt['body'])
    return d_devices_status


# 查询142可靠性跟踪的装置
def query_4G_reliability_tracing():
    df = pd.read_excel(existing_excel_file1, Sheet)
    if len(df) > 0:
        l_devices = df['Devicename'].tolist()
        l_devices = list(filter(lambda x: x is not None, l_devices))
        print(l_devices)
        # 查询控制柜装置的软件版本
        d_js262_version = query_device_module_version_by_SQL(device_ota_module='js262_cpu',
                                                             l_devices=l_devices)
        # 查询表格中简版B装置的软件版本
        l_cpd_js268_b = []
        for k, w in d_js262_version.items():
            if w is None:
                l_cpd_js268_b.append(k)
        d_js268_b_version = query_device_module_version_by_SQL(device_ota_module='js268_b',
                                                               l_devices=l_cpd_js268_b)
        for key in d_js268_b_version.keys():
            d_js262_version[key] = d_js268_b_version[key]

        # 查询表格中监控室装置的软件版本
        l_dtu_js263 = []
        for k, w in d_js262_version.items():
            if w is None:
                l_dtu_js263.append(k)
        d_dtu_js263_version = query_device_module_version_by_SQL(device_ota_module='js263',
                                                                 l_devices=l_dtu_js263)
        for key in d_dtu_js263_version.keys():
            d_js262_version[key] = d_dtu_js263_version[key]
        js262_version_list = [d_js262_version.get(str(cpd_id)) for cpd_id in df['Devicename']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df['js262_version'] = js262_version_list

        # 查询装置的在线状态
        d_dev_online_status = query_devices_status(l_devices)
        print(d_dev_online_status)
        dev_online_status_list = [d_dev_online_status.get(str(Devicename)) for Devicename in df['Devicename']]
        df['Oline_Status'] = dev_online_status_list

        # 查询对应的物模型属性

        # 查询4G_FirewareVersion
        d_4G_FirewareVersion = query_devices_thing_status_to_dict(
            function_block_id='default',
            thing_lable_name='4G_FirewareVersion',
            l_devices=l_devices,
        )
        FirewareVersion_4G_list = [d_4G_FirewareVersion.get(int(Devicename)) for Devicename in df['Devicename']]
        # 将 4G_FirewareVersion 列表赋值给 df_mnt_info 的新列 '4G_FirewareVersion'
        df['4G_FirewareVersion'] = FirewareVersion_4G_list

        # 4G_HardwareType
        d_4G_HardwareType = query_devices_thing_status_to_dict(
            function_block_id='default',
            thing_lable_name='4G_HardwareType',
            l_devices=l_devices,
        )
        l_4G_HardwareType = [d_4G_HardwareType.get(int(Devicename)) for Devicename in df['Devicename']]
        # 将 4G_HardwareType 列表赋值给 df_mnt_info 的新列 '4G_HardwareType'
        df['4G_HardwareType'] = l_4G_HardwareType

        # 查询4G_IMEI
        d_4G_IMEI = query_devices_thing_status_to_dict(
            function_block_id='default',
            thing_lable_name='4G_IMEI',
            l_devices=l_devices,
        )
        l_4G_IMEI = [d_4G_IMEI.get(int(Devicename)) for Devicename in df['Devicename']]

        # 将 4G_IMEI 列表赋值给 df_mnt_info 的新列 '4G_IMEI'
        df['4G_IMEI'] = l_4G_IMEI

        # 将查询调用时间戳加入到df中
        df['4G模块信息收集时间'] = datetime.now()
        df['Devicename'] = df['Devicename'].astype(str)
        df.to_excel(existing_excel_file1, sheet_name=Sheet,
                    index=False)



    else:
        print('装置列表为空')
        pass


# 查询装置与4G模块相关的物模型属性
def query_CPD_4g_things():
    df = pd.read_excel(existing_excel_file2, Sheet)
    if len(df) > 0:
        l_devices = df['Devicename'].tolist()
        l_devices = list(filter(lambda x: x is not None, l_devices))
        print(l_devices)
        # 查询控制柜装置的软件版本
        d_js262_version = query_device_module_version_by_SQL(device_ota_module='js262_cpu',
                                                             l_devices=l_devices)
        # 查询表格中简版B装置的软件版本
        l_cpd_js268_b = []
        for k, w in d_js262_version.items():
            if w is None:
                l_cpd_js268_b.append(k)
        d_js268_b_version = query_device_module_version_by_SQL(device_ota_module='js268_b',
                                                               l_devices=l_cpd_js268_b)
        for key in d_js268_b_version.keys():
            d_js262_version[key] = d_js268_b_version[key]

        # 查询表格中监控室装置的软件版本
        l_dtu_js263 = []
        for k, w in d_js262_version.items():
            if w is None:
                l_dtu_js263.append(k)
        d_dtu_js263_version = query_device_module_version_by_SQL(device_ota_module='js263',
                                                                 l_devices=l_dtu_js263)
        for key in d_dtu_js263_version.keys():
            d_js262_version[key] = d_dtu_js263_version[key]
        js262_version_list = [d_js262_version.get(str(cpd_id)) for cpd_id in df['Devicename']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df['js262_version'] = js262_version_list

        # 查询对应的物模型属性

        # 查询4G_FirewareVersion
        d_4G_FirewareVersion = query_devices_thing_status_to_dict(
            function_block_id='default',
            thing_lable_name='4G_FirewareVersion',
            l_devices=l_devices,
        )
        FirewareVersion_4G_list = [d_4G_FirewareVersion.get(int(Devicename)) for Devicename in df['Devicename']]
        # 将 4G_FirewareVersion 列表赋值给 df_mnt_info 的新列 '4G_FirewareVersion'
        df['4G_FirewareVersion'] = FirewareVersion_4G_list

        # 4G_HardwareType
        d_4G_HardwareType = query_devices_thing_status_to_dict(
            function_block_id='default',
            thing_lable_name='4G_HardwareType',
            l_devices=l_devices,
        )
        l_4G_HardwareType = [d_4G_FirewareVersion.get(int(Devicename)) for Devicename in df['Devicename']]
        # 将 4G_HardwareType 列表赋值给 df_mnt_info 的新列 '4G_HardwareType'
        df['4G_HardwareType'] = l_4G_HardwareType

        # 查询4G_IMEI
        d_4G_IMEI = query_devices_thing_status_to_dict(
            function_block_id='default',
            thing_lable_name='4G_IMEI',
            l_devices=l_devices,
        )
        l_4G_IMEI = [d_4G_IMEI.get(int(Devicename)) for Devicename in df['Devicename']]

        # 将 4G_IMEI 列表赋值给 df_mnt_info 的新列 '4G_IMEI'
        df['4G_IMEI'] = l_4G_IMEI

        # 将查询调用时间戳加入到df中
        df['4G模块信息收集时间'] = datetime.now()

        df.to_excel(existing_excel_file2, sheet_name=Sheet,
                    index=False)



    else:
        print('装置列表为空')
        pass


if __name__ == "__main__":
    # 遍历到所有配置摔倒功能的电梯（商务合同），并查询这些电梯的一些信息，生成对应的excel
    print('开查')

    # # 该函数用于查询142固件版本物模型，仅需将装置复制到对应existing_excel_file2的xlsx文件中
    # query_CPD_4g_things()

    # 该函数用于142可靠性跟踪
    query_4G_reliability_tracing()
    print('查完了')
