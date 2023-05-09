from remes_aliyun_openapi.iot.thing_model_use import set_devices_property
from remes_aliyun_openapi.iot.device_manage import batch_query_device_detail
from remes_aliyun_openapi.iot.device_manage import query_device_prop
import pandas as pd
import openpyxl
import logging
import json
from remes_mysql.db_config import AliyunBizDb

file_path = r"D:\my_documents\智慧电梯\花园三期.xlsx"
logging.basicConfig(level=logging.DEBUG, handlers=[logging.FileHandler(r'D:\software\Notepad++\change.log')])
product_key = "g4xdsqZciZ0"
iot_instance_id = "iot-060a02m5"
cpdid_column_id = 4
dev_status_column_id = 6


# 获取file_path的excel表格中装置的列表
def extract_colum_cpdid() -> list:
    xlsx = pd.ExcelFile(file_path)
    l_devices = []
    # 该函数导出xlsx文件中的所有sheet
    for sheet_name in xlsx.sheet_names:
        df = pd.read_excel(xlsx, sheet_name)
        try:
            l_dev_name = df.iloc[:, cpdid_column_id].tolist()
        except:
            l_dev_name = []
            logging.info('导出CPDID列表失败，问题列表为：' + sheet_name)
        l_devices.extend(l_dev_name)
    logging.info(l_devices)
    logging.info(len(l_devices))
    return l_devices


# 查询装置的标签字典
def query_devices_label(l_devices: list[int]) -> dict:
    dict_devices_labels = {}
    for device in l_devices:
        response = query_device_prop(iot_instance_id, product_key, str(device))
        logging.info('下面是装置的标签信息')
        logging.info(response)
        if response['body']['Success']:
            dev_props = response['body']['Props']
            dict_dev_labels = json.loads(dev_props)
            dict_devices_labels[str(device)] = dict_dev_labels
        else:
            logging.info(device)
            logging.info('装置调用接口失败')
            logging.info(response['body'])
    return dict_devices_labels


# 查询装置列表的在线状态，并将在线状态填写到表格对应的位置
def query_devices_status_to_xlsx(l_devices: list):
    wb = openpyxl.load_workbook(file_path)
    # 获取表中第一个sheet对象,用于写入表格
    sheet = wb.active

    response_dev_mnt = batch_query_device_detail(product_key, l_devices, iot_instance_id)
    logging.info(response_dev_mnt)
    if response_dev_mnt['body']['Success']:
        # 从返回值中拿到设备详细信息的列表
        l_device_detail = response_dev_mnt['body']['Data']['Data']
        logging.info(l_device_detail)
        print('表格中装置的状态为：')
        for dict_dev_item in l_device_detail:
            print(dict_dev_item['DeviceName'], dict_dev_item['Status'])
            # 将装置的在线状态更新到表格中
            for row in sheet.iter_rows():
                if row[cpdid_column_id].value == dict_dev_item['DeviceName']:
                    row_num = row[cpdid_column_id].row
                    # 把装置的在线状态写入对应的表格
                    row_later = sheet[row_num]
                    row_later[dev_status_column_id].value = dict_dev_item['Status']
        wb.save(file_path)

    else:
        print('装置在线状态查询接口调用失败，详细信息为：', response_dev_mnt['body'])


# 开通装置列表种所有装置的CRT参数
def activate_param_crt(l_devices: list) -> dict:
    items = '{"pCRT": 1}'
    response = set_devices_property(product_key, items, l_devices, iot_instance_id)
    print("开通CRT参数调用结果为：", response['body'])
    return response


# 开通列表所有装置的LIC参数
def activate_param_license(l_devices: list) -> dict:
    items = '{"WirelessCallFunc": 1}'
    response = set_devices_property(product_key, items, l_devices, iot_instance_id)
    print("开通无线通话参数参数调用结果为：")
    print(response['body'])
    return response


# 根据列表中装置的cpd_id，在无线通话开通表中查询对应的合同号梯号(注意：传入的cpdid列表元素应该为str类型)
def query_license_device(l_device: list) -> dict:
    dict_lic_device_cid = {}
    for device_name_cpd in l_device:
        sql = f'''
            select
                ele_id
            from
                license_cpd_elevator
            where
                cpd_id = {device_name_cpd}
        '''
        df_cpd_ele_id = AliyunBizDb().read_data(sql=sql)
        current_ele_id = df_cpd_ele_id["ele_id"][0]
        sql = f'''
            select
                ele_contract_no
            from
                remes_elevator_base
            where
                ele_id = '{current_ele_id}'
        '''
        df_cid_ele_id = AliyunBizDb().read_data(sql=sql)
        dict_lic_device_cid[device_name_cpd] = df_cid_ele_id["ele_contract_no"][0]
    return dict_lic_device_cid


if __name__ == "__main__":
    # l_devices_from_excel = extract_colum_cpdid()
    l_devices_from_excel_int = [150203401035,
150100801346,
151130103390,
150203404723,
150203404670,
151130102581,
150203400873,
140920700227,
140920700338,
150100804885]
    l_devices_from_excel = [str(i) for i in l_devices_from_excel_int]
    print('CPD装置总数量：', len(l_devices_from_excel))
    dict_lic_info = activate_param_crt(l_devices_from_excel)
    logging.info(dict_lic_info)
    # 调用接口拿到设备的详细信息,里面包含设备的在线状态
    # query_devices_status_to_xlsx(l_devices_from_excel)
    # dict_dev_labels = query_devices_label(l_devices_from_excel)
    # logging.info(dict_dev_labels)
    # print(activate_param_license(l_devices_from_excel)['body'])
