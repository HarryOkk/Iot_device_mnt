from remes_aliyun_openapi.iot.device_manage import query_device_by_sql, batch_query_device_detail
from remes_mysql.db_config import AliyunBizDb

from iot_device_mnt import query_devices_thing_status, SMEC_CPD, iot_instance_id, SMEC_GW
import pandas as pd
import time
import datetime
from remes_aliyun_openapi.iot.thing_model_use import query_device_property_data, query_device_property_status 


existing_excel_file = r'D:\my_documents\IVRD_DATA_COLLECT\设备通信状态履历.xlsx'
timestamp_seconds = time.time()
current_timestamp_milliseconds = int(timestamp_seconds * 1000)
start_timestamp_milliseconds = (current_timestamp_milliseconds - 604800000)


def extract_colum_cpdid() -> list:
    xlsx = pd.ExcelFile(r'D:\my_documents\IVRD_DATA_COLLECT\设备基本信息.xlsx')
    # 该函数导出xlsx文件中所有sheet
    df = pd.read_excel(xlsx, 'Sheet2')
    try:
        l_devices = df.iloc[:, 1].tolist()
    except:
        l_devices = []
        print(f"导出CPDID列表失败")
    print(f"从文件拿到的所有控制柜装置如下：{l_devices}")
    print(f'装置的一共有{len(l_devices)}台')
    return l_devices


# 导出CPD对应的ELEID字典，前提是文件中已经保存了这样的字典
def extract_colum_ele_id() -> dict:
    xlsx = pd.ExcelFile(r'D:\my_documents\IVRD_DATA_COLLECT\设备基本信息.xlsx')
    # 该函数导出xlsx文件中所有sheet
    df = pd.read_excel(xlsx, 'Sheet2')
    try:
        data_dict = dict(zip(df['cpd_id'], df['ele_id']))
        print(data_dict)
    except:
        print(f"导出CPDID和ELEID列表失败")
    return data_dict


# 查询列表cpdid的物模型属性并返回字典
def query_devices_thing_status_to_dict(function_block_id: str,
                                       thing_lable_name: str,
                                       l_devices: list[str],
                                       ) -> dict:
    d_response = {}
    for device_name in l_devices:
        if function_block_id == 'default':
            d_func_block = query_device_property_status(iot_instance_id=iot_instance_id,
                                                        product_key=SMEC_CPD,
                                                        device_name=device_name)
        else:
            d_func_block = query_device_property_status(iot_instance_id=iot_instance_id,
                                                        product_key=SMEC_CPD,
                                                        device_name=device_name,
                                                        function_block_id=function_block_id)
        l_thing_of_device = d_func_block['body']['Data']['List']['PropertyStatusInfo']
        for d_single_func_thing in l_thing_of_device:
            try:
                if d_single_func_thing['Identifier'] == thing_lable_name:
                    d_response[device_name] = d_single_func_thing['Value']
            except KeyError:
                d_response[device_name] = None
    return d_response


# 查询物模型当前状态并填入
def query_devices_basic():
    l_devices_ivrd = extract_colum_cpdid()
    d_enable_state = query_devices_thing_status(
        function_block_id='SmartDevMnt',
        thing_lable_name='SmartDevMnt:IVRD_EnableState',
        l_devices=l_devices_ivrd,
        excel_dev_line=0,
        excel_thing_module_line=1
    )
    d_enable_state = query_devices_thing_status(
        function_block_id='SmartDevMnt',
        thing_lable_name='SmartDevMnt:IVRD_AppVersion',
        l_devices=l_devices_ivrd,
        excel_dev_line=0,
        excel_thing_module_line=2
    )
    d_psn = query_devices_thing_status(
        function_block_id='SmartDevMnt',
        thing_lable_name='SmartDevMnt:IVRD_PSN',
        l_devices=l_devices_ivrd,
        excel_dev_line=0,
        excel_thing_module_line=3
    )
    d_ip = query_devices_thing_status(
        function_block_id='SmartDevMnt',
        thing_lable_name='SmartDevMnt:IVRD_IP',
        l_devices=l_devices_ivrd,
        excel_dev_line=0,
        excel_thing_module_line=4
    )
    d_comm_state = query_devices_thing_status(
        function_block_id='SmartDevMnt',
        thing_lable_name='SmartDevMnt:IVRD_CommState',
        l_devices=l_devices_ivrd,
        excel_dev_line=0,
        excel_thing_module_line=5
    )


# 将毫秒级时间戳转换成标准时间格式（用于查询物模型履历）
def trans_milliseconds_to_formattedtime(timestamp_milliseconds: int) -> str:
    # 将毫秒级时间戳转换为日期时间对象
    timestamp_seconds = timestamp_milliseconds / 1000
    dt_object = datetime.datetime.fromtimestamp(timestamp_seconds)

    # 格式化日期时间对象为2023-07-24 12:37:15形式
    formatted_time = dt_object.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_time


def query_comm_states_resume(device_name: str) -> dict:
    print('当前调用接口的设备名称为：', device_name, type(device_name))
    if device_name == device_name :
        result = query_device_property_data(
            start_time=start_timestamp_milliseconds,
            identifier='SmartDevMnt:IVRD_CommState',
            asc=1,
            end_time=current_timestamp_milliseconds,
            iot_instance_id='iot-060a02m5',
            product_key='g4xdsqZciZ0',
            device_name=f'{device_name}',
        )[0]['body']
        print(result)
        if result['Success'] and result.get('Data') and result['Data']['List'].get('PropertyInfo'):
            next_time = trans_milliseconds_to_formattedtime(result['Data']['NextTime'])
            have_next_page = result['Data']['NextValid']
            print(r'下一页面属性记录的起始时间为：', next_time)
            print(r'是否有下一页：', have_next_page)
            response = result['Data']['List']['PropertyInfo']
            return response
        else:
            print(f"查询设备通信状态失败")
            return {}
    else:
        print('devicename为nan', device_name, type(device_name))
        return {}


# 查询七天内控制柜装置的上下线情况
def query_CPD_status():
    file_path = r'\\smecnas3.smec-cn.com\k2data_share\wireless_call_device_signal\wireless_call_device_signal.csv'

    # 读取CSV文件为DataFrame
    df = pd.read_csv(file_path, low_memory=False, parse_dates=['TIMESTAMP'],
                     dtype={'DEVICENAME': str, 'LICFLAG': str, 'DATATYPE': str, 'VALUE': str, 'REASON': str},
                     encoding='utf-8', skiprows=0)
    # 筛选出VALUE字段为OPERATION的记录
    filtered_df = df[df['DATATYPE'] == 'OPERATION']
    filtered_df.reset_index(drop=False, inplace=True)  # drop=False表示保留原来的第一列作为新的一列
    filtered_df.set_index('index', inplace=True)  # 设置新的索引为原来的第一列，并删除原来的索引列
    # 打印筛选后的结果

    output_file = r'D:\my_documents\IVRD_DATA_COLLECT\CPD_STATUS.xlsx'
    filtered_df.to_excel(output_file, index=False)


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
                product_key = 'g4xdsqZciZ0'
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


# 查询装置的在线状态
def query_devices_status(l_devices: list[str]) -> dict:
    print('查询装置的状态......')
    d_cpd_status = {}
    chunk_size = 100  # 分片大小,因该接口每次调用最多查询100台装置，所以需要对list进行分片
    for i in range(0, len(l_devices), chunk_size):
        chunk = l_devices[i:i + chunk_size]
        # 在这里对每个分片进行处理

        response_dev_mnt = batch_query_device_detail(SMEC_CPD, chunk, iot_instance_id)
        if response_dev_mnt['body']['Success']:
            # 从返回值中拿到设备详细信息的列表
            l_device_detail = response_dev_mnt['body']['Data']['Data']
            for dict_dev_item in l_device_detail:
                d_cpd_status[dict_dev_item['DeviceName']] = dict_dev_item['Status']
    return d_cpd_status


# 查询所有含有云care摔倒功能的IVRD设备对应的ele_id：
def query_all_ivrd_ele_id():
    sql = f'''
            select
                distinct zecn.ele_id,
                tce.cpd_id,
                tce.created_date ,
                tce.status,
                tce.user_id,
                reb.ele_contract_no ,
                reb.customer_name,
                reb.mnt_build_name,
                zec.ele_local_name 
            from
                zhdt_data_db.zhdt_ele_commerce_new zecn
            left join
                remes_db.t_cpd_elevator tce 
            on
                zecn.ele_id = tce.ele_id 
            left join 
                remes_db.remes_elevator_base reb 
            on 
                zecn.ele_id = reb.ele_id
            left join 
                zhdt_view_db.zv_elevator_config zec 
            on 
                zecn.ele_id  = zec.ele_id 
            where
                zecn.pot_id=1007
            and
                zecn.has_commerce_flag='Y'
            '''
    print('sql准备完成，正在查')
    df_mnt_info = AliyunBizDb().read_data(sql=sql)
    if len(df_mnt_info) > 0:
        df_mnt_info.to_excel(r'D:\my_documents\IVRD_DATA_COLLECT\设备基本信息.xlsx', sheet_name='Sheet2', index=False)

        cpd_id_list = df_mnt_info['cpd_id'].tolist()
        cpd_id_list = list(filter(lambda x: x is not None, cpd_id_list))

        # 查询控制柜装置的软件版本
        d_js262_version = query_device_module_version_by_SQL(device_ota_module='js262_cpu',
                                                             l_devices=cpd_id_list)
        l_cpd_js268_b = []
        for k, w in d_js262_version.items():
            if w is None:
                l_cpd_js268_b.append(k)
        d_js268_b_version = query_device_module_version_by_SQL(device_ota_module='js268_b',
                                                               l_devices=l_cpd_js268_b)
        for key in d_js268_b_version.keys():
            d_js262_version[key] = d_js268_b_version[key]

        js262_version_list = [d_js262_version.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['js262_version'] = js262_version_list

        # 查询装置列表的在线状态，并将在线状态填写到表格对应的位置
        d_cpd_status = query_devices_status(l_devices=cpd_id_list)
        print(d_cpd_status)
        cpd_status_list = [d_cpd_status.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 enable_state_list 列表赋值给 df_mnt_info 的新列 'ENABLE_STATE'
        df_mnt_info['cpd_status'] = cpd_status_list

        # 查询对应的物模型属性

        # 查询使能状态
        d_IVRD_EnableState = query_devices_thing_status_to_dict(
            function_block_id='SmartDevMnt',
            thing_lable_name='SmartDevMnt:IVRD_EnableState',
            l_devices=cpd_id_list,
        )
        print(d_IVRD_EnableState)
        IVRD_EnableState_list = [d_IVRD_EnableState.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 enable_state_list 列表赋值给 df_mnt_info 的新列 'ENABLE_STATE'
        df_mnt_info['ENABLE_STATE'] = IVRD_EnableState_list

        # 查询IVRD_AppVersion
        d_IVRD_AppVersion = query_devices_thing_status_to_dict(
            function_block_id='SmartDevMnt',
            thing_lable_name='SmartDevMnt:IVRD_AppVersion',
            l_devices=cpd_id_list,
        )
        print(d_IVRD_AppVersion)
        IVRD_AppVersion_list = [d_IVRD_AppVersion.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 IVRD_AppVersion 列表赋值给 df_mnt_info 的新列 'ENABLE_STATE'
        df_mnt_info['IVRD_AppVersion'] = IVRD_AppVersion_list

        # 查询IVRD_PSN
        d_IVRD_PSN = query_devices_thing_status_to_dict(
            function_block_id='SmartDevMnt',
            thing_lable_name='SmartDevMnt:IVRD_PSN',
            l_devices=cpd_id_list,
        )
        print(d_IVRD_PSN)
        IVRD_PSN_list = [d_IVRD_PSN.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 IVRD_PSN 列表赋值给 df_mnt_info 的新列 'ENABLE_STATE'
        df_mnt_info['IVRD_PSN'] = IVRD_PSN_list

        # 查询IVRD_IP
        d_IVRD_IP = query_devices_thing_status_to_dict(
            function_block_id='SmartDevMnt',
            thing_lable_name='SmartDevMnt:IVRD_IP',
            l_devices=cpd_id_list,
        )
        print(d_IVRD_IP)
        IVRD_IP_list = [d_IVRD_IP.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 IVRD_PSN 列表赋值给 df_mnt_info 的新列 'ENABLE_STATE'
        df_mnt_info['IVRD_IP'] = IVRD_IP_list

        # 查询IVRD_CommState
        d_IVRD_CommState = query_devices_thing_status_to_dict(
            function_block_id='SmartDevMnt',
            thing_lable_name='SmartDevMnt:IVRD_CommState',
            l_devices=cpd_id_list,
        )
        print(d_IVRD_CommState)
        IVRD_CommState_list = [d_IVRD_CommState.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 IVRD_PSN 列表赋值给 df_mnt_info 的新列 'ENABLE_STATE'
        df_mnt_info['IVRD_CommState'] = IVRD_CommState_list

        # 查询识别功能使能状态：摔倒识别
        d_pPassengerFall = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pPassengerFall',
            l_devices=cpd_id_list,
        )
        print(d_pPassengerFall)
        pPassengerFall_list = [d_pPassengerFall.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['pPassengerFall'] = pPassengerFall_list

        # 查询识别功能使能状态：乘客扒门
        d_pForceDoorOpen = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pForceDoorOpen',
            l_devices=cpd_id_list,
        )
        print(d_pForceDoorOpen)
        pForceDoorOpen_list = [d_pForceDoorOpen.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info[' pForceDoorOpen'] = pForceDoorOpen_list

        # 查询识别功能使能状态：乘客挡门
        d_pPassengerBlockDoor = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pPassengerBlockDoor',
            l_devices=cpd_id_list,
        )
        print(d_pPassengerBlockDoor)
        pPassengerBlockDoor_list = [d_pPassengerBlockDoor.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['pPassengerBlockDoor'] = pPassengerBlockDoor_list

        # 查询识别功能使能状态：乘客吸烟
        d_pPassengerSmoking = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pPassengerSmoking',
            l_devices=cpd_id_list,
        )
        print(d_pPassengerSmoking)
        pPassengerSmoking_list = [d_pPassengerSmoking.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['pPassengerSmoking'] = pPassengerSmoking_list

        # 查询识别功能使能状态：宠物乘梯
        d_pPet = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pPet',
            l_devices=cpd_id_list,
        )
        print(d_pPet)
        pPet_list = [d_pPet.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['pPet'] = pPet_list

        # 查询识别功能使能状态：电动自行车进轿厢
        d_pEbike = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pEbike',
            l_devices=cpd_id_list,
        )
        print(d_pEbike)
        pEbike_list = [d_pEbike.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['pEbike'] = pEbike_list

        # 查询识别功能使能状态：海康摄像头功能设置
        d_HikvisionCameraFuncSet = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:HikvisionCameraFuncSet',
            l_devices=cpd_id_list,
        )
        print(d_HikvisionCameraFuncSet)
        HikvisionCameraFuncSet_list = [d_HikvisionCameraFuncSet.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['HikvisionCameraFuncSet'] = HikvisionCameraFuncSet_list

        # 查询识别功能使能状态：挥手求助
        d_pWaveForHelp = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pWaveForHelp',
            l_devices=cpd_id_list,
        )
        print(d_pWaveForHelp)
        pWaveForHelp_list = [d_pWaveForHelp.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['pWaveForHelp'] = pWaveForHelp_list

        # 查询识别功能使能状态：轿内遗留物品识别
        d_pObjectLeft = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pObjectLeft',
            l_devices=cpd_id_list,
        )
        print(d_pObjectLeft)
        pObjectLeft_list = [d_pObjectLeft.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['pObjectLeft'] = pObjectLeft_list

        # 查询识别功能使能状态：困人识别
        d_pPassengerTrapped = query_devices_thing_status_to_dict(
            function_block_id='FuncMnt',
            thing_lable_name='FuncMnt:pPassengerTrapped',
            l_devices=cpd_id_list,
        )
        print(d_pPassengerTrapped)
        pPassengerTrapped_list = [d_pPassengerTrapped.get(str(cpd_id)) for cpd_id in df_mnt_info['cpd_id']]
        # 将 pPassengerFall_list 列表赋值给 df_mnt_info 的新列 'pPassengerFall_list'
        df_mnt_info['pPassengerTrapped'] = pPassengerTrapped_list

        df_mnt_info.to_excel(r'D:\my_documents\IVRD_DATA_COLLECT\设备基本信息.xlsx', sheet_name='Sheet2', index=False)
    else:
        pass


if __name__ == "__main__":
    # 遍历到所有配置摔倒功能的电梯（商务合同），并查询这些电梯的一些信息，生成对应的excel
    print('开查')
    query_all_ivrd_ele_id()
    print('查完了')
    # 查询七天内装置的上下线情况，并保存到履历excel中
    query_CPD_status()

    # 查询目前的装置列表
    l_devices_ivrd = extract_colum_cpdid()
    # 拿到表格中装置和电梯的对应字典
    d_device_ele = extract_colum_ele_id()

    # 查询装置的通信状态履历
    result_df = pd.DataFrame()
    for device, ele in d_device_ele.items():
        l_device_comm_resume = query_comm_states_resume(device)
        if l_device_comm_resume is not None:
            for d_value in l_device_comm_resume:
                d_value['Time'] = trans_milliseconds_to_formattedtime(d_value['Time'])
                df = pd.DataFrame(d_value, index=[0])
                df['product_key'] = 'SMEC_CPD'
                df['device_name'] = f'{device}'
                df['ele_id'] = f'{ele}'
                # 将当前记录追加到结果DataFrame中
                result_df = pd.concat([result_df, df], ignore_index=True)
        # 将合并后的DataFrame写入新的Excel文件，注意设置index=False以防止写入行索引
    with pd.ExcelWriter(existing_excel_file, engine='openpyxl') as writer:
        result_df.to_excel(writer, index=False,
                           header=['datatime', 'commte_state', 'product_key', 'device_name', 'ele_id'],
                           sheet_name='Sheet')
