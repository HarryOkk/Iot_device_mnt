from remes_aliyun_openapi.iot.thing_model_use import query_device_properties_data
import pandas as pd
from remes_aliyun_openapi.iot.thing_model_use import query_device_properties_data
from datetime import datetime

# 日志配置


"""物模型属性批量查询工具"""


def dm_properties_query(
        start_time: str,
        end_time: str,
        l_dm_identifier: list,
        product_key: str,
        iot_instance_id: str,
        device_name: str
) -> pd.DataFrame:
    """
    查询设备一段时间内的属性数据，并返回 DataFrame 表格。
    
    Args:
        start_time (str): 起始时间，格式为 "%Y-%m-%d %H:%M:%S"。
        end_time (str): 结束时间，格式为 "%Y-%m-%d %H:%M:%S"。
        l_dm_identifier (list): 物模型标识符列表。
        product_key (str): 产品 Key。
        iot_instance_id (str): IoT 实例 ID。
        device_name (str): 设备名称。
    Returns:
        pd.DataFrame: 包含查询结果的 DataFrame 表格。
    
    """
    df_total = pd.DataFrame()
    start_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    """对属性进行分片，并将查询结果拼接"""

    l_responses_ct = []
    if len(l_dm_identifier) > 10:
        # 对多个物模型执行分片查询并拼接
        i = 0
        while i < len(l_dm_identifier):
            l_dm_identifier_slice = l_dm_identifier[i:i + 10]
            l_responses_ct.append(query_device_properties_data(
                start_time=start_datetime,
                asc=1,
                end_time=end_datetime,
                identifier=l_dm_identifier_slice,
                iot_instance_id=iot_instance_id,
                product_key=product_key,
                device_name=device_name,
            ))
            i += 10
    else:
        l_responses_ct = query_device_properties_data(
            start_time=start_time,
            asc=1,
            end_time=end_time,
            identifier=l_dm_identifier,
            iot_instance_id=iot_instance_id,
            product_key=product_key,
            device_name=device_name,
        )

    for l_response in l_responses_ct:  # 对应每个物模型切片列表
        for d_response in l_response:  # l_response应该是一组物模型
            if d_response['body']['Success']:  # 进入单次请求的返回
                df_total_temp = pd.DataFrame()
                l_dm_dates = d_response['body']['PropertyDataInfos']['PropertyDataInfo']
                for dm_data in l_dm_dates:  # dm_date对应单次查询的单个物模型属性及其值列表
                    dm_name = dm_data['Identifier']
                    # 对应设备单个物模型属性的数据内容，List[Dict]
                    l_single_dm_datas = dm_data['List']['PropertyInfo']
                    if len(l_single_dm_datas) > 0:
                        df_single_dm_datas = pd.DataFrame(l_single_dm_datas)
                        df_single_dm_datas.rename(columns={'Value': dm_name}, inplace=True)
                        if df_total_temp.empty:
                            df_total_temp = df_single_dm_datas
                        else:
                            print(df_single_dm_datas.columns)
                            print(df_total_temp.columns)
                            print("////////////////////////////////////")
                            df_total_temp = df_total_temp.merge(df_single_dm_datas, on='Time', how='outer')
                if df_total.empty:
                    df_total = df_total_temp
                else:
                    df_total = pd.concat([df_total, df_total_temp], axis=0, ignore_index=True)
            else:
                print(d_response['body']['ErrorMessage'])

    # 把time列的毫秒级时间戳转换成标准时间格式
    df_total['Time'] = df_total['Time'].apply(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    # 将记录按时间time排序
    df_total.sort_values(by=['Time'], inplace=True, ignore_index=True)
    return df_total


if __name__ == "__main__":
    l_dm = ['TPIoTMnt:tIsSafetyCircuitOpen', 'TPIoTMnt:tSafetyCircuitSigThreshold'
        , 'TPIoTMnt:tAlarmSigThreshold'
        , 'TPIoTMnt:tDevTotalPowerOn'
        , 'TPIoTMnt:tOverLoad,TPIoTMnt:tRtEleDispFloor'
        , 'TPIoTMnt:tRtEleDispFloor'
        , 'TPIoTMnt:tElePowerState'
        , 'TPIoTMnt:tEleRatedSpeed'
        , 'TPIoTMnt:tEleErrCode'
        , 'TPIoTMnt:tEleRapidBrakesCnt'
        , 'TPIoTMnt:tEleService'
        , 'TPIoTMnt:tEleBankRelation'
        , 'TPIoTMnt:tRtCarSpeed'
        , 'TPIoTMnt:tEleRunCnt'
        , 'TPIoTMnt:tRtEleRunDir'
        , 'TPIoTMnt:tEleRunDistance'
        , 'TPIoTMnt:tRtEleRunMode'
        , 'TPIoTMnt:tEleRunTime'
        , 'TPIoTMnt:tRtEleInService'
        , 'TPIoTMnt:tTotalFloorHeight'
        , 'TPIoTMnt:tEleDoorOpenCloseTotalCnt'
        , 'TPIoTMnt:tEleFloorCnt'
        , 'TPIoTMnt:tWireropeBendCnt'
        , 'TPIoTMnt:tRtRearDoorState'
        , 'TPIoTMnt:tEmerStopCnt'
        , 'TPIoTMnt:tHandModeRunCnt'
        , 'TPIoTMnt:tRtCarHasPeople'
        , 'TPIoTMnt:tCarReg'
        , 'TPIoTMnt:tOpenTheDoorSigThreshold'
        , 'TPIoTMnt:tFullLoad'
        , 'TPIoTMnt:tEleSlowRunCnt'
        , 'TPIoTMnt:tEleFloorDoorOpenCntTable'
        , 'TPIoTMnt:tEleFloorPassCntTable'
        , 'TPIoTMnt:tRtEleInDoorZone'
        , 'TPIoTMnt:tFloorSigThreshold'
        , 'TPIoTMnt:tRtFrontDoorState'
        , 'TPIoTMnt:tSEIDReserveSig'
        , 'TPIoTMnt:tIsUpperLimitMoved'
        , 'TPIoTMnt:tUpButtonAction'
        , 'TPIoTMnt:tMaintainSigThreshold'
        , 'TPIoTMnt:tIsLowerLimitMoved'
        , 'TPIoTMnt:tDownButtonAction'
        , 'TPIoTMnt:tEleTractionState'
        , 'TPIoTMnt:tTdadCommState'
        , 'TPIoTMnt:tPowerOnCnt'
        , 'TPIoTMnt:tDevErrCode'
        , 'TPIoTMnt:tDeviceWorkState']

    # l_dm_identifier = ['TPIoTMnt:tIsSafetyCircuitOpen']
    """指定物模型属性的查询参数"""
    my_iot_instance_id = 'iot-060a02m5'
    my_product_key = 'g4xdsqZciZ0'
    my_device_name = '180100101629'

    my_start_time = "2024-08-26 11:00:00"
    my_end_time = "2024-08-26 11:15:00"

    dm_properties_query(start_time=my_start_time, end_time=my_end_time, l_dm_identifier=l_dm,
                        product_key=my_product_key,
                        iot_instance_id=my_iot_instance_id,
                        device_name=my_device_name
                        ).to_csv("test.csv")
