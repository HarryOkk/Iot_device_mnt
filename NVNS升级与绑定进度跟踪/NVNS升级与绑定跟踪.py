import pandas as pd
import numpy as np
from remes_mysql.db_config import AliyunBizDb


# 通过cid查询对应二维码绑定关系的CPDID
def query_cpdid_from_cid(cid: str):
    sql = f"""
            select
                ele_contract_no,
                cpd_id,
                status
            from
                t_cpd_elevator 
            inner join 
                remes_elevator_base 
            on
                t_cpd_elevator.ele_id=remes_elevator_base.ele_id
            where
               ele_contract_no='{cid}'
       """
    df = AliyunBizDb().read_data(sql=sql)
    if len(df) > 0:
        return df['status'][0]
    else:
        return None


def query_bind_relation_by_cid():
    d_ele_cpd_bind_relation = {}
    df = pd.read_excel('NV_NS平台 第一批升级进度.xlsx', 'Sheet1')
    for ele_cid in df['ele_contract_no']:
        the_ele_bind_relation = query_cpdid_from_cid(ele_cid)
        d_ele_cpd_bind_relation[ele_cid] = the_ele_bind_relation
    l_bind_relation_counter_by_ele = [d_ele_cpd_bind_relation.get(str(ele)) for ele in df['ele_contract_no']]
    df['二维码'] = l_bind_relation_counter_by_ele
    df.to_excel('NV_NS平台 第一批升级进度.xlsx', 'Sheet1', index=False)
    pass


if __name__ == "__main__":
    query_bind_relation_by_cid()
