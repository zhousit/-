import pandas as pd
import os
import re
from setting import *


class OrderData:
    def __init__(self, commodity_path=COMMODITY_PATH, goods_youzhan_path=GOODS_YOUZAN_PATH, seller_path=SELLER_PATH ,
                 project_group=PROJECT_GROUP_PATH):
        self.commodity_path = commodity_path
        self.goods_youzhan_path = goods_youzhan_path
        self.seller_path = seller_path
        self.project_group = project_group

    def get_aim_file(self, path):
        file_list = os.listdir(path)
        file = file_list[0] if file_list else None
        return os.path.join(path, file) if file else None

    def deal_project_adgroup(self):
        if hasattr(self, "project_adgroup_d" ): return self.project_adgroup_d
        project_adgroup_d = {}
        file_path = self.get_aim_file(self.project_group)
        if not file_path: return
        df_pro = pd.read_excel(file_path)
        cols = list(df_pro)
        for col in cols:
            pro_ch = filter(lambda x: not pd.isna(x), df_pro[col])
            for i in pro_ch: project_adgroup_d[i] = col
        self.project_adgroup_d = project_adgroup_d
        return self.project_adgroup_d


    def read_excel(self, path=None):
        if path:
            file_path = self.get_aim_file(path)
            return pd.read_csv(file_path) if file_path else None
        com_file_path, goods_file_path = self.get_aim_file(self.commodity_path), self.get_aim_file(self.goods_youzhan_path)
        if com_file_path and goods_file_path:
            df_com, df_goods = pd.read_excel(com_file_path), pd.read_csv(goods_file_path)
            return df_com, df_goods
        return None, None

    def check_adgroup(self, x):
        project_adgroup_d = self.deal_project_adgroup()
        for k, v in project_adgroup_d.items():
            if not pd.isna(x["所属分组"]) and re.search(k , x["所属分组"]):
                return  v
        else:
            return "未分组订单"



    def deal_data(self):
        df_com, df_goods = self.read_excel()
        if df_goods is None and df_com is None: return
        df_com = df_com[["所属分组", "订单佣金", "订单号"]]
        df_merge = pd.merge(left=df_goods, right=df_com, how="left", on="订单号" )
        df_merge = df_merge[df_merge["订单商品状态"] != "交易关闭"]
        df_merge["项目群"] = df_merge.apply(self.check_adgroup, axis=1)
        cols = list(df_merge)
        for i in ["所属分组", "订单佣金","项目群"]: cols.insert( cols.index("归属店铺"),cols.pop(cols.index(i)))
        df_merge = df_merge.loc[:, cols]
        df_merge.to_csv(os.path.join(COMPLETION_PATH, "result1.csv"), index=False)
        input("完成手动补全请点击ENTER")
        df_res = self.read_excel(path=COMPLETION_PATH)
        g_amount = df_res["订单佣金"].groupby(df_res["项目群"]).sum()
        g_mun = df_res["商品数量"].groupby(df_res["项目群"]).sum()
        g_amount_d , g_mun_d = g_amount.to_dict(), g_mun.to_dict()
        order_l = sorted(g_mun_d.keys(), key=lambda x: g_mun_d[x], reverse=True)
        data_l = [(key, g_mun_d[key], g_amount_d[key] ) for key in order_l]
        df = pd.DataFrame(data_l, columns=["项目群", "销量", "佣金总额"])
        df.to_excel(os.path.join(SUMMARY_PATH, "sum.xlsx"), index=False)

if __name__ == '__main__':
    order = OrderData()
    order.deal_data()
    # order.check_adgroup()