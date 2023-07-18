import pandas as pd
import pickle
from tqdm import tqdm
from datetime import datetime
import numpy as np
import db
from sklearn.preprocessing import MinMaxScaler


class Stock:

    def __init__(self, start_date, end_date=db.date_today):

        self.start_date = start_date
        self.end_date = end_date
        self.list_date_eom = pd.date_range(start_date, end_date, freq="M")
        self.dict_market_cap = {}

        # 가격 Dictionary 생성
        with open(r"D:\MyProject\StockPrice\DictDfStock.pickle", 'rb') as fr:
            self.dict_df_stock = pickle.load(fr)

        # 월별 전략 스케줄 생성
        with open(r'D:\MyProject\FactorSelection\monthly_invest_strategy.pickle', 'rb') as fr:
            self.monthly_invest_strategy = pickle.load(fr)

        # 월별 전략 스케줄 백업
        file_name = 'monthly_invest_strategy' + datetime.today().strftime("%Y%m%d")
        with open(r'D:\MyProject\FactorSelection\{}.pickle'.format(file_name), 'wb') as fw:
            pickle.dump(self.monthly_invest_strategy, fw)

        # self.monthly_invest_strategy = {
        #     "stock": {},
        #     "bond": {},
        #     "commodity": {}
        # }

    def hashing_market_cap(self, df_factor_data):

        list_item_cd = df_factor_data["cmp_cd"].unique()
        list_date = pd.to_datetime(df_factor_data["date"].unique())

        for item_cd in tqdm(list_item_cd):

            if (item_cd in self.dict_market_cap.keys()) == False:
                self.dict_market_cap[item_cd] = {}

            for p_date in list_date:
                try:
                    self.dict_market_cap[item_cd][p_date] = self.dict_df_stock[item_cd].loc[p_date, "MarketCap"]
                except:
                    continue

    def get_market_cap(self, v_date, cmp_cd):
        '''
        리밸런싱 일자 시가총액
        '''

        try:
            market_cap = self.dict_market_cap[cmp_cd][v_date]

        except:
            print()
            if cmp_cd not in (self.dict_df_stock.keys()):
                return 0
            else:
                df_stock = self.dict_df_stock[cmp_cd].reset_index()
                if len(df_stock) == 0:
                    return 0

            if len(df_stock.loc[df_stock["Date"] <= v_date]) == 0:
                return 0
            else:
                market_cap = df_stock.loc[df_stock["Date"] <= v_date].iloc[-1]["MarketCap"]

        return market_cap

    def get_weight(self, w_type, df_factor_data):
        '''
        종목군별 비중 조절
        '''

        if w_type == "equal":

            # weight 생성 : (1/종목수)
            df_grp_count = df_factor_data.groupby("date").agg({"cmp_cd": "count"}).rename(
                columns={"cmp_cd": "count"}).reset_index()

            df_factor_data["weight"] = 0
            df_factor_data["weight"] = df_factor_data["date"].apply(
                lambda x: df_grp_count.loc[df_grp_count["date"] == x, "count"].values[0])
            df_factor_data["weight"] = 1 / df_factor_data["weight"]

        elif w_type == "market_cap":

            # 시가총액 칼럼 생성
            for i, rows in tqdm(df_factor_data.iterrows(), total=len(df_factor_data)):
                df_factor_data.loc[i, "market_cap"] = self.get_market_cap(rows["date"], rows["cmp_cd"])

            # weight 생성:  종목 시총 / 시가총액 총합
            df_grp_sum = df_factor_data.groupby("date").agg({"market_cap": "sum"}).rename(
                columns={"market_cap": "sum"}).reset_index()

            df_factor_data["weight"] = 0
            df_factor_data["weight"] = df_factor_data["date"].apply(
                lambda x: df_grp_sum.loc[df_grp_sum["date"] == x, "sum"].values[0])
            df_factor_data["weight"] = df_factor_data["market_cap"] / df_factor_data["weight"]


        elif w_type == "z_score":

            # MinMax Scaling
            scaler = MinMaxScaler()

            list_date = sorted(df_factor_data["date"].unique())

            for v_date in list_date:
                data = np.array(df_factor_data.loc[df_factor_data["date"] == v_date, "z_score"]).reshape(-1, 1)

                df_factor_data.loc[df_factor_data["date"] == v_date, "z_score"] = scaler.fit_transform(data)

            # weight 생성:  z_score 값 / z_score 총합

            df_grp_sum = df_factor_data.groupby("date").agg({"z_score": "sum"}).rename(
                columns={"z_score": "sum"}).reset_index()

            df_factor_data["weight"] = 0

            df_factor_data["weight"] = df_factor_data["date"].apply(
                lambda x: df_grp_sum.loc[df_grp_sum["date"] == x, "sum"].values[0])

            df_factor_data["weight"] = df_factor_data["z_score"] / df_factor_data["weight"]

        return df_factor_data

    def save_data(self):

        # save data
        with open(r'D:\MyProject\FactorSelection\monthly_invest_strategy.pickle', 'wb') as fw:
            pickle.dump(self.monthly_invest_strategy, fw)


class Value(Stock):

    my_strategy = {
        "por": pd.DataFrame(),  # POR 하위 20% 종목
        "por_cagr": pd.DataFrame(),  # POR_CAGR 하위 20% 종목
        "por_spr": pd.DataFrame()  # POR_SPREAD 상위 20% 종목 (밸류는 업사이드로 산정했기 때문에 상위 목록이 상대적 밸류가 좋다)
    }

    style_type = 'value'

    # save data
    with open(r'D:\MyProject\FactorSelection\stock_factor_value_quantiling.pickle', 'rb') as fr:
        stock_factor = pickle.load(fr)

    def filter_factor_data(self, df_factor_data, strategy_nm):

        if strategy_nm == "por":
            df_factor_data = self.stock_factor[self.stock_factor["quantile"] == 0]
            df_factor_data = df_factor_data[df_factor_data["item_nm"] == "por"]
            df_factor_data["z_score"] = 1 / df_factor_data["z_score"]

        elif strategy_nm == "por_cagr":
            df_factor_data = self.stock_factor[self.stock_factor["quantile"] == 0]
            df_factor_data = df_factor_data[df_factor_data["item_nm"] == "por_cagr"]
            df_factor_data["z_score"] = 1 / df_factor_data["z_score"]

        elif strategy_nm == "por_spr":
            df_factor_data = self.stock_factor[self.stock_factor["quantile"] == 4]
            df_factor_data = df_factor_data[df_factor_data["item_nm"] == "por_spr"]

        df_factor_data = df_factor_data.sort_values(["date", "cmp_cd"]).reset_index(drop=True)
        df_factor_data = df_factor_data[["date", "cmp_cd", "z_score"]]

        return df_factor_data

    def update_schedule(self):
        '''
        밸류 전략 업데이트
        :return:
        '''

        for strategy_nm in self.my_strategy.keys():

            df_invest_sch = pd.DataFrame(columns=["date", "item_type", "item_cd", "w_type", "weight"])
            df_factor_data = pd.DataFrame()

            # 1. 투자 종목군 설정
            df_factor_data = self.filter_factor_data(df_factor_data, strategy_nm)
            self.hashing_market_cap(df_factor_data)

            # 2. 투자 비중(weight) 설정
            for w_type in ["equal", "market_cap", "z_score"]:

                df_factor_data = self.get_weight(w_type, df_factor_data)
                df_factor_data["w_type"] = w_type

                df = df_factor_data[["date", "cmp_cd", "w_type", "weight"]].rename(columns={"cmp_cd": "item_cd"})
                df["item_type"] = "stock"
                df = df[["date", "item_type", "item_cd", "w_type", "weight"]]

                df_invest_sch = pd.concat([df_invest_sch, df])

            # save
            self.my_strategy[strategy_nm] = df_invest_sch

        self.monthly_invest_strategy["stock"][self.style_type] = self.my_strategy
        self.save_data()
        print("Complete:" , __name__)


class Growth(Stock):

    my_strategy = {
        "op_yoy": pd.DataFrame(),  #
        "op_yoy_cagr": pd.DataFrame(),  #
        "op_yoy_spread": pd.DataFrame(),  #
        "op_qoq": pd.DataFrame(),  #
        "op_qoq_cagr": pd.DataFrame(),  #
        "op_qoq_spread": pd.DataFrame()  #
    }

    style_type = 'growth'

    # save data
    with open(r'D:\MyProject\FactorSelection\stock_factor_growth_quantiling.pickle', 'rb') as fr:
        stock_factor_growth = pickle.load(fr)

    def filter_factor_data(self, strategy_nm):

        df_factor_data = self.stock_factor_growth[self.stock_factor_growth["quantile"] == 4]
        df_factor_data = df_factor_data[df_factor_data["item_nm"] == strategy_nm]

        df_factor_data = df_factor_data.sort_values(["date", "cmp_cd"]).reset_index(drop=True)
        df_factor_data = df_factor_data[["date", "cmp_cd", "z_score"]]

        return df_factor_data

    def update_schedule(self):
        '''
        밸류 전략 업데이트
        :return:
        '''

        for strategy_nm in self.my_strategy.keys():

            df_invest_sch = pd.DataFrame(columns=["date", "item_type", "item_cd", "w_type", "weight"])
            df_factor_data = pd.DataFrame()

            # 1. 투자 종목군 설정
            df_factor_data = self.filter_factor_data(strategy_nm)
            self.hashing_market_cap(df_factor_data)

            # 2. 투자 비중(weight) 설정
            for w_type in ["equal", "market_cap", "z_score"]:

                df_factor_data = self.get_weight(w_type, df_factor_data)
                df_factor_data["w_type"] = w_type

                df = df_factor_data[["date", "cmp_cd", "w_type", "weight"]].rename(columns={"cmp_cd": "item_cd"})
                df["item_type"] = "stock"
                df = df[["date", "item_type", "item_cd", "w_type", "weight"]]

                df_invest_sch = pd.concat([df_invest_sch, df])

            # save
            self.my_strategy[strategy_nm] = df_invest_sch

        self.monthly_invest_strategy["stock"][self.style_type] = self.my_strategy
        self.save_data()
        print("Complete:", __name__)

class Size(Stock):

    my_strategy = {
        "big_cap": pd.DataFrame(),  # 시총 상위 20% 종목
        "small_cap": pd.DataFrame(),  # 시총 하위 20% 종목
    }

    style_type = 'size'

    # save data
    with open(r'D:\MyProject\FactorSelection\stock_factor_size_quantiling.pickle', 'rb') as fr:
        stock_factor = pickle.load(fr)

    def filter_factor_data(self, df_factor_data, strategy_nm):

        if strategy_nm == "small_cap":
            df_factor_data = self.stock_factor[self.stock_factor["quantile"] == 0]
            df_factor_data = df_factor_data[df_factor_data["item_nm"] == "market_cap"]
            df_factor_data["z_score"] = 1 / df_factor_data["z_score"]

        elif strategy_nm == "big_cap":
            df_factor_data = self.stock_factor[self.stock_factor["quantile"] == 4]
            df_factor_data = df_factor_data[df_factor_data["item_nm"] == "market_cap"]
            df_factor_data["z_score"] = 1 / df_factor_data["z_score"]

        df_factor_data = df_factor_data.sort_values(["date", "cmp_cd"]).reset_index(drop=True)
        df_factor_data = df_factor_data[["date", "cmp_cd", "z_score"]]

        return df_factor_data

    def update_schedule(self):
        '''
        전략 업데이트
        :return:
        '''

        for strategy_nm in self.my_strategy.keys():

            df_invest_sch = pd.DataFrame(columns=["date", "item_type", "item_cd", "w_type", "weight"])
            df_factor_data = pd.DataFrame()

            # 1. 투자 종목군 설정
            df_factor_data = self.filter_factor_data(df_factor_data, strategy_nm)
            self.hashing_market_cap(df_factor_data)

            # 2. 투자 비중(weight) 설정
            for w_type in ["equal", "market_cap", "z_score"]:

                df_factor_data = self.get_weight(w_type, df_factor_data)
                df_factor_data["w_type"] = w_type

                df = df_factor_data[["date", "cmp_cd", "w_type", "weight"]].rename(columns={"cmp_cd": "item_cd"})
                df["item_type"] = "stock"
                df = df[["date", "item_type", "item_cd", "w_type", "weight"]]

                df_invest_sch = pd.concat([df_invest_sch, df])

            # save
            self.my_strategy[strategy_nm] = df_invest_sch

        self.monthly_invest_strategy["stock"][self.style_type] = self.my_strategy
        self.save_data()
        print("Complete:", __name__)


class Quality(Stock):

    my_strategy = {
        "gpm": pd.DataFrame(),
        "gpm_cagr": pd.DataFrame(),
        "gpm_spread": pd.DataFrame(),
        "opm": pd.DataFrame(),
        "opm_cagr": pd.DataFrame(),
        "opm_spread": pd.DataFrame(),
        "roe": pd.DataFrame(),
        "roe_cagr": pd.DataFrame(),
        "roe_spread": pd.DataFrame()
    }

    style_type = "quality"

    # save data
    with open(r'D:\MyProject\FactorSelection\stock_factor_quality_quantiling.pickle', 'rb') as fr:
        stock_factor = pickle.load(fr)

    def filter_factor_data(self, strategy_nm):

        df_factor_data = self.stock_factor[self.stock_factor["quantile"] == 4]
        df_factor_data = df_factor_data[df_factor_data["item_nm"] == strategy_nm]

        df_factor_data = df_factor_data.sort_values(["date", "cmp_cd"]).reset_index(drop=True)
        df_factor_data = df_factor_data[["date", "cmp_cd", "z_score"]]

        return df_factor_data

    def update_schedule(self):
        '''
        전략 업데이트
        :return:
        '''

        for strategy_nm in self.my_strategy.keys():

            df_invest_sch = pd.DataFrame(columns=["date", "item_type", "item_cd", "w_type", "weight"])
            df_factor_data = pd.DataFrame()

            # 1. 투자 종목군 설정
            df_factor_data = self.filter_factor_data(strategy_nm)
            self.hashing_market_cap(df_factor_data)

            # 2. 투자 비중(weight) 설정
            for w_type in ["equal", "market_cap", "z_score"]:

                df_factor_data = self.get_weight(w_type, df_factor_data)
                df_factor_data["w_type"] = w_type

                df = df_factor_data[["date", "cmp_cd", "w_type", "weight"]].rename(columns={"cmp_cd": "item_cd"})
                df["item_type"] = "stock"
                df = df[["date", "item_type", "item_cd", "w_type", "weight"]]

                df_invest_sch = pd.concat([df_invest_sch, df])

            # save
            self.my_strategy[strategy_nm] = df_invest_sch

        self.monthly_invest_strategy["stock"][self.style_type] = self.my_strategy
        self.save_data()
        print("Complete:", __name__)


class Momentum(Stock):

    my_strategy = {
        "z_score_0to5": pd.DataFrame(),  # 구간 변화율 z_score
        "z_score_5to20": pd.DataFrame(),  # 구간 변화율 z_score
        "z_score_20to60": pd.DataFrame(),  # 구간 변화율 z_score
        "z_score_60to120": pd.DataFrame(),  # 구간 변화율 z_score
        "z_score_avg": pd.DataFrame(),  # 구간별 변화율 z_score 평균
    }

    style_type = 'momentum'

    # save data
    with open(r'D:\MyProject\FactorSelection\stock_factor_momentum_quantiling.pickle', 'rb') as fr:
        stock_factor = pickle.load(fr)

    def filter_factor_data(self, df_factor_data, strategy_nm):

        df_factor_data = self.stock_factor[self.stock_factor["quantile"] == 4]
        df_factor_data = df_factor_data[df_factor_data["item_nm"] == strategy_nm]

        df_factor_data = df_factor_data.sort_values(["date", "cmp_cd"]).reset_index(drop=True)
        df_factor_data = df_factor_data[["date", "cmp_cd", "z_score"]]

        return df_factor_data

    def update_schedule(self):
        '''
        전략 업데이트
        :return:
        '''

        for strategy_nm in self.my_strategy.keys():

            df_invest_sch = pd.DataFrame(columns=["date", "item_type", "item_cd", "w_type", "weight"])
            df_factor_data = pd.DataFrame()

            # 1. 투자 종목군 설정
            df_factor_data = self.filter_factor_data(df_factor_data, strategy_nm)
            self.hashing_market_cap(df_factor_data)

            # 2. 투자 비중(weight) 설정
            for w_type in ["equal", "market_cap", "z_score"]:

                df_factor_data = self.get_weight(w_type, df_factor_data)
                df_factor_data["w_type"] = w_type

                df = df_factor_data[["date", "cmp_cd", "w_type", "weight"]].rename(columns={"cmp_cd": "item_cd"})
                df["item_type"] = "stock"
                df = df[["date", "item_type", "item_cd", "w_type", "weight"]]

                df_invest_sch = pd.concat([df_invest_sch, df])

            # save
            self.my_strategy[strategy_nm] = df_invest_sch

        self.monthly_invest_strategy["stock"][self.style_type] = self.my_strategy
        self.save_data()
        print("Complete:", __name__)

