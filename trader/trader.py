import os
import sys
import time
import logging
import sqlite3
import pyupbit
import pandas as pd
from PyQt5.QtCore import QThread
from pyupbit import WebSocketManager
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.setting import *
from utility.static import now, timedelta_sec, strf_time, telegram_msg, timedelta_hour, strp_time


class Trader(QThread):
    def __init__(self, windowQ, workerQ, queryQ, soundQ, stgQ):
        super().__init__()
        self.log = logging.getLogger('Worker')
        self.log.setLevel(logging.INFO)
        filehandler = logging.FileHandler(filename=f"{system_path}/log/S{strf_time('%Y%m%d')}.txt", encoding='utf-8')
        self.log.addHandler(filehandler)

        self.windowQ = windowQ
        self.workerQ = workerQ
        self.queryQ = queryQ
        self.soundQ = soundQ
        self.stgQ = stgQ

        self.upbit = None                               # 매도수 주문 및 체결 확인용 객체
        self.buy_uuid = None                            # 매수 주문 저장용 list: [티커명, uuid]
        self.sell_uuid = None                           # 매도 주문 저장용 list: [티커명, uuid]
        self.websocketQ = None                          # 실시간데이터 수신용 웹소켓큐

        self.df_cj = pd.DataFrame(columns=columns_cj)   # 체결목록
        self.df_jg = pd.DataFrame(columns=columns_jg)   # 잔고목록
        self.df_tj = pd.DataFrame(columns=columns_tj)   # 잔고평가
        self.df_td = pd.DataFrame(columns=columns_td)   # 거래목록
        self.df_tt = pd.DataFrame(columns=columns_tt)   # 실현손익
        self.str_today = strf_time('%Y%m%d', timedelta_hour(-9))
        self.int_ctime = int(strf_time('%H%M%S', timedelta_hour(-9)))
        self.dict_jcdt = {}                             # 종목별 체결시간 저장용
        self.dict_intg = {
            '예수금': 0,
            '종목당투자금': 0,                            # 종목당 투자금은 int(예수금 / 최대매수종목수)로 계산
            '최대매수종목수': 5,
            '업비트수수료': 0.                            # 0.5% 일경우 0.005로 입력
        }
        self.dict_bool = {
            '모의모드': True,                            # 모의모드 False 상태시만 주문 전송
            '장초단타전략중단': False,
            '장중단타전략중단': False
        }
        self.dict_time = {
            '체결확인': now(),                           # 1초 마다 체결 확인용
            '거래정보': now(),                           # UI 갱신용
            '부가정보': now()                            # UI 갱신용
        }

    def run(self):
        self.LoadDatabase()
        self.GetKey()
        self.GetBalances()
        self.EventLoop()

    def LoadDatabase(self):
        """
        프로그램 구동 시 당일 체결목록, 당일 거래목록, 잔고목록을 불러온다.
        체결과 거래목록은 바로 갱신하고 잔고목록은 예수금을 불러온 이후 갱신한다.
        """
        con = sqlite3.connect(db_stg)
        df = pd.read_sql(f"SELECT * FROM chegeollist WHERE 체결시간 LIKE '{self.str_today}%'", con)
        self.df_cj = df.set_index('index').sort_values(by=['체결시간'], ascending=False)
        df = pd.read_sql(f'SELECT * FROM jangolist', con)
        self.df_jg = df.set_index('index').sort_values(by=['매입금액'], ascending=False)
        df = pd.read_sql(f"SELECT * FROM tradelist WHERE 체결시간 LIKE '{self.str_today}%'", con)
        self.df_td = df.set_index('index').sort_values(by=['체결시간'], ascending=False)
        con.close()

        if len(self.df_cj) > 0:
            self.windowQ.put([ui_num['체결목록'], self.df_cj])
        if len(self.df_td) > 0:
            self.windowQ.put([ui_num['거래목록'], self.df_td])

    def GetKey(self):
        """
        user.txt 파일에서 업비트 access 키와 secret 키를 읽어 self.upbit 객체 생성
        해당 객체는 매도수 주문 및 체결확인용이다.
        """
        f = open(f'{system_path}/utility/user.txt')
        lines = f.readlines()
        access_key = lines[0].strip()
        secret_key = lines[1].strip()
        f.close()
        self.upbit = pyupbit.Upbit(access_key, secret_key)

    def GetBalances(self):
        """ 예수금 조회 및 종목당투자금 계산 """
        if self.dict_bool['모의모드']:
            self.dict_intg['예수금'] = 100000000
        else:
            self.dict_intg['예수금'] = int(float(self.upbit.get_balances()[0]['balance']))
        self.dict_intg['종목당투자금'] = int(self.dict_intg['예수금'] / self.dict_intg['최대매수종목수'])

        if len(self.df_td) > 0:
            self.UpdateTotaltradelist(first=True)

    def EventLoop(self):
        tickers = pyupbit.get_tickers(fiat="KRW")
        self.stgQ.put(['관심종목초기화', tickers])
        websocketQ = WebSocketManager('ticker', tickers)
        while True:
            """
            주문용 큐를 감시한다.
            주문용 큐에 대한 입력은 모두 전략 연산 프로세스에서 이뤄진다.
            """
            if not self.workerQ.empty():
                data = self.workerQ.get()
                if data[0] == '매수':
                    self.Buy(data[1], data[2], data[3])
                elif data[0] == '매도':
                    self.Sell(data[1], data[2], data[3])

            """
            실시간 웹소켓큐로 데이터가 들어오면 우선 티커명, 시간을 뽑아
            티커별 마지막 시간이 저장된 self.dict_jcdt의 시간과 틀리면 전략 연산 프로세스로 데이터를 보낸다. 
            """
            data = websocketQ.get()
            ticker = data['code']
            d = data['trade_date']
            t = data['trade_time']

            try:
                last_jcct = self.dict_jcdt[ticker]
            except KeyError:
                last_jcct = None

            if last_jcct is None or t != last_jcct:
                self.dict_jcdt[ticker] = t

                c = data['trade_price']
                h = data['high_price']
                low = data['low_price']
                per = round(data['change_rate'] * 100, 2)
                dm = data['acc_trade_price']
                bid = data['acc_bid_volume']
                ask = data['acc_ask_volume']

                uuidnone = self.buy_uuid is None
                injango = ticker in self.df_jg.index
                data = [ticker, c, h, low, per, dm, bid, ask, t, uuidnone, injango, self.dict_intg['종목당투자금']]
                self.stgQ.put(data)

                """ 잔고목록 갱신 및 매도조건 확인 """
                if injango:
                    ch = round(bid / ask * 100, 2)
                    self.UpdateJango(ticker, c, ch)

            """
            전략은 9시~10시 장초단타전략과 이외의 시간 장중단타전략 두가지로 운영한다.
            전략의 시간이 변경될 때 현재 잔고를 모두 청산하고
            전략 연산 프로세스의 관심종목용 딕셔너리를 초기화하는 명령을 보낸다.  
            """
            if self.int_ctime <= 90000 < int(t) and not self.dict_bool['장중단타전략중단']:
                self.dict_bool['장중단타전략중단'] = True
                self.dict_bool['장초단타전략중단'] = False
                self.JangoCheongsan()
                self.stgQ.put(['장초단타전략시작', ''])

            if self.int_ctime <= 100000 < int(t) and not self.dict_bool['장초단타전략중단']:
                self.dict_bool['장초단타전략중단'] = True
                self.dict_bool['장중단타전략중단'] = False
                self.JangoCheongsan()
                self.stgQ.put(['장중단타전략시작', ''])

            """ 전략 운영 시간 기록용 """
            if int(t) > self.int_ctime:
                self.int_ctime = int(t)

            """ 날짜 변경시 날짜변수 갱신, 당일 실현손익 저장, 각종목록 초기화 """
            if int(d) > int(self.str_today):
                self.str_today = d
                self.queryQ.put([self.df_tt, 'totaltradelist', 'append'])
                self.df_cj = pd.DataFrame(columns=columns_cj)
                self.df_td = pd.DataFrame(columns=columns_td)
                self.df_tt = pd.DataFrame(columns=columns_tt)
                telegram_msg('관심종목 및 거래정보를 업데이트하였습니다.')

            """ 주문의 체결확인은 1초마다 반복한다. """
            if not self.dict_bool['모의투자']:
                if self.buy_uuid is not None and ticker == self.buy_uuid[0] and now() > self.dict_time['매수체결확인']:
                    self.CheckBuyChegeol(ticker)
                    self.dict_time['매수체결확인'] = timedelta_sec(1)
                if self.sell_uuid is not None and ticker == self.sell_uuid[0] and now() > self.dict_time['매도체결확인']:
                    self.CheckSellChegeol(ticker)
                    self.dict_time['매도체결확인'] = timedelta_sec(1)

            """ 잔고평가 및 잔고목록 갱신도 1초마다 반복한다. """
            if now() > self.dict_time['거래정보']:
                self.UpdateTotaljango()
                self.dict_time['거래정보'] = timedelta_sec(1)
            if now() > self.dict_time['부가정보']:
                self.windowQ.put([1, '부가정보업데이트'])
                self.dict_time['부가정보'] = timedelta_sec(2)

    """
    모의모드 시 실제 매도수 주문을 전송하지 않고 바로 체결목록, 잔고목록 등을 갱신한다.
    실매매 시 매도수 아이디 및 티커명을 매도, 매수 구분하여 변수에 저장하고
    해당 변수값이 None이 아닐 경우 get_order 함수로 체결확인을 1초마다 반복실행한다.
    체결이 완료되면 관련목록을 갱신하고 변수값이 다시 None으로 변경된다.
    체결확인 후 잔고목록를 갱신 한 이후에 전략 연산 프로세스로 체결완료 신호를 보낸다.
    모든 목록은 갱신될 때마다 쿼리 프로세스로 보내어 DB에 실시간으로 기록된다.
    매수 주문은 예수금 부족인지 아닌지를 우선 확인하여 예수금 부족일 경우 주문구분을 시드부족으로 체결목록에 기록하고
    전략 연산 프로세스의 주문 리스트 삭제용 매수완료 신호만 보낸다.
    예수금 부족 상태이며 잔고목록에 없는 상태일 경우 전략 프로세스에서 지속적으로 매수 신호가 발생할 수 있다.
    그러므로 재차 시드부족이 발생한 종목은 체결목록에서 마지막 체결시간이 3분이내면 체결목록에 기록하지 않는다.
    """
    def Buy(self, ticker, c, oc):
        if self.buy_uuid is not None:
            self.stgQ.put(['매수완료', ticker])
            return

        if self.dict_intg['예수금'] < c * oc:
            df = self.df_cj[(self.df_cj['주문구분'] == '시드부족') & (self.df_cj['종목명'] == ticker)]
            if len(df) == 0 or now() > timedelta_sec(180, strp_time('%Y%m%d%H%M%S%f', df['체결시간'][0])):
                self.UpdateBuy(ticker, c, oc, cancle=True)
            self.stgQ.put(['매수완료', ticker])
            return

        if self.dict_bool['모의모드']:
            self.UpdateBuy(ticker, c, oc)
        else:
            ret = self.upbit.buy_market_order(ticker, self.dict_intg['종목당투자금'])
            self.buy_uuid = [ticker, ret[0]['uuid']]
            self.dict_time['체결확인'] = timedelta_sec(1)

    def Sell(self, ticker, c, oc):
        if self.sell_uuid is not None:
            self.stgQ.put(['매도완료', ticker])
            return

        if self.dict_bool['모의모드']:
            self.UpdateSell(ticker, c, oc)
        else:
            ret = self.upbit.sell_market_order(ticker, oc)
            self.sell_uuid = [ticker, ret[0]['uuid']]
            self.dict_time['체결확인'] = timedelta_sec(1)

    def UpdateJango(self, ticker, c, ch):
        prec = self.df_jg['현재가'][ticker]
        if prec != c:
            bg = self.df_jg['매입금액'][ticker]
            jc = int(self.df_jg['보유수량'][ticker])
            pg, sg, sp = self.GetPgSgSp(bg, jc * c)
            columns = ['현재가', '수익률', '평가손익', '평가금액']
            self.df_jg.at[ticker, columns] = c, sp, sg, pg
            data = [ticker, sp, jc, ch, c]
            self.stgQ.put(data)

    def JangoCheongsan(self):
        for ticker in self.df_jg.index:
            c = self.df_jg['현재가'][ticker]
            oc = self.df_jg['보유수량'][ticker]
            if self.dict_bool['모의모드']:
                self.UpdateSell(ticker, c, oc)
            else:
                self.upbit.sell_market_order(ticker, oc)
                time.sleep(0.2)
        self.soundQ.put('잔고청산 주문을 전송하였습니다.')

    def CheckBuyChegeol(self, ticker):
        ret = self.upbit.get_order(self.buy_uuid[1])
        if ret is not None and ret['state'] == 'done':
            cp = ret['price']
            cc = ret['executed_volume']
            self.UpdateBuy(ticker, cp, cc)
            self.cstgQ.put(['매수완료', ticker])
            self.buy_uuid = None

    def CheckSellChegeol(self, ticker):
        ret = self.upbit.get_order(self.sell_uuid[1])
        if ret is not None and ret['state'] == 'done':
            cp = ret['price']
            cc = ret['executed_volume']
            self.UpdateSell(ticker, cp, cc)
            self.cstgQ.put(['매도완료', ticker])
            self.sell_uuid = None

    def UpdateBuy(self, ticker, cp, cc, cancle=False):
        dt = strf_time('%Y%m%d%H%M%S%f', timedelta_hour(-9))
        order_gubun = '매수' if not cancle else '시드부족'
        self.df_cj.at[dt] = ticker, order_gubun, cc, 0, cp, cp, dt
        self.df_cj.sort_values(by='체결시간', ascending=False, inplace=True)
        self.windowQ.put([ui_num['체결목록'], self.df_cj])
        if not cancle:
            bg = cp * cc
            pg, sg, sp = self.GetPgSgSp(bg, bg)
            self.dict_intg['예수금'] -= bg
            self.df_jg.at[ticker] = ticker, cp, cp, sp, sg, bg, pg, cc
            self.df_jg.sort_values(by=['매입금액'], ascending=False, inplace=True)
            self.queryQ.put([self.df_jg, 'jangolist', 'replace'])
            text = f'매매 시스템 체결 알림 - {ticker} {cc}코인 매수'
            self.log.info(f'[{now()}] {text}')
            self.windowQ.put([0, text])
            self.soundQ.put(f'{ticker} {cc}코인을 매수하였습니다.')
            telegram_msg(f'매수 알림 - {ticker} {cp} {cc}')
        df = pd.DataFrame([[ticker, order_gubun, cc, 0, cp, cp, dt]], columns=columns_cj, index=[dt])
        self.queryQ.put([df, 'chegeollist', 'append'])

    def UpdateSell(self, ticker, cp, cc):
        dt = strf_time('%Y%m%d%H%M%S%f', timedelta_hour(-9))
        bp = self.df_jg['매입가'][ticker]
        bg = bp * cc
        pg, sg, sp = self.GetPgSgSp(bg, cp * cc)
        self.dict_intg['예수금'] += bg + sg

        self.df_jg.drop(index=ticker, inplace=True)
        self.df_cj.at[dt] = ticker, '매도', cc, 0, cp, cp, dt
        self.df_td.at[dt] = ticker, bg, pg, cc, sp, sg, dt
        self.df_td.sort_values(by=['체결시간'], ascending=False, inplace=True)

        self.windowQ.put([ui_num['체결목록'], self.df_cj])
        self.windowQ.put([ui_num['거래목록'], self.df_td])

        text = f'매매 시스템 체결 알림 - {ticker} {bp}코인 매도'
        self.log.info(f'[{now()}] {text}')
        self.windowQ.put([0, text])
        self.soundQ.put(f'{ticker} {cc}코인을 매도하였습니다.')

        self.queryQ.put([self.df_jg, 'jangolist', 'replace'])
        df = pd.DataFrame([[ticker, '매도', cc, 0, cp, cp, dt]], columns=columns_cj, index=[dt])
        self.queryQ.put([df, 'chegeollist', 'append'])
        df = pd.DataFrame([[ticker, bg, pg, cc, sp, sg, dt]], columns=columns_td, index=[dt])
        self.queryQ.put([df, 'tradelist', 'append'])
        telegram_msg(f'매도 알림 - {ticker} {cp} {cc}')
        self.UpdateTotaltradelist()

    def UpdateTotaltradelist(self, first=False):
        tsg = self.df_td['매도금액'].sum()
        tbg = self.df_td['매수금액'].sum()
        tsig = self.df_td[self.df_td['수익금'] > 0]['수익금'].sum()
        tssg = self.df_td[self.df_td['수익금'] < 0]['수익금'].sum()
        sg = self.df_td['수익금'].sum()
        sp = round(sg / tbg * 100, 2)
        tdct = len(self.df_td)
        self.df_tt = pd.DataFrame([[tdct, tbg, tsg, tsig, tssg, sp, sg]], columns=columns_tt, index=[self.str_today])
        self.windowQ.put([ui_num['거래합계'], self.df_tt])
        if not first:
            telegram_msg(f'손익 알림 - 총매수금액 {tbg}, 총매도금액{tsg}, 수익 {tsig}, 손실 {tssg}, 수익급합계 {sg}')

    # noinspection PyMethodMayBeStatic
    def GetPgSgSp(self, bg, cg):
        sfee = cg * self.dict_intg['업비트수수료']
        bfee = bg * self.dict_intg['업비트수수료']
        pg = int(cg - sfee - bfee)
        sg = pg - bg
        sp = round(sg / bg * 100, 2)
        return pg, sg, sp

    def UpdateTotaljango(self):
        if len(self.df_jg) > 0:
            tsg = self.df_jg['평가손익'].sum()
            tbg = self.df_jg['매입금액'].sum()
            tpg = self.df_jg['평가금액'].sum()
            bct = len(self.df_jg)
            tsp = round(tsg / tbg * 100, 2)
            ttg = self.dict_intg['예수금'] + tpg
            self.df_tj.at[self.str_today] = ttg, self.dict_intg['예수금'], bct, tsp, tsg, tbg, tpg
        else:
            self.df_tj.at[self.str_today] = self.dict_intg['예수금'], self.dict_intg['예수금'], 0, 0.0, 0, 0, 0
        self.windowQ.put([ui_num['잔고목록'], self.df_jg])
        self.windowQ.put([ui_num['잔고평가'], self.df_tj])
