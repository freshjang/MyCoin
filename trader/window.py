import os
import sys
import psutil
import sqlite3
import pandas as pd
from PyQt5.QtGui import QPalette
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import Qt, QThread
from multiprocessing import Queue, Process
from trader import Trader
from strategy import Strategy
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from utility.query import Query
from utility.sound import Sound
from utility.setting import *
from utility.static import strf_time, thread_decorator, strp_time, timedelta_hour


class Window(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()

        def setPushbutton(name, groupbox, buttonclicked, cmd=None):
            pushbutton = QtWidgets.QPushButton(name, groupbox)
            pushbutton.setStyleSheet(style_bc_bt)
            pushbutton.setFont(qfont12)
            if cmd is not None:
                pushbutton.clicked.connect(lambda: buttonclicked(cmd))
            else:
                pushbutton.clicked.connect(lambda: buttonclicked(name))
            return pushbutton

        def setTablewidget(tab, columns, colcount, rowcount):
            tableWidget = QtWidgets.QTableWidget(tab)
            tableWidget.verticalHeader().setDefaultSectionSize(23)
            tableWidget.verticalHeader().setVisible(False)
            tableWidget.setAlternatingRowColors(True)
            tableWidget.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
            tableWidget.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
            tableWidget.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            tableWidget.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            tableWidget.setColumnCount(len(columns))
            tableWidget.setRowCount(rowcount)
            tableWidget.setHorizontalHeaderLabels(columns)
            if columns[0] == 'ticker':
                tableWidget.setColumnWidth(0, 85)
                tableWidget.setColumnWidth(1, 55)
                tableWidget.setColumnWidth(2, 55)
                tableWidget.setColumnWidth(3, 90)
                tableWidget.setColumnWidth(4, 126)
                tableWidget.setColumnWidth(5, 55)
                tableWidget.setColumnWidth(6, 90)
                tableWidget.setColumnWidth(7, 55)
                tableWidget.setColumnWidth(8, 55)
            elif colcount >= 7:
                tableWidget.setColumnWidth(0, 126)
                tableWidget.setColumnWidth(1, 90)
                tableWidget.setColumnWidth(2, 90)
                tableWidget.setColumnWidth(3, 90)
                tableWidget.setColumnWidth(4, 90)
                tableWidget.setColumnWidth(5, 90)
                tableWidget.setColumnWidth(6, 90)
                if colcount >= 8:
                    tableWidget.setColumnWidth(7, 90)
            return tableWidget

        self.setFont(qfont12)
        self.setWindowFlags(Qt.FramelessWindowHint)

        self.table_tabWidget = QtWidgets.QTabWidget(self)
        self.td_tab = QtWidgets.QWidget()
        self.gj_tab = QtWidgets.QWidget()
        self.st_tab = QtWidgets.QWidget()
        self.sg_tab = QtWidgets.QWidget()

        self.tt_tableWidget = setTablewidget(self.td_tab, columns_tt, len(columns_tt), 1)
        self.td_tableWidget = setTablewidget(self.td_tab, columns_td, len(columns_td), 17)
        self.tj_tableWidget = setTablewidget(self.td_tab, columns_tj, len(columns_tj), 1)
        self.jg_tableWidget = setTablewidget(self.td_tab, columns_jg, len(columns_jg), 17)
        self.cj_tableWidget = setTablewidget(self.td_tab, columns_cj, len(columns_cj), 17)
        self.gj_tableWidget = setTablewidget(self.gj_tab, columns_gj3, len(columns_gj3), 56)

        self.st_groupBox = QtWidgets.QGroupBox(self.st_tab)
        self.calendarWidget = QtWidgets.QCalendarWidget(self.st_groupBox)
        todayDate = QtCore.QDate.currentDate()
        self.calendarWidget.setCurrentPage(todayDate.year(), todayDate.month())
        self.calendarWidget.clicked.connect(self.CalendarClicked)
        self.stn_tableWidget = setTablewidget(self.st_tab, columns_sn, len(columns_sn), 1)
        self.stl_tableWidget = setTablewidget(self.st_tab, columns_st, len(columns_st), 44)

        self.sg_groupBox = QtWidgets.QGroupBox(self.sg_tab)
        self.sg_pushButton_01 = setPushbutton('일별집계', self.sg_groupBox, self.ButtonClicked)
        self.sg_pushButton_02 = setPushbutton('월별집계', self.sg_groupBox, self.ButtonClicked)
        self.sg_pushButton_03 = setPushbutton('연도별집계', self.sg_groupBox, self.ButtonClicked)
        self.sgt_tableWidget = setTablewidget(self.sg_tab, columns_ln, len(columns_ln), 1)
        self.sgl_tableWidget = setTablewidget(self.sg_tab, columns_lt, len(columns_lt), 54)

        self.table_tabWidget.addTab(self.td_tab, '계좌평가')
        self.table_tabWidget.addTab(self.gj_tab, '관심종목')
        self.table_tabWidget.addTab(self.st_tab, '거래목록')
        self.table_tabWidget.addTab(self.sg_tab, '수익현황')

        self.info_label = QtWidgets.QLabel(self)

        self.setGeometry(2065, 0, 692, 1400)
        self.table_tabWidget.setGeometry(5, 5, 682, 1390)
        self.info_label.setGeometry(285, 1, 400, 30)

        self.tt_tableWidget.setGeometry(5, 5, 668, 42)
        self.td_tableWidget.setGeometry(5, 52, 668, 415)
        self.tj_tableWidget.setGeometry(5, 472, 668, 42)
        self.jg_tableWidget.setGeometry(5, 519, 668, 415)
        self.cj_tableWidget.setGeometry(5, 939, 668, 415)

        self.gj_tableWidget.setGeometry(5, 5, 668, 1352)

        self.st_groupBox.setGeometry(5, 3, 668, 278)
        self.calendarWidget.setGeometry(5, 11, 658, 258)
        self.stn_tableWidget.setGeometry(5, 287, 668, 42)
        self.stl_tableWidget.setGeometry(5, 334, 668, 1022)

        self.sg_groupBox.setGeometry(5, 3, 668, 48)
        self.sg_pushButton_01.setGeometry(5, 11, 216, 30)
        self.sg_pushButton_02.setGeometry(226, 12, 216, 30)
        self.sg_pushButton_03.setGeometry(447, 12, 216, 30)
        self.sgt_tableWidget.setGeometry(5, 57, 668, 42)
        self.sgl_tableWidget.setGeometry(5, 104, 668, 1252)

        self.dict_intg = {
            '체결강도차이1': 0.,
            '평균시간1': 0,
            '거래대금차이1': 0,
            '체결강도하한1': 0.,
            '누적거래대금하한1': 0,
            '등락율하한1': 0.,
            '등락율상한1': 0.,
            '청산수익률1': 0.,

            '체결강도차이2': 0.,
            '평균시간2': 0,
            '거래대금차이2': 0,
            '체결강도하한2': 0.,
            '누적거래대금하한2': 0,
            '등락율하한2': 0.,
            '등락율상한2': 0.,
            '청산수익률2': 0.
        }

        self.info1 = [0., 0, 0.]
        self.info2 = [0., 0, 0.]

        self.worker = Trader(windowQ, workerQ, queryQ, soundQ, stgQ)
        self.worker.start()

        self.writer = Writer()
        self.writer.data0.connect(self.UpdateTablewidget)
        self.writer.data1.connect(self.UpdateGoansimjongmok)
        self.writer.data2.connect(self.UpdateInfo)
        self.writer.start()

    def UpdateGoansimjongmok(self, data):
        gubun = data[0]
        dict_df = data[1]

        if gubun == ui_num['단타설정']:
            df = data[1]
            self.dict_intg['체결강도차이1'] = df['체결강도차이1'][0]
            self.dict_intg['평균시간1'] = df['평균시간1'][0]
            self.dict_intg['거래대금차이1'] = df['거래대금차이1'][0]
            self.dict_intg['체결강도하한1'] = df['체결강도하한1'][0]
            self.dict_intg['누적거래대금하한1'] = df['누적거래대금하한1'][0]
            self.dict_intg['등락율하한1'] = df['등락율하한1'][0]
            self.dict_intg['등락율상한1'] = df['등락율상한1'][0]
            self.dict_intg['청산수익률1'] = df['청산수익률1'][0]
            self.dict_intg['체결강도차이2'] = df['체결강도차이2'][0]
            self.dict_intg['평균시간2'] = df['평균시간2'][0]
            self.dict_intg['거래대금차이2'] = df['거래대금차이2'][0]
            self.dict_intg['체결강도하한2'] = df['체결강도하한2'][0]
            self.dict_intg['누적거래대금하한2'] = df['누적거래대금하한2'][0]
            self.dict_intg['등락율하한2'] = df['등락율하한2'][0]
            self.dict_intg['등락율상한2'] = df['등락율상한2'][0]
            self.dict_intg['청산수익률2'] = df['청산수익률2'][0]
            return

        if gubun == ui_num['관심종목'] and self.table_tabWidget.currentWidget() != self.gj_tab:
            return

        if len(dict_df) == 0:
            self.gj_tableWidget.clearContents()
            return

        def changeFormat(text):
            text = str(text)
            try:
                format_data = format(int(text), ',')
            except ValueError:
                format_data = format(float(text), ',')
                if len(format_data.split('.')) >= 2:
                    if len(format_data.split('.')[1]) == 1:
                        format_data += '0'
            return format_data

        self.gj_tableWidget.setRowCount(len(dict_df))
        time = 1 if 90000 < int(strf_time('%H%M%S', timedelta_hour(-9))) <= 1000000 else 2
        for j, ticker in enumerate(list(dict_df.keys())):
            item = QtWidgets.QTableWidgetItem(ticker)
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
            self.gj_tableWidget.setItem(j, 0, item)

            smavg = dict_df[ticker]['거래대금'][self.dict_intg[f'평균시간{time}'] + 1]
            item = QtWidgets.QTableWidgetItem(changeFormat(smavg).split('.')[0])
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.gj_tableWidget.setItem(j, columns_gj3.index('smavg'), item)

            chavg = dict_df[ticker]['체결강도'][self.dict_intg[f'평균시간{time}'] + 1]
            item = QtWidgets.QTableWidgetItem(changeFormat(chavg))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.gj_tableWidget.setItem(j, columns_gj3.index('chavg'), item)

            chhigh = dict_df[ticker]['최고체결강도'][self.dict_intg[f'평균시간{time}'] + 1]
            item = QtWidgets.QTableWidgetItem(changeFormat(chhigh))
            item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            self.gj_tableWidget.setItem(j, columns_gj3.index('chhigh'), item)

            for i, column in enumerate(columns_gj2):
                if column in ['거래대금', '누적거래대금']:
                    item = QtWidgets.QTableWidgetItem(changeFormat(dict_df[ticker][column][0]).split('.')[0])
                else:
                    item = QtWidgets.QTableWidgetItem(changeFormat(dict_df[ticker][column][0]))
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
                if column == '등락율':
                    if self.dict_intg[f'등락율하한{time}'] <= dict_df[ticker][column][0] <= self.dict_intg[f'등락율상한{time}']:
                        item.setForeground(color_fg_bt)
                    else:
                        item.setForeground(color_fg_dk)
                elif column == '고저평균대비등락율':
                    if dict_df[ticker][column][0] >= 0:
                        item.setForeground(color_fg_bt)
                    else:
                        item.setForeground(color_fg_dk)
                elif column == '거래대금':
                    if dict_df[ticker][column][0] >= smavg + self.dict_intg[f'거래대금차이{time}']:
                        item.setForeground(color_fg_bt)
                    else:
                        item.setForeground(color_fg_dk)
                elif column == '누적거래대금':
                    if dict_df[ticker][column][0] >= self.dict_intg[f'누적거래대금하한{time}']:
                        item.setForeground(color_fg_bt)
                    else:
                        item.setForeground(color_fg_dk)
                elif column == '체결강도':
                    if dict_df[ticker][column][0] >= self.dict_intg[f'체결강도하한{time}'] and \
                            dict_df[ticker][column][0] >= chavg + self.dict_intg[f'체결강도차이{time}']:
                        item.setForeground(color_fg_bt)
                    else:
                        item.setForeground(color_fg_dk)
                self.gj_tableWidget.setItem(j, i + 1, item)

        if len(dict_df) < 57:
            self.gj_tableWidget.setRowCount(57)

    def UpdateTablewidget(self, data):
        gubun = data[0]
        df = data[1]

        def changeFormat(text):
            text = str(text)
            try:
                format_data = format(int(text), ',')
            except ValueError:
                format_data = format(float(text), ',')
                if len(format_data.split('.')) >= 2:
                    if len(format_data.split('.')[1]) == 1:
                        format_data += '0'
            return format_data

        tableWidget = None
        if gubun == ui_num['거래합계']:
            tableWidget = self.tt_tableWidget
        elif gubun == ui_num['거래목록']:
            tableWidget = self.td_tableWidget
        elif gubun == ui_num['잔고평가']:
            tableWidget = self.tj_tableWidget
        elif gubun == ui_num['잔고목록']:
            tableWidget = self.jg_tableWidget
        elif gubun == ui_num['체결목록']:
            tableWidget = self.cj_tableWidget
        elif gubun == ui_num['당일합계']:
            tableWidget = self.stn_tableWidget
        elif gubun == ui_num['당일상세']:
            tableWidget = self.stl_tableWidget
        elif gubun == ui_num['누적합계']:
            tableWidget = self.sgt_tableWidget
        elif gubun == ui_num['누적상세']:
            tableWidget = self.sgl_tableWidget
        if tableWidget is None:
            return

        if len(df) == 0:
            tableWidget.clearContents()
            return

        tableWidget.setRowCount(len(df))
        for j, index in enumerate(df.index):
            for i, column in enumerate(df.columns):
                if column == '체결시간':
                    cgtime = df[column][index]
                    cgtime = f'{cgtime[8:10]}:{cgtime[10:12]}:{cgtime[12:14]}'
                    item = QtWidgets.QTableWidgetItem(cgtime)
                elif column in ['거래일자', '일자']:
                    day = df[column][index]
                    if '.' not in day:
                        day = day[:4] + '.' + day[4:6] + '.' + day[6:]
                    item = QtWidgets.QTableWidgetItem(day)
                elif column in ['종목명', '주문구분', '기간']:
                    item = QtWidgets.QTableWidgetItem(str(df[column][index]))
                elif column != '수익률':
                    item = QtWidgets.QTableWidgetItem(changeFormat(df[column][index]).split('.')[0])
                else:
                    item = QtWidgets.QTableWidgetItem(changeFormat(df[column][index]))

                if column == '종목명':
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
                elif column in ['거래횟수', '추정예탁자산', '추정예수금', '보유종목수', '주문구분',
                                '체결시간', '거래일자', '기간', '일자']:
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignCenter)
                else:
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)

                if '수익률' in df.columns:
                    if df['수익률'][index] >= 0:
                        item.setForeground(color_fg_bt)
                    else:
                        item.setForeground(color_fg_dk)
                elif gubun == ui_num['체결목록']:
                    if df['주문구분'][index] == '매수':
                        item.setForeground(color_fg_bt)
                    elif df['주문구분'][index] == '매도':
                        item.setForeground(color_fg_dk)
                    elif df['주문구분'][index] in ['매도취소', '매수취소']:
                        item.setForeground(color_fg_bc)
                tableWidget.setItem(j, i, item)

        if len(df) < 13 and gubun in [ui_num['거래목록'], ui_num['잔고목록'], ui_num['체결목록']]:
            tableWidget.setRowCount(17)
        elif len(df) < 44 and gubun == ui_num['당일상세']:
            tableWidget.setRowCount(44)
        elif len(df) < 54 and gubun == ui_num['누적상세']:
            tableWidget.setRowCount(54)

    def UpdateInfo(self, data):
        if data[0] == 0:
            self.info_label.setText(data[1])
        elif data[0] == 1:
            memory = round(self.info1[0] + self.info2[0], 2)
            thread = self.info1[1] + self.info2[1]
            cpu = round(self.info1[2] + self.info2[2], 2)
            text = f'Total Process - Memory {memory}MB | Thread {thread}EA | CPU {cpu}%'
            self.info_label.setText(text)
            self.GetInfo()
        elif data[0] == 2:
            self.info2 = [data[1], data[2], data[3]]

    @thread_decorator
    def GetInfo(self):
        p = psutil.Process(os.getpid())
        memory = round(p.memory_info()[0] / 2 ** 20.86, 2)
        thread = p.num_threads()
        cpu = round(p.cpu_percent(interval=2) / 2, 2)
        self.info1 = [memory, thread, cpu]

    def CalendarClicked(self):
        date = self.calendarWidget.selectedDate()
        searchday = date.toString('yyyyMMdd')
        con = sqlite3.connect(db_stg)
        df = pd.read_sql(f"SELECT * FROM tradelist WHERE 체결시간 LIKE '{searchday}%'", con)
        con.close()
        if len(df) > 0:
            df = df.set_index('index')
            df.sort_values(by=['체결시간'], ascending=True, inplace=True)
            df = df[['체결시간', '종목명', '매수금액', '매도금액', '주문수량', '수익률', '수익금']].copy()
            nbg, nsg = df['매수금액'].sum(), df['매도금액'].sum()
            sp = round((nsg / nbg - 1) * 100, 2)
            npg, nmg, nsig = df[df['수익금'] > 0]['수익금'].sum(), df[df['수익금'] < 0]['수익금'].sum(), df['수익금'].sum()
            df2 = pd.DataFrame(columns=columns_sn)
            df2.at[0] = searchday, nbg, nsg, npg, nmg, sp, nsig
        else:
            df = pd.DataFrame(columns=columns_st)
            df2 = pd.DataFrame(columns=columns_sn)
        self.UpdateTablewidget([ui_num['당일합계'], df2])
        self.UpdateTablewidget([ui_num['당일상세'], df])

    def ButtonClicked(self, cmd):
        if '집계' in cmd:
            con = sqlite3.connect(db_stg)
            df = pd.read_sql('SELECT * FROM totaltradelist', con)
            con.close()
            df = df[::-1]
            if len(df) > 0:
                sd = strp_time('%Y%m%d', df['index'][df.index[0]])
                ld = strp_time('%Y%m%d', df['index'][df.index[-1]])
                pr = str((sd - ld).days + 1) + '일'
                nbg, nsg = df['총매수금액'].sum(), df['총매도금액'].sum()
                sp = round((nsg / nbg - 1) * 100, 2)
                npg, nmg = df['총수익금액'].sum(), df['총손실금액'].sum()
                nsig = df['수익금합계'].sum()
                df2 = pd.DataFrame(columns=columns_ln)
                df2.at[0] = pr, nbg, nsg, npg, nmg, sp, nsig
                self.UpdateTablewidget([ui_num['누적합계'], df2])
            else:
                return
            if cmd == '일별집계':
                df = df.rename(columns={'index': '일자'})
                self.UpdateTablewidget([ui_num['누적상세'], df])
            elif cmd == '월별집계':
                df['일자'] = df['index'].apply(lambda x: x[:6])
                df2 = pd.DataFrame(columns=columns_lt)
                lastmonth = df['일자'][df.index[-1]]
                month = strf_time('%Y%m')
                while int(month) >= int(lastmonth):
                    df3 = df[df['일자'] == month]
                    if len(df3) > 0:
                        tbg, tsg = df3['총매수금액'].sum(), df3['총매도금액'].sum()
                        sp = round((tsg / tbg - 1) * 100, 2)
                        tpg, tmg = df3['총수익금액'].sum(), df3['총손실금액'].sum()
                        ttsg = df3['수익금합계'].sum()
                        df2.at[month] = month, tbg, tsg, tpg, tmg, sp, ttsg
                    month = str(int(month) - 89) if int(month[4:]) == 1 else str(int(month) - 1)
                self.UpdateTablewidget([ui_num['누적상세'], df2])
            elif cmd == '연도별집계':
                df['일자'] = df['index'].apply(lambda x: x[:4])
                df2 = pd.DataFrame(columns=columns_lt)
                lastyear = df['일자'][df.index[-1]]
                year = strf_time('%Y')
                while int(year) >= int(lastyear):
                    df3 = df[df['일자'] == year]
                    if len(df3) > 0:
                        tbg, tsg = df3['총매수금액'].sum(), df3['총매도금액'].sum()
                        sp = round((tsg / tbg - 1) * 100, 2)
                        tpg, tmg = df3['총수익금액'].sum(), df3['총손실금액'].sum()
                        ttsg = df3['수익금합계'].sum()
                        df2.at[year] = year, tbg, tsg, tpg, tmg, sp, ttsg
                    year = str(int(year) - 1)
                self.UpdateTablewidget([ui_num['누적상세'], df2])


class Writer(QThread):
    data0 = QtCore.pyqtSignal(list)
    data1 = QtCore.pyqtSignal(list)
    data2 = QtCore.pyqtSignal(list)

    def __init__(self):
        super().__init__()
        self.windowQ = windowQ

    def run(self):
        while True:
            data = self.windowQ.get()
            if len(data) == 2:
                if type(data[1]) == pd.DataFrame:
                    if data[0] != ui_num['단타설정']:
                        self.data0.emit(data)
                    else:
                        self.data1.emit(data)
                elif type(data[1]) == dict:
                    self.data1.emit(data)
                elif type(data[1]) == str:
                    self.data2.emit(data)
            elif len(data) == 4:
                self.data2.emit(data)


if __name__ == '__main__':
    windowQ, workerQ, queryQ, soundQ, stgQ = Queue(), Queue(), Queue(), Queue(), Queue()
    Process(target=Query, args=(queryQ,)).start()
    Process(target=Sound, args=(soundQ,)).start()
    Process(target=Strategy, args=(windowQ, workerQ, queryQ, stgQ)).start()
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('fusion')
    palette = QPalette()
    palette.setColor(QPalette.Window, color_bg_bc)
    palette.setColor(QPalette.Background, color_bg_bc)
    palette.setColor(QPalette.WindowText, color_fg_bc)
    palette.setColor(QPalette.Base, color_bg_bc)
    palette.setColor(QPalette.AlternateBase, color_bg_dk)
    palette.setColor(QPalette.Text, color_fg_bc)
    palette.setColor(QPalette.Button, color_bg_bc)
    palette.setColor(QPalette.ButtonText, color_fg_bc)
    palette.setColor(QPalette.Link, color_fg_bk)
    palette.setColor(QPalette.Highlight, color_fg_bk)
    palette.setColor(QPalette.HighlightedText, color_bg_bk)
    app.setPalette(palette)
    window = Window()
    window.show()
    app.exec_()
