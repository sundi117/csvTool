import re

import pandas as pd
import os
import time
from flask import Flask, request, Response, jsonify
from flask import render_template
# from flask_script import Manager
import requests
import json

"""
CSV文件综合处理工具

备注：
    1、os.path.isfile(testPath)  这个方法既可以确定传入路径是否为文件而非文件夹，也可以判断存不存在这个文件
"""

app = Flask(__name__)

# pandas内置属性
pd.set_option('display.max_columns', None)
# pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# 每一次执行readFileColumn 将会确定正确的 Encoding
encoding = 'utf-8'
colunmsTypes = pd.core.series.Series()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/readFileColumn', methods=['GET', 'POST'])
def readFileColumn():
    data = json.loads(request.form.get('data'))

    # print("data:" + str(data))
    from_path = data['from_path']
    # to_path = request.form.get('to_path')
    data = {}
    columnsList = []
    columns, statusCode = readFileColumnGo(str(from_path))

    if statusCode == 1000:
        for colunm in columns:
            columnsList.append(colunm)
            # print(colunm)
        columnsList.sort()
        data['columns'] = columnsList
        data['status'] = 'success'
    elif statusCode == 1001:
        data['status'] = 'fail'
        data['msg'] = '路径不存在，请检查路径。'
    elif statusCode == 1002:
        data['status'] = 'fail'
        data['msg'] = '文件不为csv格式。'
    elif statusCode == 1003:
        data['status'] = 'fail'
        data['msg'] = '你的csv文件有点问题，可能是编码不正确，请用notePad打开后转码成utf-8格式再次尝试。'
    elif statusCode == 1004:
        data['status'] = 'fail'
        data['msg'] = '你输入的文件路径下面一个csv文件都木有。'
    elif statusCode == 1005:
        data['status'] = 'fail'
        data['msg'] = '你输入的文件夹路径下面没有文件。'

    data['statusCode'] = statusCode
    # return Response(json.dumps(data), mimetype='application/json')
    # return jsonify(data)
    return json.dumps(data)


@app.route('/run', methods=['GET', 'POST'])
def run():
    data = json.loads(request.form.get('data'))
    from_path = data['from_path']
    to_path = data['to_path']
    to_one = data['to_one']
    conditions_putong = data['conditions']['putong']
    conditions_gaoji = data['conditions']['gaoji']

    print('from_path:%s' % from_path)
    print('to_path:%s' % to_path)
    print('conditions_putong:%s' % conditions_putong)
    print('conditions_gaoji:%s' % conditions_gaoji)

    data = {}
    if conditions_putong:
        condition_putong = transferConditions_putong(conditions_putong)
    else:
        condition_putong = None
    if conditions_gaoji:
        conditions_gaoji = transferConditions_gaoji(conditions_gaoji)
    else:
        conditions_gaoji = None

    statusCode, resultPath = deal_csv(from_path, to_path, condition_putong, conditions_gaoji, to_one)
    if statusCode == 1000:
        data['status'] = 'success'
        data['msg'] = '筛选成功！输出文件路径为：%s' % resultPath
    elif statusCode == 1001:
        data['status'] = 'fail'
        data['msg'] = '路径不存在，请检查路径。'
    elif statusCode == 1002:
        data['status'] = 'fail'
        data['msg'] = '文件不为csv格式。'
    elif statusCode == 1003:
        data['status'] = 'fail'
        data['msg'] = '你的csv文件有点问题，可能是编码不正确，请用notePad打开后转码成utf-8格式再次尝试。'
    elif statusCode == 1004:
        data['status'] = 'fail'
        data['msg'] = '你输入的文件路径下面一个csv文件都木有。'
    elif statusCode == 1005:
        data['status'] = 'fail'
        data['msg'] = '你输入的文件夹路径下面没有文件。'
    elif statusCode == 1006:
        data['status'] = 'fail'
        data['msg'] = '你的输出路径不存在。'

    data['statusCode'] = statusCode
    return json.dumps(data)


def transferConditions_putong(conditions):
    """
         处理conditions
         conditions:[{'columnSelected': 'ESN', 'operatorType': '包含', 'inputValue': 'esn1,esn2,esn3'},
                     {'columnSelected': 'PLMN', 'operatorType': '等于', 'inputValue': 'p1'},
                     {'columnSelected': 'eNodeBID', 'operatorType': '大于', 'inputValue': 'enode1'},
                     {'columnSelected': 'SINR', 'operatorType': '包含', 'inputValue': 't1,t2,t3'},
                     {'columnSelected': 'TotalConnectTime', 'operatorType': '等于', 'inputValue': '999'}]
        :return:
    """

    # finalCondition = r'''  df[(df['ESN'] =='YCQ7S19319001938' )]
    finalCondition = r'''  df[ '''

    for condition in conditions:
        conditionTemp = r"(df['%s'] '%s' '%s' ) "

        columnSelected = condition.get('columnSelected')
        operatorType = condition.get('operatorType')
        inputValue = condition.get('inputValue')

        if operatorType == 'in':
            sepsymbol = ','
            if str(inputValue).find(',') == -1:
                sepsymbol = '，'
            inputValueList = str(inputValue).split(sepsymbol)
            conditionTemp = r" ( "
            for index, iv in enumerate(inputValueList):
                # iv = iv.strip()   # 可以去掉了
                if (int(index) + 1) == len(inputValueList):
                    conditionTemp += r"( df['%s'] %s  %s )  )  " % (
                        columnSelected, '==', inputValue_dealer(columnSelected, iv))
                else:
                    conditionTemp += r"( df['%s'] %s  %s ) |  " % (
                        columnSelected, '==', inputValue_dealer(columnSelected, iv))
            print('conditionTemp:%s' % conditionTemp)

        elif operatorType == '介于...之间':
            sepsymbol = ','
            if str(inputValue).find(',') == -1:
                sepsymbol = '，'
            inputValueList = str(inputValue).split(sepsymbol)
            minTemp = inputValue_dealer(columnSelected, inputValueList[0])
            maxTemp = inputValue_dealer(columnSelected, inputValueList[1])
            minValue, maxValue = min(minTemp, maxTemp), max(minTemp, maxTemp)

            conditionTemp = r"( ( df['%s'] > %s ) & ( df['%s'] < %s ) )" % (
                columnSelected, minValue, columnSelected, maxValue)
            print('conditionTemp:%s' % conditionTemp)

        elif operatorType == '等于':
            conditionTemp = r"(df['%s'] %s  %s ) " % (
                columnSelected, '==', inputValue_dealer(columnSelected, inputValue))
            print('conditionTemp:%s' % conditionTemp)

        elif operatorType == '大于':
            conditionTemp = r"(df['%s'] %s  %s ) " % (
                columnSelected, '>', inputValue_dealer(columnSelected, inputValue))
            print('conditionTemp:%s' % conditionTemp)

        elif operatorType == '小于':
            conditionTemp = r"(df['%s'] %s  %s ) " % (
                columnSelected, '<', inputValue_dealer(columnSelected, inputValue))
            print('conditionTemp:%s' % conditionTemp)

        elif operatorType == '含有(模糊)':
            # ( df['Discount_rate'].str.contains(':')  )
            conditionTemp = r"(df['%s'].str.contains(  %s , regex=False ) ) " % (
                columnSelected, inputValue_dealer(columnSelected, inputValue))
            print('conditionTemp:%s' % conditionTemp)
        else:
            pass

        finalCondition += (conditionTemp + " & ")

    finalCondition = finalCondition[:-2] + " ]"
    print("finalCondition:%s" % finalCondition)
    return finalCondition


def transferConditions_gaoji(conditions):
    """
         处理conditions
         conditions:[{"columnSelected":"JBYY, JDDS1","operatorType":"删除列"},
                    {"columnSelected":"ZYJLX, ZZCS","operatorType":"去重"}]
        :return:
    """

    finalConditions = []
    for condition in conditions:
        inputValue = condition.get('columnSelected')
        operatorType = condition.get('operatorType')
        params = condition.get('params')
        if operatorType == '删除列':  # df.drop(['id'],axis=1,inplace=True)
            finalCondition = r'''  dfResult.drop([ '''
            sepsymbol = ','
            if str(inputValue).find(',') == -1:
                sepsymbol = '，'
            valueList = str(inputValue).split(sepsymbol)
            for index, iv in enumerate(valueList):
                if (int(index) + 1) == len(valueList):
                    finalCondition += r"'%s'" % (inputValue_dealer('', iv))
                else:
                    finalCondition += r"'%s'," % (inputValue_dealer('', iv))
            finalCondition += r''' ],axis=1,inplace=True )   '''
            print('finalCondition:%s' % finalCondition)
            finalConditions.append(finalCondition)
        elif operatorType == '增加列':
            finalCondition = r'''dfResult['%s']='%s' ''' % (
                inputValue_dealer('', inputValue), inputValue_dealer('', params))
            finalConditions.append(finalCondition)
        elif operatorType == '去重':
            finalCondition = r'''  dfResult.drop_duplicates(subset=[ '''
            sepsymbol = ','
            if str(inputValue).find(',') == -1:
                sepsymbol = '，'
            valueList = str(inputValue).split(sepsymbol)
            for index, iv in enumerate(valueList):
                if (int(index) + 1) == len(valueList):
                    finalCondition += r"'%s'" % (inputValue_dealer('', iv))
                else:
                    finalCondition += r"'%s'," % (inputValue_dealer('', iv))
            finalCondition += r''' ],keep='%s',inplace=True)   ''' % ('first' if params == '1' else 'last')
            print('finalCondition:%s' % finalCondition)
            finalConditions.append(finalCondition)

    print("finalConditions:%s" % finalConditions)
    return finalConditions


def readFileColumnGo(from_path_root):
    '''
        读取csv文件表头
    '''

    columns = []
    global encoding
    global colunmsTypes

    if not os.path.exists(from_path_root):
        print("路径不存在，请重新确认！")
        return columns, 1001

    # 如果传入的from_path_root是文件
    if os.path.isfile(from_path_root):
        (filepath, filenameAndextension) = os.path.split(from_path_root)
        (filename, extension) = os.path.splitext(filenameAndextension)
        if extension.lower() != '.csv':
            print("非CSV文件，请重新填写路径")
            return columns, 1002
        else:
            try:
                df = pd.read_csv(from_path_root, encoding='utf-8', error_bad_lines=False, sep=',', low_memory=False,
                                 index_col=False)
            except UnicodeDecodeError as e:
                df = pd.read_csv(from_path_root, encoding='gbk', error_bad_lines=False, sep=',', low_memory=False,
                                 index_col=False)
                encoding = 'gbk'
            except Exception as e:
                return columns, 1003

            columns = df.columns
            colunmsTypes = df.dtypes
            return columns, 1000

    # 如果传入的from_path_root是文件夹
    else:
        # 遍历出根路径下面的所有csv文件
        for root, dirs, files in os.walk(from_path_root):
            if not files:
                return columns, 1005
            for index, file in enumerate(files):
                (filename, extension) = os.path.splitext(file)
                if extension.lower() != '.csv':
                    if index + 1 == len(files):
                        return columns, 1004
                    continue
                else:
                    filepath = os.path.join(root, file)
                # print("子文件---", file)
                # print("文件路径：", filepath)
                print("正在处理 ", filepath, " ,请稍后...")
                try:
                    df = pd.read_csv(filepath, encoding='utf-8', error_bad_lines=False, sep=',', low_memory=False,
                                     index_col=False)
                except UnicodeDecodeError as e:
                    df = pd.read_csv(filepath, encoding='gbk', error_bad_lines=False, sep=',', low_memory=False,
                                     index_col=False)
                    encoding = 'gbk'
                except Exception as e:
                    return columns, 1003

                columns = df.columns
                colunmsTypes = df.dtypes
                return columns, 1000


def deal_csv(from_path_root, to_path, condition_putong, conditions_gaoji, to_one):
    '''
        处理csv文件
    '''

    if not os.path.exists(from_path_root):
        print("源路径不存在，请重新确认！")
        return 1001, ''

    if not os.path.exists(to_path):
        print("输出路径不存在，请重新确认！")
        return 1006, ''

    print('-' * 50 + "START" + '-' * 50)

    # 如果传入的from_path_root是文件
    if os.path.isfile(from_path_root):
        (filepath, filenameAndextension) = os.path.split(from_path_root)
        (filename, extension) = os.path.splitext(filenameAndextension)
        if extension.lower() != '.csv':
            print("非CSV文件，请重新填写路径")
            return 1002, ''
        else:
            print("正在处理csv文件 ", from_path_root, " ,请稍后...")
            #  这里有个巨坑！！！如果csv文件格式不规则，行尾会有分隔符，导致列体多出一列，和列名不对应，加上 index_col=False完美解决。
            try:
                df = pd.read_csv(from_path_root, encoding=encoding, error_bad_lines=False, sep=',', low_memory=False,
                                 index_col=False)
            except Exception as e:
                return 1003, ''

            # dfResult = df[eval(condition_putong)]
            # dfResult = df.drop_duplicates(subset=['TIME'], keep='first')['TIME']
            print('-' * 50 + "df" + '-' * 50)
            print(df.head())
            # print('-' * 50 + "dtypes" + '-' * 50)
            # print(colunmsTypes)

            # 如果有普通功能
            if condition_putong:
                dfResult = eval(condition_putong)
            else:
                dfResult = df

            # 如果有高级功能
            if conditions_gaoji:
                for condition_gaoji in conditions_gaoji:
                    # 注意：前方高能巨坑！！！！！！！！！！！！！！！
                    # 如果是这种赋值条件 dfResult['age']='32'，只能用exec的函数执行！！！
                    if re.findall(r"dfResult\[", condition_gaoji):
                        exec(condition_gaoji)
                    else:
                        eval(condition_gaoji)

            print('-' * 50 + "dfResult" + '-' * 50)
            print(dfResult.head())
            timestr = time.strftime('%Y%m%d_%H%M%S', time.localtime(time.time()))
            resultPath = to_path + r"\%s_result_%s.csv" % (filename, timestr)
            dfResult.to_csv(resultPath, index=False, encoding=encoding)
            print(filepath + "  处理完毕！")
            print("文件输出在：" + resultPath)
            return 1000, resultPath

    # 如果传入的from_path_root是文件夹
    else:
        dfAll = None
        runTimes = 0
        timestr = time.strftime('%Y%m%d_%H%M%S', time.localtime(time.time()))
        # 遍历出根路径下面的所有csv文件
        for root, dirs, files in os.walk(from_path_root):
            if not files:
                return 1005, ''

            for index, file in enumerate(files):
                (filename, extension) = os.path.splitext(file)
                if extension.lower() != '.csv':
                    if index + 1 == len(files):
                        return 1004, ''
                    continue
                else:
                    filepath = os.path.join(root, file)
                # print("子文件---", file)
                # print("文件路径：", filepath)
                print("正在处理 ", filepath, " ,请稍后...")
                try:
                    df = pd.read_csv(filepath, encoding=encoding, error_bad_lines=False, sep=',',
                                     low_memory=False,
                                     index_col=False)
                except Exception as e:
                    return 1003, ''
                print('-' * 50 + "df" + '-' * 50)
                print(df.head())
                # dfResult = df[eval(condition_putong)]
                # print('-' * 50 + "dtypes" + '-' * 50)
                # print(colunmsTypes)

                # 如果有普通功能
                if condition_putong:
                    dfResult = eval(condition_putong)
                else:
                    dfResult = df

                # 如果有高级功能
                if conditions_gaoji:
                    for condition_gaoji in conditions_gaoji:
                        # 注意：前方高能巨坑！！！！！！！！！！！！！！！
                        # 如果是这种赋值条件 dfResult['age']='32'，只能用exec的函数执行！！！
                        if re.findall(r"dfResult\[", condition_gaoji):
                            exec(condition_gaoji)
                        else:
                            eval(condition_gaoji)

                print('-' * 50 + "dfResult" + '-' * 50)
                print(dfResult.head())

                if to_one == 'true':
                    # 拼接多个dataFrame
                    if runTimes == 0:
                        dfAll = dfResult
                    else:
                        dfAll = dfAll.append(dfResult)
                    runTimes += 1
                elif to_one == 'false':
                    resultPath = to_path + r"\%s_result_%s.csv" % (filename, timestr)
                    dfResult.to_csv(resultPath, index=False, encoding=encoding)

        if to_one == 'true':
            resultPath = to_path + r"\result_all_%s.csv" % (timestr)
            # 注意！！！
            # 在to_one 模式下，如果条件中有高级功能的去重，多个文件分别去重合并后还需要一次总去重
            if conditions_gaoji:
                for condition_gaoji in conditions_gaoji:
                    if condition_gaoji.find('dfResult.drop_duplicates') != -1:  # 如果有去重功能
                        eval(condition_gaoji.replace('dfResult', 'dfAll'))

            dfAll.to_csv(resultPath, index=False, encoding=encoding)
        print(filepath + "  处理完毕！")
        print("文件输出在：" + resultPath)
        print('-' * 50 + "END" + '-' * 50)
        return 1000, (resultPath if (to_one == 'true') else to_path)


def inputValue_dealer(columnSelected, inputValue):
    # dateframe 中一般有这几种数据类型 float64  int64 object

    inputValue = str(inputValue).strip()

    if columnSelected == '':
        pass
    else:
        colunmsType = colunmsTypes.get(columnSelected)
        if colunmsType == 'object':
            inputValue = "'%s'" % inputValue
        elif colunmsType == 'int64':
            inputValue = int(inputValue)
        elif colunmsType == 'float64':
            inputValue = float(inputValue)

    return inputValue


def deal_exception(from_path_root, encoding):
    try:
        df = pd.read_csv(from_path_root, encoding=encoding, error_bad_lines=False, sep=',', low_memory=False,
                         index_col=False)
    except UnicodeDecodeError as e:
        df = pd.read_csv(from_path_root, encoding=encoding, error_bad_lines=False, sep=',', low_memory=False,
                         index_col=False)

    return df


if __name__ == '__main__':
    app.run('0.0.0.0', port=1234, debug=True)
