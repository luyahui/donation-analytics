import sys
import os
import math
import datetime
import pandas as pd


# read the data
def read_data(input_file):
    lines = []
    with open(input_file, 'r') as f:
        line = f.readline()
        while line:
            lines.append(line.split('|'))
            line = f.readline()
    f.close()
    return lines


# read the number of percentile
def read_percentile(input_file):
    with open(input_file, 'r') as f:
        line = f.readline()
        return int(line)


# validate name, zip code or date by string
def validate_record(original_str, type):
    if type == 'name':
        try:
            return original_str.replace(',', ' ').replace(' ', '').isalpha()
        except ValueError:
            return False

    if type == 'zip':
        return original_str.isnumeric() and len(original_str) >= 5

    if type == 'date':
        try:
            datetime.datetime.strptime(original_str, '%m%d%Y')
            return True
        except ValueError:
            return False


def process_record(lines):
    # construct a dataframe
    header = ['CMTE_ID', 'AMNDT_IND', 'RPT_TP', 'TRANSACTION_PGI', 'IMAGE_NUM', 'TRANSACTION_TP',
              'ENTITY_TP', 'NAME', 'CITY', 'STATE', 'ZIP_CODE', 'EMPLOYER', 'OCCUPATION', 'TRANSACTION_DT',
              'TRANSACTION_AMT', 'OTHER_ID', 'TRAN_ID', 'FILE_NUM', 'MEMO_CD', 'MEMO_TEXT', 'SUB_ID']
    df = pd.DataFrame(data=lines, columns=header)
    df = df[['CMTE_ID', 'NAME', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']]

    # drop the unqualified data, and format the qualified ones
    drop_list = []
    formatted_amt = []
    formatted_zip = []
    formatted_dt = []

    for i in list(df.index.values):
        if df.loc[i].CMTE_ID == '' \
                or validate_record(df.loc[i].NAME, 'name') == False \
                or validate_record(df.loc[i].ZIP_CODE, 'zip') == False \
                or validate_record(df.loc[i].TRANSACTION_DT, 'date') == False \
                or df.loc[i].TRANSACTION_AMT == '' \
                or df.loc[i].OTHER_ID != '':
            drop_list.append(i)
        else:
            formatted_amt.append(int(df.loc[i].TRANSACTION_AMT))
            formatted_zip.append(df.loc[i].ZIP_CODE[:5])
            formatted_dt.append(df.loc[i].TRANSACTION_DT[-4:])

    df.drop(drop_list, inplace=True)

    df['ZIP_CODE'] = formatted_zip
    df['TRANSACTION_DT'] = formatted_dt
    df['TRANSACTION_AMT'] = formatted_amt
    # print(df)

    return df


# print into output file
def emit(cmte_id, zip_code, transaction_dt, percentile, total_amount, total_number, output_path):
    # print([cmte_id, zip_code, transaction_dt, percentile, total_amount, total_number])
    with open(output_path, 'a') as f:
        f.write(
            str(cmte_id) + '|' + str(zip_code) + '|' + str(transaction_dt) + '|' + str(percentile) + '|' + str(
                total_amount) + '|' + str(total_number) + '\n')
    f.close()


def compute(results, cmte_id, zip_code, transaction_dt, percentile, output_path):
    if cmte_id + zip_code + transaction_dt not in results:
        return

    transactions = results[cmte_id + zip_code + transaction_dt]
    transactions.sort()
    emit(cmte_id, zip_code, transaction_dt,
         transactions[math.floor(len(transactions) * percentile / 100)], sum(transactions), len(transactions),
         output_path)


def analyse(df, percentile, output_path):
    df = df.sort_values(by=['TRANSACTION_DT'])

    donors = []
    results = {}

    for i in df.index.values:
        s = df.loc[i]
        if s.NAME not in donors:
            donors.append(s.NAME)
            continue

        if s.CMTE_ID + s.ZIP_CODE + s.TRANSACTION_DT in results:
            results[s.CMTE_ID + s.ZIP_CODE + s.TRANSACTION_DT].append(s.TRANSACTION_AMT)
        else:
            results[s.CMTE_ID + s.ZIP_CODE + s.TRANSACTION_DT] = [s.TRANSACTION_AMT]
        compute(results, s.CMTE_ID, s.ZIP_CODE, s.TRANSACTION_DT, percentile, output_path)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        exit()

    itcont_path, percentile_path, output_path = sys.argv[1], sys.argv[2], sys.argv[3]
    df = process_record(read_data(itcont_path))
    percentile = read_percentile(percentile_path)
    if os.path.exists(output_path):
        os.remove(output_path)
    analyse(df, percentile, output_path)
