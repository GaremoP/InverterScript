# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# -*- coding: utf-8 -*-
import os
import shutil
import time
import pandas as pd


def check_file_type(filepath_to_check):
    base_file_name = os.path.basename(filepath_to_check)
    if base_file_name.startswith('SEP'):
        return 'SEP'
    elif base_file_name.startswith('EP'):
        return 'EP'
    else:
        return False


def treat_sep_files(filepath_sep):
    df = pd.read_csv(filepath_sep, on_bad_lines='skip')
    df['Time'] = pd.to_datetime(df['Time'], dayfirst=True)
    df.set_index('Time', inplace=True)
    df = df.tz_localize('Europe/Madrid')
    df.reset_index(inplace=True)
    df['Time'] = df['Time'].dt.tz_localize(None)

    df.dropna(how='any', inplace=True)

    path = os.path.join(PATH_OUTPUT_FILES, os.path.basename(filepath_sep))
    df.to_csv(path, index=False)
    push_into_repo(path)
    return path


def treat_ep_files(filepath_ep):
    filename_original = os.path.basename(filepath_ep)
    df = pd.read_csv(filepath_ep, on_bad_lines='skip')

    df['Time'] = pd.to_datetime(df['Time'], dayfirst=True)
    df.set_index('Time', inplace=True)
    df = df.tz_localize('Europe/Madrid')
    df.reset_index(inplace=True)
    df['Time'] = df['Time'].dt.tz_localize(None)

    energy_cols = [col for col in df.columns if "(Wh)" in col]
    df.dropna(subset=energy_cols, how='all', inplace=True)

    path = os.path.join(PATH_OUTPUT_FILES, filename_original)
    df.to_csv(path, index=False)
    return path


def push_into_repo(filepath_to_push):
    new_data = pd.read_csv(filepath_to_push)
    database = pd.read_csv(PATH_REPO_INVERTER)

    overlapping_records = database[database['Time'].isin(new_data['Time'])]
    if overlapping_records.empty:
        database = pd.concat([database, new_data])
    else:
        database.set_index('Time', inplace=True)
        new_data.set_index('Time', inplace=True)
        # database = database.combine_first(dfs)
        database.update(new_data)
        database.reset_index(inplace=True)

    # Order the database by 'Datetime' in ascending order
    database.sort_values(by='Time', inplace=True)
    database.to_csv(PATH_REPO_INVERTER, index=False)


def cut_and_paste_file(source_path, destination_path):
    try:
        shutil.move(source_path, destination_path)
    except FileNotFoundError:
        print("Source file not found.")
    except PermissionError:
        print("Permission denied. Unable to cut and paste the file.")
    except Exception as e:
        print("An error occurred while cutting and pasting the file:", str(e))


def resample_24(filepath):
    df = pd.read_csv(filepath, parse_dates=['Time'], index_col='Time')
    df = df.resample('H').asfreq()
    # Create a date range from 1 AM of the first day to 12 AM of the next day
    start_date = df.index[0].replace(hour=1, minute=0, second=0)
    end_date = start_date + pd.DateOffset(days=1) - pd.DateOffset(hours=1)
    date_range = pd.date_range(start=start_date, end=end_date, freq='H')

    # Reindex the dataframe with the new date range
    df = df.reindex(date_range)
    df.index.name = 'Time'
    df = df.fillna(0)
    df.to_csv(filepath)


def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        print('File '+file_path+' removed correctly!')


def main(full_input_path):
    is_input_ok = check_file_type(full_input_path)
    if is_input_ok is False:
        delete_file(full_input_path)
        return
    elif is_input_ok == 'SEP':
        path_out = treat_sep_files(full_input_path)
        print(full_input_path + ' SEP was parsed')
    else:
        path_out = treat_ep_files(full_input_path)
        print(full_input_path + ' EP was parsed')
    # To save on raw data parsed the file as it came
    out_total = os.path.join(PATH_RAW_PARSED, os.path.basename(full_input_path))
    cut_and_paste_file(full_input_path, out_total)
    resample_24(path_out)
    push_into_repo(path_out)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    PATH_INPUT_FILES = r"C:\Users\pablo\Documents\ProgrammingProjects\PycharmProjects\InputData\Inverter"
    PATH_RAW_PARSED = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RawDataParsed\INVERTER"
    PATH_OUTPUT_FILES = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\DatosInversor_Gijon"
    PATH_REPO_INVERTER = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados\InverterData.csv"
    while True:
        for file in os.listdir(PATH_INPUT_FILES):
            if file.lower().endswith('.csv'):
                file_inverter = os.path.join(PATH_INPUT_FILES, file)
                main(file_inverter)
        time.sleep(10)
