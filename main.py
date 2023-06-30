# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
# -*- coding: utf-8 -*-
import os
import shutil
import time
import pandas as pd


def treat_ep_files(filepath_ep, tracker):
    df = pd.read_csv(filepath_ep, on_bad_lines='skip')
    df['Time'] = pd.to_datetime(df['Time'], dayfirst=True)
    df.set_index('Time', inplace=True)
    df = df.tz_localize('Europe/Madrid')
    df.reset_index(inplace=True)
    df['Time'] = df['Time'].dt.tz_localize(None)

    energy_cols = [col for col in df.columns if "(Wh)" in col]
    df.dropna(subset=energy_cols, how='all', inplace=True)

    path = os.path.join(path_output_files[tracker], os.path.basename(filepath_ep))
    df.to_csv(path, index=False)
    return path


def push_into_repo(filepath_to_push, tracker):
    new_data = pd.read_csv(filepath_to_push)
    database = pd.read_csv(paths_repos[tracker])

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
    database.to_csv(paths_repos[tracker], index=False)


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


def main(full_input_path, tracker_num):

    path_out = treat_ep_files(full_input_path, tracker_num)
    # To save on raw data parsed the file as it came
    out_total = os.path.join(paths_raw_parsed[tracker_num], os.path.basename(full_input_path))

    cut_and_paste_file(full_input_path, out_total)
    resample_24(path_out)
    push_into_repo(path_out, tracker_num)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    PATH_INPUT_FILES_T1 = r"C:\Users\pablo\Documents\ProgrammingProjects\PycharmProjects\InputData\InverterTracker1"
    PATH_INPUT_FILES_T2 = r"C:\Users\pablo\Documents\ProgrammingProjects\PycharmProjects\InputData\InverterTracker2"

    PATH_RAW_PARSED_T1 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RawDataParsed\INVERTER_T1"
    PATH_RAW_PARSED_T2 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\RawDataParsed\INVERTER_T2"
    paths_raw_parsed = ["", PATH_RAW_PARSED_T1, PATH_RAW_PARSED_T2]

    PATH_OUTPUT_FILES_T1 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados" \
                           r"\DatosInversor_T1_Gijon"
    PATH_OUTPUT_FILES_T2 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados" \
                           r"\DatosInversor_T2_Gijon"
    path_output_files = ["", PATH_OUTPUT_FILES_T1, PATH_OUTPUT_FILES_T2]

    PATH_REPO_INVERTER_T1 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados" \
                            r"\InverterDataT1.csv"
    PATH_REPO_INVERTER_T2 = r"C:\Users\pablo\Documents\Teleco2018-\5CURSO\TFG\TratarDatos\DatosTratados" \
                            r"\InverterDataT2.csv"
    paths_repos = ["", PATH_REPO_INVERTER_T1, PATH_REPO_INVERTER_T2]
    while True:
        # Check the first path for CSV files
        for file in os.listdir(PATH_INPUT_FILES_T1):
            if file.lower().endswith('.csv'):
                file_inverter = os.path.join(PATH_INPUT_FILES_T1, file)
                main(file_inverter, 1)

        # Check the second path for CSV files
        for file in os.listdir(PATH_INPUT_FILES_T2):
            if file.lower().endswith('.csv'):
                file_inverter = os.path.join(PATH_INPUT_FILES_T2, file)
                main(file_inverter, 2)
        time.sleep(10)
