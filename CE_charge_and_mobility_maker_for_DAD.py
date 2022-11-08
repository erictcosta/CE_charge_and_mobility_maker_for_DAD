import matplotlib.pyplot as plt
import pandas as pd
import glob
import os
from collections import defaultdict

#######   Inform path and files infos ################
WHERE_IS_THE_DATAS = "C:\\Users"
DATAS_FILES_NAME = "teste.txt"
SAVE_DATAS_ON_THIS_FOLDER = WHERE_IS_THE_DATAS + "\\CE_datas"
DECIMAL = "." #decimal point to output data"
##########################################

############  CE parameters ##############
LDUV = 13.331  # distance of UV detector in cm
LT = 50  # length of capillary in cm
I = 23.32  # Current applied on the BGE in uA
V = 24.016  # voltage applied on the BGE in kV
###########################################



keys_names = {"CE1_A": "Voltage(kV)",
              "CE1_B": "Current(uA)",
              "CE1_C": "Power(W)",
              "CE1_D": "Leak_Current(uA)",
              "CE1_E": "Internal_Pressure",
              "CE1_F": "Replenish_Pressure",
              "CE1_G": "Vacuum",
              "CE1_H": "Injction_Presure",
              "CE1_I": "External_Pressure",
              "CE1_J": "Tray_temp(oC)",
              "CE1_K": "Cassette_Temp(oC)",
              "CEDAD1_I": "Lamp_Voltage"}


PROCESSSED_DATA = "processed_data" + os.sep

FILE_NAMES = ""
PATH = "."

MOBILITY_COLUMN = "Mobility(10-5*cm^2/V/s)"
CHARGE_COLUMN = "Charge(uC)"
TIME_COLUMN = "Time(min)"


def ordering_columns(columns):
    order = ["#"] * 20
    for i in columns:
        if "Time" in i:
            order.insert(0, i)
            continue
        if "Charge" in i:
            order.insert(1, i)
            continue
        if "Mobility" in i:
            order.insert(2, i)
            continue
        if "DAD" in i:
            order.insert(3, i)
            continue
        if "Current" in i:
            order.insert(4, i)
            continue
        else:
            order.append(i)
    return [i for i in order if i != "#"]


def create_charge_column(data, time_col, current_col):
    data_charge = data[time_col].diff() * data[current_col] * 60
    data[CHARGE_COLUMN] = data_charge.cumsum()
    return data


def create_mobility_column(data, q, LD, LT, I, V):
    # k = LD * LT * I * 1E-6 / V / 1E3 / 1E-6
    k = LD * LT * I / V  #10-5*cm^2/V/s
    data[MOBILITY_COLUMN] = k / data[q]
    return data


def my_interpolator(keep_data, interpole_data, cols=("Time(min)", "Signal")):
    commom_col = cols[0]
    detector_col = cols[1]
    df = pd.concat([keep_data, interpole_data])
    df.sort_values(by=[commom_col], inplace=True)
    df = df.groupby(commom_col).mean()
    df_detectors = df.iloc[:, :keep_data.shape[1]]
    df = df.interpolate(method="index")
    df.reset_index(inplace=True)
    df_detectors.reset_index(inplace=True)
    return df[~df_detectors[detector_col].isna()]


def merger_files_and_interpole_files(file_names):
    print("========= merge all signal in one file em interpolate them ============")
    experiments_foldes = defaultdict(list)

    for i in file_names:
        if not "inter.dat" in i:
            key = i.split(os.sep)[-2]
            experiments_foldes[key].append(i)

    for k, i in experiments_foldes.items():
        #print(k)
        #[print("\t\t\t" + j) for j in i]
        df = pd.DataFrame()
        signals = pd.DataFrame()
        for j in i:
            if not "CEDAD" in j:
                data = pd.read_table(j)
                if signals.shape[1] > 0:
                    signals = pd.merge(signals, data, on=TIME_COLUMN)
                else:
                    signals = data

        for j in i:
            if "CEDAD" in j:
                data = pd.read_table(j)
                inter = my_interpolator(data, signals, cols=(TIME_COLUMN, data.columns[1]))
                inter = create_charge_column(inter, TIME_COLUMN, "Current(uA)")
                inter = create_mobility_column(inter, CHARGE_COLUMN, LDUV, LT, I, V)
                path = os.path.dirname(j)
                inter.fillna(0)
                order = ordering_columns(inter.columns)
                inter = inter[order]
                inter.to_csv(os.path.join(path, j[:-4] + "_inter.dat"), sep="\t", index=False, decimal=DECIMAL)
                print(os.path.join(path, j[:-4] + "_inter.dat"))
    return df


def plot_all_inter(file_name, data, column_name):
    plt.cla()
    plt.clf()
    plt.figure(1)
    plt.style.use('seaborn')
    plt.plot(data.iloc[:, 0], data.iloc[:, 1])
    plt.ylabel(data.columns[1])
    plt.title(data.columns[0])
    path = os.path.dirname(file_name)
    basename = os.path.basename(file_name)
    name = f"{column_name}_{data.columns[1]}_{basename[:-4]}.png"
    plt.savefig(os.path.join(path, name))


def simple_plot(file_name, data):
    plt.cla()
    plt.clf()
    plt.figure(1)
    plt.style.use('seaborn')
    for i in range(0, data.shape[1], 2):
        plt.plot(data.iloc[:, i], data.iloc[:,i+1])
    plt.ylabel(data.columns[1])
    plt.title(data.columns[0])
    name = f"{str(file_name[:-4])}_{data.columns[1]}_{data.columns[0]}.png"
    #print(name)
    plt.savefig(name)
    return file_name


def get_datas(file_name):
    with open(file_name) as f:
        lines = f.readlines()
    return lines


def get_experiments_names(data):
    experiments = []
    for line in data:
        if "CE1 - B:Current" in line:
            experiments.append(line[18:-2].split(".d")[0])
    [print(f'experiements {i}') for i in experiments]
    return experiments


def extract_agilent_files(data):
    file_openned = False
    count_CE = 0
    experiments = get_experiments_names(data)
    pass_next = False
    try:
        os.mkdir(SAVE_DATAS_ON_THIS_FOLDER)
        folder = SAVE_DATAS_ON_THIS_FOLDER
    except OSError as error:
        print(error)
        folder = ""
    for line in data:
        if pass_next:
            pass_next = False
            continue
        if "***NO DATA POINTS***" in line or "***ZERO ABUNDANCE***" in line:
            pass_next = True
            continue
        if '#"CE' in line:
            count_CE += 1
            if file_openned:
               f.close()
            f_name = line[2:]
            for i in ': ':
                f_name = f_name.replace(i, "_")
            f_name = f_name.replace('.d"\n', ".dat")
            f_name = f_name.replace('_-_', "_")
            f_name = f_name.replace("__", "_")
            folder_name = ""
            for i in experiments:
                if i in line:
                    folder_name = i.replace('_-_', "_")
                    folder_name = folder_name.replace(":", "_")
                    folder_name = folder_name.replace(" ", "_")
                    folder_name = folder_name.replace("__", "_")
                    folder_name = folder_name.replace("__", "_")
            try:
                os.mkdir(os.path.join(SAVE_DATAS_ON_THIS_FOLDER, folder_name))
            except OSError as error:
                pass

            if folder:
                folder_name = os.path.join(folder, folder_name)
            f = open(os.path.join(SAVE_DATAS_ON_THIS_FOLDER, folder_name, f_name), 'w')
            file_openned = True
        else:
            if file_openned and line != "":
                f.write(line)


def plot_inter_data(file_names):
    for i in file_names:
        datas = pd.read_table(i)
        for j in datas.columns:
            if "DAD" in j:
                signal = j
                plot_all_inter(i, datas[[TIME_COLUMN, signal]], "time")
                if CHARGE_COLUMN in datas.columns:
                    plot_all_inter(i, datas[[CHARGE_COLUMN, signal]], "Charge")
                if MOBILITY_COLUMN in datas.columns:
                    mob = datas[[MOBILITY_COLUMN, signal]]
                    mob = mob[mob[MOBILITY_COLUMN] > 0.004]
                    mob = mob[mob[MOBILITY_COLUMN] < 100]
                    plot_all_inter(i, mob, "Mobility")


def rename_columns(file_names):
    for i in file_names:
        df = pd.read_table(i, header=0)
        df.drop(df.columns[0], axis=1, inplace=True)
        time = df.columns[0]
        signal_name = df.columns[1]
        if "Minutes" in df.columns[0]:
            time = TIME_COLUMN
        base_name = os.path.basename(i)
        signal = base_name.split("_")
        if "CE1" == signal[0]:
            key = signal[0] + "_" + signal[1]
            signal_name = keys_names[key]
        if "CEDAD" in signal[0]:
            signal_name = "_".join([signal[0], signal[1], signal[2], signal[3]])
        df.columns = [time, signal_name]
        df.to_csv(i, sep="\t", index=False)


def plot_all_datas(file_names):
    for i in file_names:
        df = pd.read_table(i, header=0)
        simple_plot(i, df)


def extract_agilent_data(file_names):
    print(f"{file_names=}")
    for file_name in file_names:
        data = get_datas(file_name)
        print(f"{len(data)=}")
        extract_agilent_files(data)


def get_files_names(path, wildcard, recursive=False, mode="command_line", not_wildcard=""):
    if mode == "command_line":
        if recursive:
            files = glob.glob(os.path.join(path, "**", wildcard), recursive=recursive)
        else:
            files = glob.glob(os.path.join(path, wildcard))
    if not_wildcard != "":
        for i in files:
            if not_wildcard in i:
                files.remove(i)
    return files


if __name__ == '__main__':
    #### Extract Agilent data to multiples files ####
    file_names = get_files_names(path=WHERE_IS_THE_DATAS, wildcard=DATAS_FILES_NAME)
    extract_agilent_data(file_names)

    #### convert agilent's columns name to human columns name  ####
    file_names = get_files_names(path=SAVE_DATAS_ON_THIS_FOLDER, wildcard="*.dat", not_wildcard="_inter.dat", recursive=True)
    rename_columns(file_names)

    #### plot all agilent datas  ####
    plot_all_datas(file_names)

    #### create charge and mobility datas  ####
    merger_files_and_interpole_files(file_names)

    #### plot time, charge and mobility datas  ####
    file_names = get_files_names(path=SAVE_DATAS_ON_THIS_FOLDER, wildcard="*_inter.dat", recursive=True)
    plot_inter_data(file_names)
