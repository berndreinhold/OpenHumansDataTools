import glob
import os
import pandas as pd
from pandasgui import show

def main():
    base_dir_ = "/home/reinhold/Daten/dana_processing/"
    dir_ = [base_dir_ + "OPENonOH_NS_Data/", base_dir_ + "OPENonOH_AAPS_Uploader_Data/"]
    subdir_ = "per_measurement_csv/"
    df = []
    df.append(pd.read_csv(os.path.join(dir_[0], "OPENonOH_NS_per_day.csv"), header=0,  index_col=0, parse_dates=["date"], infer_datetime_format=True))
    df.append(pd.read_csv(os.path.join(dir_[1], "OPENonOH_AAPS_Uploader_per_day.csv"), header=0,  index_col=0, parse_dates=["date"], infer_datetime_format=True))
    for d in df:
        d.info()
        print(d.head())

    # for the final list of selected project member ids and gender
    # structured in a dictionary
    df_complete_stats = pd.read_excel(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/", r'OPENonOH complete_patient_statistics.xlsx'))
    df_pm_id_gender = df_complete_stats[["id", "gender"]]
    df_pm_id_gender.info()
    print(df_pm_id_gender)
    df_pm_id_gender.set_index("id", inplace=True)
    pm_id_gender_map = df_pm_id_gender.to_dict()['gender']

    # pm_ids are integers
    pm_ids = [df[0]["pm_id"].unique().tolist(), df[1]["pm_id"].unique().tolist()]

    pm_id_sets = []
    pm_id_sets.append((set(pm_ids[0]) - set(pm_ids[1])) & set(pm_id_gender_map.keys()))
    pm_id_sets.append((set(pm_ids[1]) - set(pm_ids[0])) & set(pm_id_gender_map.keys()))
    pm_id_sets.append(set(pm_ids[0]) & set(pm_ids[1]) & set(pm_id_gender_map.keys()))


    # loop through pm_ids that are in the OPENonOH_NS dataset only
    for pm_id in sorted(list(pm_id_sets[0])):
        # get all filenames for pm_id
        filenames = df[0][df[0]["pm_id"] == pm_id]["filename"].unique()

        df2 = []
        for filename in filenames:
            print(filename)
            #df_temp = pd.read_csv(os.path.join(dir_, subdir_, filename), header=0, index_col=0, parse_dates=["dateString"], infer_datetime_format=True)
            df_temp = pd.read_csv(os.path.join(dir_[0], subdir_, filename), header=0, index_col=0)
            df_temp["dateString2"] = pd.to_datetime(df_temp["dateString"], infer_datetime_format=True, utc=True)
            df2.append(df_temp)
        df3 = pd.concat(df2)
        df3["hour"] = df3["dateString2"].dt.hour
        print(df3.head())
        df3.drop(["dateString", "dateString2", "noise", "date"], axis=1, inplace=True)
        df4 = df3.groupby(["hour"], as_index=False).agg(['mean', 'std', 'count', 'min', 'max'])
        columns = []
        for col in df4.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df4.columns = columns

        df4["pm_id"] = pm_id
        df4["gender"] = pm_id_gender_map[pm_id]
        df4.reset_index(inplace=True)
        df4.to_csv(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/results/", "hourly_OPENonOH", f"OPENonOH_NS_per_hour_{pm_id:08d}.csv"))       


    # loop through pm_ids that are AAPS_Uploader only
    for pm_id in sorted(list(pm_id_sets[1])):
        # get all filenames for pm_id
        filenames = df[1][df[1]["pm_id"] == pm_id]["filename"].unique()

        df2 = []
        for filename in filenames:
            print(filename)
            #df_temp = pd.read_csv(os.path.join(dir_, subdir_, filename), header=0, index_col=0, parse_dates=["dateString"], infer_datetime_format=True)
            df_temp = pd.read_csv(os.path.join(dir_[1], subdir_, filename), header=0, index_col=0)
            df_temp["dateString2"] = pd.to_datetime(df_temp["dateString"], infer_datetime_format=True, utc=True)
            df2.append(df_temp)
        df3 = pd.concat(df2)
        df3["hour"] = df3["dateString2"].dt.hour
        print(df3.head())
        df3.drop(["dateString", "dateString2", "noise", "date"], axis=1, inplace=True)
        df4 = df3.groupby(["hour"], as_index=False).agg(['mean', 'std', 'count', 'min', 'max'])
        columns = []
        for col in df4.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df4.columns = columns

        df4["pm_id"] = pm_id
        df4["gender"] = pm_id_gender_map[pm_id]
        df4.reset_index(inplace=True)
        df4.to_csv(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/results/", "hourly_OPENonOH", f"OPENonOH_NS_per_hour_{pm_id:08d}.csv"))


    # loop through pm_ids which occur in both datasets
    for pm_id in sorted(list(pm_id_sets[2])):
        # get all filenames for pm_id
        filenames = [] 
        filenames.append(df[0][df[0]["pm_id"] == pm_id]["filename"].unique())
        filenames.append(df[1][df[1]["pm_id"] == pm_id]["filename"].unique())

        df2 = []
        for filename in filenames[0]:
            print(filename)
            #df_temp = pd.read_csv(os.path.join(dir_, subdir_, filename), header=0, index_col=0, parse_dates=["dateString"], infer_datetime_format=True)
            df_temp = pd.read_csv(os.path.join(dir_[0], subdir_, filename), header=0, index_col=0)
            df_temp["dateString2"] = pd.to_datetime(df_temp["dateString"], infer_datetime_format=True, utc=True)
            df2.append(df_temp)

        for filename in filenames[1]:
            print(filename)
            #df_temp = pd.read_csv(os.path.join(dir_, subdir_, filename), header=0, index_col=0, parse_dates=["dateString"], infer_datetime_format=True)
            df_temp = pd.read_csv(os.path.join(dir_[1], subdir_, filename), header=0, index_col=0)
            df_temp["dateString2"] = pd.to_datetime(df_temp["dateString"], infer_datetime_format=True, utc=True)
            df2.append(df_temp)

        df3 = pd.concat(df2)
        df3["hour"] = df3["dateString2"].dt.hour
        print(df3.head())
        df3.drop(["dateString", "dateString2", "noise", "date"], axis=1, inplace=True)
        df4 = df3.groupby(["hour"], as_index=False).agg(['mean', 'std', 'count', 'min', 'max'])
        columns = []
        for col in df4.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df4.columns = columns

        df4["pm_id"] = pm_id
        df4["gender"] = pm_id_gender_map[pm_id]
        df4.reset_index(inplace=True)
        df4.to_csv(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/results/", "hourly_OPENonOH", f"OPENonOH_Both_per_hour_{pm_id:08d}.csv"))




def test():
    dir_ = "/home/reinhold/Daten/dana_processing/OPENonOH_NS_Data/"
    df = pd.read_csv(os.path.join(dir_, "example_dates2.csv"), header=0,  index_col=0)
    df["dateString2"] = pd.to_datetime(df["dateString"], infer_datetime_format=True, utc=True)
    df.info()
    print(df.head())
    show(df)

if __name__ == '__main__':
    main()
    #test()