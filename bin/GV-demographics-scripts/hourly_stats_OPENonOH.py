import glob
import os
import pandas as pd
from pandasgui import show

def agg_per_pm_id(var="hour"):
    """
    This function creates a csv file with hourly statistics for the OPENonOH_NS and OPENonOH_AAPS_Uploader datasets.
    Originally. Now it is generalized to DoW-, month-, day-aggregations.
    """
    # create output directory, if it does not exist
    os.makedirs(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/results/", f"{var}ly_OPENonOH"), exist_ok=True)

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
        
        if var == "hour":
            df3["hour"] = df3["dateString2"].dt.hour
        elif var == "month":
            df3["month"] = df3["dateString2"].dt.month
        elif var == "day":
            df3["day"] = df3["dateString2"].dt.day
        elif var == "DoW":
            df3[var] = df3["dateString2"].dt.day_of_week
        else:
            raise(f"var must be one of 'hour', 'month', 'day', 'DoW', it was: {var}")
        print(df3.head())
        df3.drop(["dateString", "dateString2", "noise", "date"], axis=1, inplace=True)
        df4 = df3.groupby([var], as_index=False).agg(['mean', 'std', 'count', 'min', 'max'])
        columns = []
        for col in df4.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df4.columns = columns

        df4["pm_id"] = pm_id
        df4["gender"] = pm_id_gender_map[pm_id]
        df4.reset_index(inplace=True)
        df4.to_csv(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/results/", f"{var}ly_OPENonOH", f"OPENonOH_NS_per_{var}_{pm_id:08d}.csv"))       


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


        if var == "hour":
            df3["hour"] = df3["dateString2"].dt.hour
        elif var == "month":
            df3["month"] = df3["dateString2"].dt.month
        elif var == "day":
            df3["day"] = df3["dateString2"].dt.day
        elif var == "DoW":
            df3[var] = df3["dateString2"].dt.day_of_week
        else:
            raise(f"var must be one of 'hour', 'month', 'day', 'DoW', it was: {var}")
        # df3["hour"] = df3["dateString2"].dt.hour
        print(df3.head())
        df3.drop(["dateString", "dateString2", "noise", "date"], axis=1, inplace=True)
        df4 = df3.groupby([var], as_index=False).agg(['mean', 'std', 'count', 'min', 'max'])
        columns = []
        for col in df4.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df4.columns = columns

        df4["pm_id"] = pm_id
        df4["gender"] = pm_id_gender_map[pm_id]
        df4.reset_index(inplace=True)
        df4.to_csv(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/results/", f"{var}ly_OPENonOH", f"OPENonOH_NS_per_{var}_{pm_id:08d}.csv"))


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

        if var == "hour":
            df3["hour"] = df3["dateString2"].dt.hour
        elif var == "month":
            df3["month"] = df3["dateString2"].dt.month
        elif var == "day":
            df3["day"] = df3["dateString2"].dt.day
        elif var == "DoW":
            df3[var] = df3["dateString2"].dt.day_of_week
        else:
            raise(f"var must be one of 'hour', 'month', 'day', 'DoW', it was: {var}")
        print(df3.head())
        df3.drop(["dateString", "dateString2", "noise", "date"], axis=1, inplace=True)
        df4 = df3.groupby([var], as_index=False).agg(['mean', 'std', 'count', 'min', 'max'])
        columns = []
        for col in df4.columns:
            columns.append(f"{col[0]}_{col[1]}")
        df4.columns = columns

        df4["pm_id"] = pm_id
        df4["gender"] = pm_id_gender_map[pm_id]
        df4.reset_index(inplace=True)
        df4.to_csv(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/results/", f"{var}ly_OPENonOH", f"OPENonOH_Both_per_{var}_{pm_id:08d}.csv"))

def collect_into_one_file(dir_ : str, outfilename : str, var : str = "hour"):
    """
    Two output files are created: one for Female, one for Male.
    Outfilename is without extension, since the Female, Male suffix still has to be added

    Jan 2022: The gender association above is not correct, but the OPENonOH complete_patient_statistics.csv has been updated in the mean time and contains the correct pm_id to gender mapping.
    Therefore the gender is taken from the OPENonOH complete_patient_statistics.csv file.
    """
    filenames = os.listdir(dir_)
    filenames = [f for f in filenames if f.startswith("OPENonOH") and f"per_{var}" in f and f.endswith(".csv")]
    df = []
    for filename in filenames:
        df_temp = pd.read_csv(os.path.join(dir_, filename), header=0, index_col=0)
        df.append(df_temp)
    df2 = pd.concat(df)    
    #df2.set_index("hour", inplace=True)
    df2.rename(columns={'gender': 'gender_legacy'}, inplace=True)
    df2["pm_id"] = df2["pm_id"].astype(int)

    df_complete_stats = pd.read_excel(os.path.join("/home/reinhold/Daten/Paper_Datasets_Nov2022/", r'OPENonOH complete_patient_statistics.xlsx'))
    df_complete_stats = df_complete_stats[['id', 'gender']]
    # change id to int
    df_complete_stats["id"] = df_complete_stats["id"].astype(int)
    
    df2 = df2.merge(df_complete_stats, left_on="pm_id", right_on="id", how="left")

    df_male = df2[df2["gender"]=="Male"]
    df_female = df2[df2["gender"]=="Female"]

    df_male.to_csv(os.path.join(dir_, outfilename + "_Male" + ".csv"))
    df_female.to_csv(os.path.join(dir_, outfilename + "_Female" + ".csv"))

def test():
    dir_ = "/home/reinhold/Daten/dana_processing/OPENonOH_NS_Data/"
    df = pd.read_csv(os.path.join(dir_, "example_dates2.csv"), header=0,  index_col=0)
    df["dateString2"] = pd.to_datetime(df["dateString"], infer_datetime_format=True, utc=True)
    df.info()
    print(df.head())
    show(df)

def test2():
    dir_ = "/home/reinhold/Daten/Paper_Datasets_Nov2022/results/hourly_OPENonOH"
    df_Male = pd.read_csv(os.path.join(dir_, "OPENonOH_hourly_Male.csv"), header=0, index_col=0)
    print(df_Male[df_Male["hour"]==0].count())
    df_Female = pd.read_csv(os.path.join(dir_, "OPENonOH_hourly_Female.csv"), header=0, index_col=0)
    print(df_Female[df_Female["hour"]==0].count())

def main(var : str = "hour"):
    agg_per_pm_id(var)
    collect_into_one_file(f"/home/reinhold/Daten/Paper_Datasets_Nov2022/results/{var}ly_OPENonOH", f"OPENonOH_{var}ly", var)

if __name__ == '__main__':
    var = "day"
    main(var)

    #test2()