import glob
import os
import pandas as pd
from pandasgui import show

def main():
    base_dir_ = "/home/reinhold/Daten/dana_processing/"
    dir_ = [base_dir_ + "OPENonOH_NS_Data/", base_dir_ + "OPENonOH_AAPS_Uploader_Data/"]
    subdir_ = "per_measurement_csv/"
    df = pd.read_csv(os.path.join(dir_, "OPENonOH_NS_per_day.csv"), header=0,  index_col=0, parse_dates=["date"], infer_datetime_format=True)
    df.info()
    print(df.head())

    out_data = []
    # loop through pm_ids
    for pm_id in df["pm_id"].unique():
        # get all filenames for pm_id
        filenames = df[df["pm_id"] == pm_id]["filename"].unique()

        df2 = []
        for filename in filenames:
            print(filename)
            #df_temp = pd.read_csv(os.path.join(dir_, subdir_, filename), header=0, index_col=0, parse_dates=["dateString"], infer_datetime_format=True)
            df_temp = pd.read_csv(os.path.join(dir_, subdir_, filename), header=0, index_col=0)
            df_temp["dateString2"] = pd.to_datetime(df_temp["dateString"], infer_datetime_format=True, utc=True)
            df2.append(df_temp)
        df3 = pd.concat(df2)
        df3["hour"] = df3["dateString2"].dt.hour
        print(df3.head())
        df3.drop(["dateString", "dateString2", "noise", "date"], axis=1, inplace=True)
        df3.groupby(["hour"], as_index=False).agg(['mean', 'std', 'count', 'min', 'max']).to_csv(os.path.join(dir_, "per_hour", "OPENonOH_NS_per_hour_{}.csv".format(pm_id)))       


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