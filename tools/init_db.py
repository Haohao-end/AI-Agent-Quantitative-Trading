import os
import shutil
import sqlite3
import pandas as pd

# 这个数据库才是，项目测试过程中使用的
local_file = "../travel_new.sqlite"

# 创建一个备份文件，允许我们在测试的时候可以重新开始
backup_file = "../travel2.sqlite"


def update_dates():
    """
    更新数据库中的日期，使其与当前时间对齐。

    参数:
        file (str): 要更新的数据库文件路径。

    返回:
        str: 更新后的数据库文件路径。
    """
    # 使用备份文件覆盖现有文件，作为重置步骤
    shutil.copy(backup_file, local_file)  # 如果目标路径已经存在一个同名文件，shutil.copy 会覆盖该文件。

    conn = sqlite3.connect(local_file)
    # cursor = conn.cursor()

    # 获取所有表名
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn).name.tolist()
    tdf = {}

    # 读取每个表的数据
    for t in tables:
        tdf[t] = pd.read_sql(f"SELECT * from {t}", conn)

    # 找出示例时间（这里用flights表中的actual_departure的最大值）
    example_time = pd.to_datetime(tdf["flights"]["actual_departure"].replace("\\N", pd.NaT)).max()
    current_time = pd.to_datetime("now").tz_localize(example_time.tz)
    time_diff = current_time - example_time

    # 更新bookings表中的book_date
    tdf["bookings"]["book_date"] = (
            pd.to_datetime(tdf["bookings"]["book_date"].replace("\\N", pd.NaT), utc=True) + time_diff
    )

    # 需要更新的日期列
    datetime_columns = ["scheduled_departure", "scheduled_arrival", "actual_departure", "actual_arrival"]
    for column in datetime_columns:
        tdf["flights"][column] = (
                pd.to_datetime(tdf["flights"][column].replace("\\N", pd.NaT)) + time_diff
        )

    # 将更新后的数据写回数据库
    for table_name, df in tdf.items():
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        del df  # 清理内存
    del tdf  # 清理内存

    conn.commit()
    conn.close()

    return local_file


if __name__ == '__main__':

    # 执行日期更新操作
    db = update_dates()