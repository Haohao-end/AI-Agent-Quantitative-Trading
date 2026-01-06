from sqlite3 import connect, Cursor
from datetime import date, datetime
from typing import Optional, Union

from langchain_core.tools import tool

from tools.location_trans import transform_location

db = "../travel_new.sqlite"  # 这是数据库文件名


@tool
def search_hotels(
        location: Optional[str] = None,
        name: Optional[str] = None
        # price_tier: Optional[str] = None,
        # checkin_date: Optional[Union[datetime, date]] = None,
        # checkout_date: Optional[Union[datetime, date]] = None,
) -> list[dict]:
    """
    根据位置、名称、价格层级、入住日期和退房日期搜索酒店。

    参数:
        location (Optional[str]): 酒店的位置。默认为None。
        name (Optional[str]): 酒店的名称。默认为None。

    返回:
        list[dict]: 包含匹配搜索条件的酒店信息的字典列表。
    """

    conn = connect(db)
    cursor = conn.cursor()
    location = transform_location(location)
    query = "SELECT * FROM hotels WHERE 1=1"
    params = []

    if location:
        query += " AND location LIKE ?"
        params.append(f"%{location}%")
    if name:
        query += " AND name LIKE ?"
        params.append(f"%{name}%")
    # 为了本教程的目的，我们不对日期和价格层级进行严格匹配

    print('查询酒店的SQL：' + query, '参数: ', params)
    cursor.execute(query, params)
    results = cursor.fetchall()
    print('查询酒店的结果: ', results)
    conn.close()

    return [
        dict(zip([column[0] for column in cursor.description], row)) for row in results
    ]


@tool
def book_hotel(hotel_id: int) -> str:
    """
    通过ID预订酒店。

    参数:
        hotel_id (int): 要预订的酒店的ID。

    返回:
        str: 表明酒店是否成功预订的消息。
    """
    conn = connect(db)
    cursor = conn.cursor()

    cursor.execute("UPDATE hotels SET booked = 1 WHERE id = ?", (hotel_id,))
    conn.commit()

    if cursor.rowcount > 0:
        conn.close()
        return f"Hotel {hotel_id} 成功预定。"
    else:
        conn.close()
        return f"未找到ID为 {hotel_id} 的酒店。"


@tool
def update_hotel(
        hotel_id: int,
        checkin_date: Optional[Union[datetime, date]] = None,
        checkout_date: Optional[Union[datetime, date]] = None,
) -> str:
    """
    根据ID更新酒店预订的入住和退房日期。

    参数:
        hotel_id (int): 要更新的酒店预订的ID。
        checkin_date (Optional[Union[datetime, date]]): 酒店的新入住日期。默认为None。
        checkout_date (Optional[Union[datetime, date]]): 酒店的新退房日期。默认为None。

    返回:
        str: 表明酒店预订是否成功更新的消息。
    """
    conn = connect(db)
    cursor = conn.cursor()

    if checkin_date:
        cursor.execute(
            "UPDATE hotels SET checkin_date = ? WHERE id = ?", (checkin_date, hotel_id)
        )
    if checkout_date:
        cursor.execute(
            "UPDATE hotels SET checkout_date = ? WHERE id = ?", (checkout_date, hotel_id)
        )

    conn.commit()

    if cursor.rowcount > 0:
        conn.close()
        return f"Hotel {hotel_id} 成功更新。"
    else:
        conn.close()
        return f"未找到ID为 {hotel_id} 的酒店。"


@tool
def cancel_hotel(hotel_id: int) -> str:
    """
    根据ID取消酒店预订。

    参数:
        hotel_id (int): 要取消的酒店预订的ID。

    返回:
        str: 表明酒店预订是否成功取消的消息。
    """
    conn = connect(db)
    cursor = conn.cursor()

    # 将booked字段设置为0来表示取消预订
    cursor.execute("UPDATE hotels SET booked = 0 WHERE id = ?", (hotel_id,))
    conn.commit()

    if cursor.rowcount > 0:
        conn.close()
        return f"Hotel {hotel_id} 成功取消。"
    else:
        conn.close()
        return f"未找到ID为 {hotel_id} 的酒店。"
