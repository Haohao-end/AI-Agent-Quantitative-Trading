from sqlite3 import connect, Cursor
from typing import Optional, List

from langchain_core.tools import tool

from tools.location_trans import transform_location

db = "../travel_new.sqlite"  # 这是数据库文件名


@tool
def search_trip_recommendations(
        location: Optional[str] = None,
        name: Optional[str] = None,
        keywords: Optional[str] = None,
) -> List[dict]:
    """
    根据位置、名称和关键词搜索旅行推荐。

    参数:
        location (Optional[str]): 旅行推荐的位置。默认为None。
        name (Optional[str]): 旅行推荐的名称。默认为None。
        keywords (Optional[str]): 关联到旅行推荐的关键词。默认为None。

    返回:
        list[dict]: 包含匹配搜索条件的旅行推荐字典列表。
    """
    conn = connect(db)
    cursor = conn.cursor()
    location = transform_location(location)
    query = "SELECT * FROM trip_recommendations WHERE 1=1"
    params = []

    if location:
        query += " AND location LIKE ?"
        params.append(f"%{location}%")
    if name:
        query += " AND name LIKE ?"
        params.append(f"%{name}%")
    if keywords:
        keyword_list = keywords.split(",")
        keyword_conditions = " OR ".join(["keywords LIKE ?" for _ in keyword_list])
        query += f" AND ({keyword_conditions})"
        params.extend([f"%{keyword.strip()}%" for keyword in keyword_list])

    cursor.execute(query, params)
    results = cursor.fetchall()

    conn.close()

    return [
        dict(zip([column[0] for column in cursor.description], row)) for row in results
    ]


@tool
def book_excursion(recommendation_id: int) -> str:
    """
    通过推荐ID预订一次旅行项目。

    参数:
        recommendation_id (int): 要预订的旅行推荐的ID。

    返回:
        str: 表明旅行推荐是否成功预订的消息。
    """
    conn = connect(db)
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE trip_recommendations SET booked = 1 WHERE id = ?", (recommendation_id,)
    )
    conn.commit()

    if cursor.rowcount > 0:
        conn.close()
        return f"旅行推荐  {recommendation_id} 成功预定."
    else:
        conn.close()
        return f"未找到与 ID 相关的旅行推荐信息。 {recommendation_id}."


@tool
def update_excursion(recommendation_id: int, details: str) -> str:
    """
    根据ID更新旅行推荐的详细信息。

    参数:
        recommendation_id (int): 要更新的旅行推荐的ID。
        details (str): 旅行推荐的新详细信息。

    返回:
        str: 表明旅行推荐是否成功更新的消息。
    """
    conn = connect(db)
    cursor = conn.cursor()

    cursor.execute(
        "UPDATE trip_recommendations SET details = ? WHERE id = ?",
        (details, recommendation_id),
    )
    conn.commit()

    if cursor.rowcount > 0:
        conn.close()
        return f"旅行推荐 {recommendation_id} 成功更新。"
    else:
        conn.close()
        return f"未找到ID为 {recommendation_id} 的旅行推荐。"


@tool
def cancel_excursion(recommendation_id: int) -> str:
    """
    根据ID取消旅行推荐。

    参数:
        recommendation_id (int): 要取消的旅行推荐的ID。

    返回:
        str: 表明旅行推荐是否成功取消的消息。
    """
    conn = connect(db)
    cursor = conn.cursor()

    # 将booked字段设置为0来表示取消预订
    cursor.execute(
        "UPDATE trip_recommendations SET booked = 0 WHERE id = ?", (recommendation_id,)
    )
    conn.commit()

    if cursor.rowcount > 0:
        conn.close()
        return f"旅行推荐 {recommendation_id} 成功取消。"
    else:
        conn.close()
        return f"未找到ID为 {recommendation_id} 的旅行推荐。"
