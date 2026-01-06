from sqlite3 import connect, Cursor
from datetime import date, datetime
from typing import Optional, List, Dict
import pytz
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

db = "../travel_new.sqlite"  # 数据库文件名


@tool
def fetch_user_flight_information(config: RunnableConfig) -> List[Dict]:
    """
    此函数通过给定的乘客ID，从数据库中获取该乘客的所有机票信息及其相关联的航班信息和座位分配情况。
    返回:
        包含每张机票的详情、关联航班的信息及座位分配的字典列表。
    """
    configuration = config.get("configurable", {})
    passenger_id = configuration.get("passenger_id", None)
    if not passenger_id:
        raise ValueError("未配置乘客 ID。")

    conn = connect(db)
    cursor = conn.cursor()

    # SQL查询语句，连接多个表以获取所需信息
    query = """
    SELECT 
        t.ticket_no, t.book_ref,
        f.flight_id, f.flight_no, f.departure_airport, f.arrival_airport, f.scheduled_departure, f.scheduled_arrival,
        bp.seat_no, tf.fare_conditions
    FROM 
        tickets t
        JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no
        JOIN flights f ON tf.flight_id = f.flight_id
        JOIN boarding_passes bp ON bp.ticket_no = t.ticket_no AND bp.flight_id = f.flight_id
    WHERE 
        t.passenger_id = ?
    """
    cursor.execute(query, (passenger_id,))
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    results = [dict(zip(column_names, row)) for row in rows]

    cursor.close()
    conn.close()

    return results


@tool
def search_flights(
        departure_airport: Optional[str] = None,
        arrival_airport: Optional[str] = None,
        start_time: Optional[date | datetime] = None,
        end_time: Optional[date | datetime] = None,
        limit: int = 20,
) -> List[Dict]:
    """
    根据指定的参数（如出发机场、到达机场、出发时间范围等）搜索航班，并返回匹配的航班列表。
    可以设置一个限制值来控制返回的结果数量。

    参数:
    - departure_airport (Optional[str]): 出发机场（可选）。
    - arrival_airport (Optional[str]): 到达机场（可选）。
    - start_time (Optional[date | datetime]): 出发时间范围的开始时间（可选）。
    - end_time (Optional[date | datetime]): 出发时间范围的结束时间（可选）。
    - limit (int): 返回结果的最大数量，默认为20。

    返回:
        匹配条件的航班信息列表。
    """
    conn = connect(db)
    cursor = conn.cursor()

    query = "SELECT * FROM flights WHERE 1 = 1"
    params = []

    if departure_airport:
        query += " AND departure_airport = ?"
        params.append(departure_airport)

    if arrival_airport:
        query += " AND arrival_airport = ?"
        params.append(arrival_airport)

    if start_time:
        query += " AND scheduled_departure >= ?"
        params.append(start_time)

    if end_time:
        query += " AND scheduled_departure <= ?"
        params.append(end_time)

    query += " LIMIT ?"
    params.append(limit)
    cursor.execute(query, params)
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    results = [dict(zip(column_names, row)) for row in rows]

    cursor.close()
    conn.close()

    return results


@tool
def update_ticket_to_new_flight(
        ticket_no: str, new_flight_id: int, *, config: RunnableConfig
) -> str:
    """
    将用户的机票更新为新的有效航班。步骤如下：
    1、检查乘客ID：首先从传入的配置中获取乘客ID，并验证其是否存在。
    2、查询新航班详情：根据提供的新航班ID查询航班详情，包括出发机场、到达机场和计划起飞时间。
    3、时间验证：确保新选择的航班起飞时间与当前时间相差不少于3小时。
    4、确认原机票存在性：验证提供的机票号是否存在于系统中。
    5、验证乘客身份：确保请求修改机票的乘客是该机票的实际拥有者。
    6、更新机票信息：如果所有检查都通过，则更新机票对应的新航班ID，并提交更改。

    参数:
    - ticket_no (str): 要更新的机票编号。
    - new_flight_id (int): 新的航班ID。
    - config (RunnableConfig): 配置信息，包含乘客ID等必要参数。

    返回:
    - str: 操作结果的消息。
    """
    configuration = config.get("configurable", {})
    passenger_id = configuration.get("passenger_id", None)
    if not passenger_id:
        raise ValueError("未配置乘客 ID。")

    conn = connect(db)
    cursor = conn.cursor()

    # 查询新航班的信息
    cursor.execute(
        "SELECT departure_airport, arrival_airport, scheduled_departure FROM flights WHERE flight_id = ?",
        (new_flight_id,),
    )
    new_flight = cursor.fetchone()
    if not new_flight:
        cursor.close()
        conn.close()
        return "提供的新的航班 ID 无效。"
    column_names = [column[0] for column in cursor.description]
    new_flight_dict = dict(zip(column_names, new_flight))

    # 设置时区并计算当前时间和新航班起飞时间之间的差值
    timezone = pytz.timezone("Etc/GMT-3")
    current_time = datetime.now(tz=timezone)
    departure_time = datetime.strptime(
        new_flight_dict["scheduled_departure"], "%Y-%m-%d %H:%M:%S.%f%z"
    )
    time_until = (departure_time - current_time).total_seconds()
    if time_until < (3 * 3600):
        return f"不允许重新安排到距离当前时间少于 3 小时的航班。所选航班时间为 {departure_time}。"

    # 确认原机票的存在性
    cursor.execute(
        "SELECT flight_id FROM ticket_flights WHERE ticket_no = ?", (ticket_no,)
    )
    current_flight = cursor.fetchone()
    if not current_flight:
        cursor.close()
        conn.close()
        return "未找到给定机票号码的现有机票。"

    # 确认已登录用户确实拥有此机票
    cursor.execute(
        "SELECT * FROM tickets WHERE ticket_no = ? AND passenger_id = ?",
        (ticket_no, passenger_id),
    )
    current_ticket = cursor.fetchone()
    if not current_ticket:
        cursor.close()
        conn.close()
        return f"当前登录的乘客 ID 为 {passenger_id}，不是机票 {ticket_no} 的拥有者。"

    # 更新机票对应的航班ID
    cursor.execute(
        "UPDATE ticket_flights SET flight_id = ? WHERE ticket_no = ?",
        (new_flight_id, ticket_no),
    )
    conn.commit()

    cursor.close()
    conn.close()
    return "机票已成功更新为新的航班。"


@tool
def cancel_ticket(ticket_no: str, *, config: RunnableConfig) -> str:
    """
    取消用户的机票并将其从数据库中删除。步骤如下：
    1、检查乘客ID：首先从传入的配置中获取乘客ID，并验证其是否存在。
    2、查询机票存在性：根据提供的机票号查询该机票是否存在于系统中。
    3、验证乘客身份：确保请求取消机票的乘客是该机票的实际拥有者。
    4、删除机票信息：如果所有检查都通过，则从数据库中删除该机票的信息，并提交更改。

    参数:
    - ticket_no (str): 要取消的机票编号。
    - config (RunnableConfig): 配置信息，包含乘客ID等必要参数。

    返回:
    - str: 操作结果的消息。
    """
    configuration = config.get("configurable", {})
    passenger_id = configuration.get("passenger_id", None)
    if not passenger_id:
        raise ValueError("未配置乘客 ID。")

    conn = connect(db)
    cursor = conn.cursor()

    # 查询给定机票号是否存在
    cursor.execute(
        "SELECT flight_id FROM ticket_flights WHERE ticket_no = ?", (ticket_no,)
    )
    existing_ticket = cursor.fetchone()
    if not existing_ticket:
        cursor.close()
        conn.close()
        return "未找到给定机票号码的现有机票。"

    # 确认已登录用户确实拥有此机票
    cursor.execute(
        "SELECT flight_id FROM tickets WHERE ticket_no = ? AND passenger_id = ?",
        (ticket_no, passenger_id),
    )
    current_ticket = cursor.fetchone()
    if not current_ticket:
        cursor.close()
        conn.close()
        return f"当前登录的乘客 ID 为 {passenger_id}，不是机票 {ticket_no} 的拥有者。"

    # 删除机票对应的记录
    cursor.execute("DELETE FROM ticket_flights WHERE ticket_no = ?", (ticket_no,))
    conn.commit()

    cursor.close()
    conn.close()
    return "机票已成功取消。"
