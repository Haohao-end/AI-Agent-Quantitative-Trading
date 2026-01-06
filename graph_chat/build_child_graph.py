from langchain_core.messages import ToolMessage
from langgraph.constants import END
from langgraph.graph import StateGraph
from langgraph.prebuilt import tools_condition

from graph_chat.agent_assistant import update_flight_runnable, update_flight_sensitive_tools, update_flight_safe_tools, \
    book_car_rental_runnable, book_car_rental_safe_tools, book_car_rental_sensitive_tools, book_hotel_runnable, \
    book_hotel_safe_tools, book_hotel_sensitive_tools, book_excursion_runnable, book_excursion_safe_tools, \
    book_excursion_sensitive_tools
from graph_chat.assistant import CtripAssistant
from graph_chat.base_data_model import CompleteOrEscalate
from graph_chat.entry_node import create_entry_node
from tools.tools_handler import create_tool_node_with_fallback


# 航班助手的 子工作流
def build_flight_graph(builder: StateGraph) -> StateGraph:
    """构建 航班预订助理的子工作流图"""
    # 添加入口节点，当需要更新或取消航班时使用
    builder.add_node(
        "enter_update_flight",
        create_entry_node("Flight Updates & Booking Assistant", "update_flight"),  # 创建入口节点，指定助理名称和新对话状态
    )
    builder.add_node("update_flight", CtripAssistant(update_flight_runnable))  # 添加处理航班更新的实际节点
    builder.add_edge("enter_update_flight", "update_flight")  # 连接入口节点到实际处理节点

    # 添加敏感工具和安全工具的节点
    builder.add_node(
        "update_flight_sensitive_tools",
        create_tool_node_with_fallback(update_flight_sensitive_tools),  # 敏感工具节点，包含可能修改数据的操作
    )
    builder.add_node(
        "update_flight_safe_tools",
        create_tool_node_with_fallback(update_flight_safe_tools),  # 安全工具节点，通常只读查询
    )

    def route_update_flight(state: dict):
        """
        根据当前状态路由航班更新流程。

        :param state: 当前对话状态字典
        :return: 下一步应跳转到的节点名
        """
        route = tools_condition(state)  # 判断下一步的方向
        if route == END:
            return END  # 如果结束条件满足，则返回END
        tool_calls = state["messages"][-1].tool_calls  # 获取最后一条消息中的工具调用
        did_cancel = any(tc["name"] == CompleteOrEscalate.__name__ for tc in tool_calls)  # 检查是否调用了CompleteOrEscalate
        if did_cancel:
            return "leave_skill"  # 如果用户请求取消或退出，则跳转至leave_skill节点
        safe_tool_names = [t.name for t in update_flight_safe_tools]  # 获取所有安全工具的名字
        if all(tc["name"] in safe_tool_names for tc in tool_calls):  # 如果所有调用的工具都是安全工具
            return "update_flight_safe_tools"  # 跳转至安全工具处理节点
        return "update_flight_sensitive_tools"  # 否则跳转至敏感工具处理节点

    # 添加边，连接敏感工具和安全工具节点回到航班更新处理节点
    builder.add_edge("update_flight_sensitive_tools", "update_flight")
    builder.add_edge("update_flight_safe_tools", "update_flight")

    # 根据条件路由航班更新流程
    builder.add_conditional_edges(
        "update_flight",
        route_update_flight,
        ["update_flight_sensitive_tools", "update_flight_safe_tools", "leave_skill", END],
    )

    # 此节点将用于所有子助理的退出
    def pop_dialog_state(state: dict) -> dict:
        """
        弹出对话栈并返回主助理。
        这使得完整的图可以明确跟踪对话流，并根据需要委托控制给特定的子图。
        :param state: 当前对话状态字典
        :return: 包含新的对话状态和消息的字典
        """
        messages = []
        if state["messages"][-1].tool_calls:
            # 注意：目前不处理LLM同时执行多个工具调用的情况
            messages.append(
                ToolMessage(
                    content="正在恢复与主助理的对话。请回顾之前的对话并根据需要协助用户。",
                    tool_call_id=state["messages"][-1].tool_calls[0]["id"],
                )
            )
        return {
            "dialog_state": "pop",  # 更新对话状态为弹出
            "messages": messages,  # 返回消息列表
        }

    # 添加退出技能节点，并连接回主助理
    builder.add_node("leave_skill", pop_dialog_state)
    builder.add_edge("leave_skill", "primary_assistant")
    return builder


def build_car_graph(builder: StateGraph) -> StateGraph:
    # 租车助理 的子工作流
    # 添加入口节点，当需要预订租车时使用
    builder.add_node(
        "enter_book_car_rental",
        create_entry_node("Car Rental Assistant", "book_car_rental"),  # 创建入口节点，指定助理名称和新对话状态
    )
    builder.add_node("book_car_rental", CtripAssistant(book_car_rental_runnable))  # 添加处理租车预订的实际节点
    builder.add_edge("enter_book_car_rental", "book_car_rental")  # 连接入口节点到实际处理节点

    # 添加安全工具和敏感工具的节点
    builder.add_node(
        "book_car_rental_safe_tools",
        create_tool_node_with_fallback(book_car_rental_safe_tools),  # 安全工具节点，通常只读查询
    )
    builder.add_node(
        "book_car_rental_sensitive_tools",
        create_tool_node_with_fallback(book_car_rental_sensitive_tools),  # 敏感工具节点，包含可能修改数据的操作
    )

    def route_book_car_rental(state: dict):
        """
        根据当前状态路由租车预订流程。

        :param state: 当前对话状态字典
        :return: 下一步应跳转到的节点名
        """
        route = tools_condition(state)  # 判断下一步的方向
        if route == END:
            return END  # 如果结束条件满足，则返回END
        tool_calls = state["messages"][-1].tool_calls  # 获取最后一条消息中的工具调用
        did_cancel = any(tc["name"] == CompleteOrEscalate.__name__ for tc in tool_calls)  # 检查是否调用了CompleteOrEscalate
        if did_cancel:
            return "leave_skill"  # 如果用户请求取消或退出，则跳转至leave_skill节点
        safe_toolnames = [t.name for t in book_car_rental_safe_tools]  # 获取所有安全工具的名字
        if all(tc["name"] in safe_toolnames for tc in tool_calls):  # 如果所有调用的工具都是安全工具
            return "book_car_rental_safe_tools"  # 跳转至安全工具处理节点
        return "book_car_rental_sensitive_tools"  # 否则跳转至敏感工具处理节点

    # 添加边，连接敏感工具和安全工具节点回到租车预订处理节点
    builder.add_edge("book_car_rental_sensitive_tools", "book_car_rental")
    builder.add_edge("book_car_rental_safe_tools", "book_car_rental")

    # 根据条件路由租车预订流程
    builder.add_conditional_edges(
        "book_car_rental",
        route_book_car_rental,
        [
            "book_car_rental_safe_tools",
            "book_car_rental_sensitive_tools",
            "leave_skill",
            END,
        ],
    )
    return builder


# 酒店预订助理
def builder_hotel_graph(builder: StateGraph) -> StateGraph:
    # 添加入口节点，当需要预订酒店时使用
    builder.add_node(
        "enter_book_hotel",
        create_entry_node("酒店预订助理", "book_hotel"),  # 创建入口节点，指定助理名称和新对话状态
    )
    builder.add_node("book_hotel", CtripAssistant(book_hotel_runnable))  # 添加处理酒店预订的实际节点
    builder.add_edge("enter_book_hotel", "book_hotel")  # 连接入口节点到实际处理节点

    # 添加安全工具和敏感工具的节点
    builder.add_node(
        "book_hotel_safe_tools",
        create_tool_node_with_fallback(book_hotel_safe_tools),  # 安全工具节点，通常只读查询
    )
    builder.add_node(
        "book_hotel_sensitive_tools",
        create_tool_node_with_fallback(book_hotel_sensitive_tools),  # 敏感工具节点，包含可能修改数据的操作
    )

    def route_book_hotel(state: dict):
        """
        根据当前状态路由酒店预订流程。

        :param state: 当前对话状态字典
        :return: 下一步应跳转到的节点名
        """
        route = tools_condition(state)  # 判断下一步的方向
        if route == END:
            return END  # 如果结束条件满足，则返回END
        tool_calls = state["messages"][-1].tool_calls  # 获取最后一条消息中的工具调用
        did_cancel = any(tc["name"] == CompleteOrEscalate.__name__ for tc in tool_calls)  # 检查是否调用了CompleteOrEscalate
        if did_cancel:
            return "leave_skill"  # 如果用户请求取消或退出，则跳转至leave_skill节点
        safe_toolnames = [t.name for t in book_hotel_safe_tools]  # 获取所有安全工具的名字
        if all(tc["name"] in safe_toolnames for tc in tool_calls):  # 如果所有调用的工具都是安全工具
            return "book_hotel_safe_tools"  # 跳转至安全工具处理节点
        return "book_hotel_sensitive_tools"  # 否则跳转至敏感工具处理节点

    # 添加边，连接敏感工具和安全工具节点回到酒店预订处理节点
    builder.add_edge("book_hotel_sensitive_tools", "book_hotel")
    builder.add_edge("book_hotel_safe_tools", "book_hotel")

    # 根据条件路由酒店预订流程
    builder.add_conditional_edges(
        "book_hotel",
        route_book_hotel,
        ["leave_skill", "book_hotel_safe_tools", "book_hotel_sensitive_tools", END],
    )
    return builder


# 构建一个旅游产品的子图
def builder_excursion_graph(builder: StateGraph) -> StateGraph:
    # 添加入口节点，当需要预订游览或获取旅行推荐时使用
    builder.add_node(
        "enter_book_excursion",
        create_entry_node("旅行推荐助理", "book_excursion"),  # 创建入口节点，指定助理名称和新对话状态
    )
    builder.add_node("book_excursion", CtripAssistant(book_excursion_runnable))  # 添加处理游览预订的实际节点
    builder.add_edge("enter_book_excursion", "book_excursion")  # 连接入口节点到实际处理节点

    # 添加安全工具和敏感工具的节点
    builder.add_node(
        "book_excursion_safe_tools",
        create_tool_node_with_fallback(book_excursion_safe_tools),  # 安全工具节点，通常只读查询
    )
    builder.add_node(
        "book_excursion_sensitive_tools",
        create_tool_node_with_fallback(book_excursion_sensitive_tools),  # 敏感工具节点，包含可能修改数据的操作
    )

    def route_book_excursion(state: dict):
        """
        根据当前状态路由游览预订流程。

        :param state: 当前对话状态字典
        :return: 下一步应跳转到的节点名
        """
        route = tools_condition(state)  # 判断下一步的方向
        if route == END:
            return END  # 如果结束条件满足，则返回END
        tool_calls = state["messages"][-1].tool_calls  # 获取最后一条消息中的工具调用
        did_cancel = any(tc["name"] == CompleteOrEscalate.__name__ for tc in tool_calls)  # 检查是否调用了CompleteOrEscalate
        if did_cancel:
            return "leave_skill"  # 如果用户请求取消或退出，则跳转至leave_skill节点
        safe_toolnames = [t.name for t in book_excursion_safe_tools]  # 获取所有安全工具的名字
        if all(tc["name"] in safe_toolnames for tc in tool_calls):  # 如果所有调用的工具都是安全工具
            return "book_excursion_safe_tools"  # 跳转至安全工具处理节点
        return "book_excursion_sensitive_tools"  # 否则跳转至敏感工具处理节点

    # 添加边，连接敏感工具和安全工具节点回到游览预订处理节点
    builder.add_edge("book_excursion_sensitive_tools", "book_excursion")
    builder.add_edge("book_excursion_safe_tools", "book_excursion")

    # 根据条件路由游览预订流程
    builder.add_conditional_edges(
        "book_excursion",
        route_book_excursion,
        ["book_excursion_safe_tools", "book_excursion_sensitive_tools", "leave_skill", END],
    )
    return builder
