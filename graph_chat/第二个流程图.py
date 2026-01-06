import uuid

from langchain_core.messages import ToolMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.constants import START, END
from langgraph.graph import StateGraph
from langgraph.prebuilt import tools_condition
from tools.flights_tools import fetch_user_flight_information
from graph_chat.assistant import create_assistant_node, safe_tools, sensitive_tools, sensitive_tool_names
from graph_chat.draw_png import draw_graph
from graph_chat.state import State
from tools.init_db import update_dates
from tools.tools_handler import create_tool_node_with_fallback, _print_event

# 定义了一个流程图的构建对象
builder = StateGraph(State)


def get_user_info(state: State):
    """
    获取用户的航班信息并更新状态字典。
    参数:
        state (State): 当前状态字典。
    返回:
        dict: 包含用户信息的新状态字典。
    """
    return {"user_info": fetch_user_flight_information.invoke({})}


# 新增：fetch_user_info节点首先运行，这意味着我们的助手可以在不采取任何行动的情况下看到用户的航班信息
builder.add_node('fetch_user_info', get_user_info)
builder.add_edge(START, 'fetch_user_info')

# 自定义函数代表节点，Runnable，或者一个自定义的类都可以是节点
builder.add_node('assistant', create_assistant_node())
# 把所有的工具拆成两个节点
builder.add_node("safe_tools", create_tool_node_with_fallback(safe_tools))
builder.add_node("sensitive_tools", create_tool_node_with_fallback(sensitive_tools))

# 从起始点START到"assistant"节点添加一条边
builder.add_edge('fetch_user_info', "assistant")


def route_conditional_tools(state: State) -> str:
    """
    根据当前状态，来决定下一个要执行的节点
    :param state: 当前的状态
    :return: str 下一个要执行节点的名字
    """
    next_node = tools_condition(state)
    if next_node == END:
        return END

    ai_message = state['messages'][-1]
    tool_call = ai_message.tool_calls[0]
    if tool_call['name'] in sensitive_tool_names:  # 敏感的工具调用
        return 'sensitive_tools'
    return 'safe_tools'


builder.add_conditional_edges(
    "assistant",
    route_conditional_tools,
    ['safe_tools', 'sensitive_tools', END]
)
# 从"tools"节点回到"assistant"节点添加一条边
builder.add_edge("safe_tools", "assistant")
builder.add_edge("sensitive_tools", "assistant")

# 检查点让状态图可以持久化其状态
# 这是整个状态图的完整内存
memory = MemorySaver()

# 编译状态图，配置检查点为memory, 配置中断点
graph = builder.compile(checkpointer=memory, interrupt_before=['sensitive_tools'], )

#
# draw_graph(graph, 'graph3.png')

session_id = str(uuid.uuid4())
update_dates()  # 每次测试的时候：保证数据库是全新的，保证，时间也是最近的时间

# 配置参数，包含乘客ID和线程ID
config = {
    "configurable": {
        # passenger_id用于我们的航班工具，以获取用户的航班信息
        "passenger_id": "3442 587242",
        # 检查点由session_id访问
        "thread_id": session_id,
    }
}

_printed = set()  # set集合，避免重复打印

# 执行工作流
while True:
    question = input('用户：')
    if question.lower() in ['q', 'exit', 'quit']:
        print('对话结束，拜拜！')
        break
    else:
        events = graph.stream({'messages': ('user', question)}, config, stream_mode='values')
        # 打印消息
        for event in events:
            _print_event(event, _printed)

        current_state = graph.get_state(config)
        if current_state.next:
            user_input = input(
                "您是否批准上述操作？输入'y'继续；否则，请说明您请求的更改。\n"
            )
            if user_input.strip().lower() == "y":
                # 继续执行
                events = graph.stream(None, config, stream_mode='values')
                # 打印消息
                for event in events:
                    _print_event(event, _printed)
            else:
                # 通过提供关于请求的更改/改变主意的指示来满足工具调用
                result = graph.stream(
                    {
                        "messages": [
                            ToolMessage(
                                tool_call_id=event["messages"][-1].tool_calls[0]["id"],
                                content=f"Tool的调用被用户拒绝。原因：'{user_input}'。",
                            )
                        ]
                    },
                    config,
                )
                # 打印事件详情
                for event in result:
                    _print_event(event, _printed)
