from typing import Callable
from langchain_core.messages import ToolMessage


#
def create_entry_node(assistant_name: str, new_dialog_state: str) -> Callable:
    """
    这是一个函数工程： 创建一个入口节点函数，当对话状态转换时调用。
    该函数生成一条新的对话消息，并更新对话的状态。

    :param assistant_name: 新助理的名字或描述。
    :param new_dialog_state: 要更新到的新对话状态。
    :return: 返回一个根据给定的assistant_name和new_dialog_state处理对话状态的函数。
    """

    def entry_node(state: dict) -> dict:
        """
        根据当前对话状态生成新的对话消息并更新对话状态。

        :param state: 当前对话状态，包含所有消息。
        :return: 包含新消息和更新后的对话状态的字典。
        """
        # 获取最后一个消息中的工具调用ID
        tool_call_id = state["messages"][-1].tool_calls[0]["id"]

        return {
            "messages": [
                ToolMessage(
                    content=f"现在助手是{assistant_name}。请回顾上述主助理与用户之间的对话。"
                            f"用户的意图尚未满足。使用提供的工具协助用户。记住，您是{assistant_name}，"
                            "并且预订、更新或其他操作未完成，直到成功调用了适当的工具。"
                            "如果用户改变主意或需要帮助进行其他任务，请调用CompleteOrEscalate函数让主要的主助理接管。"
                            "不要提及你是谁——仅作为助理的代理。",
                    tool_call_id=tool_call_id,
                )
            ],
            "dialog_state": new_dialog_state,
        }

    return entry_node
