import ollama 
from app_config.config  import settings
from utils.logger import logger
import re
import asyncio
class LLMService:
    def __init__(self):
        """
        初始化LLM服务
        - model: 从配置中获取使用的语言模型名称
        - semaphore: 创建信号量控制并发请求数量
        """
        self.model = settings.OLLAMA_MODEL
        # 用于控制并发请求的数量，用于限制同时运行的协程数量，最多允许settings.WORKERS个协程同时运行。
        self.semaphore = asyncio.Semaphore(settings.WORKERS)

    async def generate_response(self, question: str, context: str, chat_history: list):
        """
        生成回答
        
        Args:
            question: 用户当前问题
            context: 相关文档上下文
            chat_history: 历史对话记录
            
        Returns:
            str: 生成的回答内容
        """
        try:
            # 记录用户问题
            logger.info(f"用户问题: {question}")

            # 构建系统提示词
            system_prompt = """你是一个专业的AI助手。请基于提供的上下文回答问题。
            - 回答要简洁明了，避免重复
            - 如果上下文中没有相关信息，请直接说明
            - 保持回答的连贯性和逻辑性
            - 请用中文回答"""
            
            # 构建用户提示词
            user_prompt = f"""基于以下信息回答问题：
            问题：{question}
            相关上下文：{context}"""

            # 调用Ollama API，使用信号量控制并发（限制同时运行的并发请求数量）
            async with self.semaphore:  
                # 使用asyncio.to_thread将同步操作转换为异步操作，避免阻塞事件循环
                response = await asyncio.to_thread(
                    ollama.chat, # 一个同步阻塞调用
                    model=self.model, 
                    # 消息格式
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ]
                )
            
            # 处理响应
            content = response["message"]["content"]
            # 移除思考过程标记
            final_answer = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()
            
            # 记录模型回答
            logger.info(f"模型回答: {final_answer}")
            
            return final_answer
            
        except Exception as e:
            logger.error(f"生成回答失败: {str(e)}", exc_info=True)
            return "抱歉，生成回答时出现错误，请稍后重试。"
        
