import sys
# 禁用Python生成.pyc文件，避免缓存文件的生成
sys.dont_write_bytecode = True

import gradio as gr 
from fronted.chat_interface import ChatInterface 
from app_config.config import settings  
from utils.logger import logger
import asyncio

async def main():
    """
    主函数：初始化并启动Gradio聊天界面服务
    
    异步函数，负责：
    1. 创建聊天接口实例
    2. 初始化Gradio界面
    3. 配置并启动Web服务
    """
    try:  
        # 初始化聊天接口实例
        chat_interface = ChatInterface()
        # 创建Gradio界面
        demo = await chat_interface.create_interface()
        logger.info("Gradio界面创建成功")
        
        # 配置并启动Gradio服务，api_open=False禁用API端点暴露
        # queue()处理并发请求，确保请求按照队列顺序进行处理。
        demo.queue(api_open=False)
        demo.launch(
            server_name=settings.HOST,  # 服务器主机地址
            server_port=settings.PORT,  # 服务器端口
            show_api=False,  # 不显示API文档
            share=False  # 不创建公共URL
        )
        logger.info("服务启动成功")
        
    except Exception as e:
        # 记录错误日志并重新抛出异常
        logger.error(f"服务启动失败: {str(e)}")
        raise

if __name__ == "__main__":
    # 启动事件循环并执行异步任务
    asyncio.run(main()) 