import gradio as gr
from core.pdf_processor import PDFProcessor
from core.vector_store import VectorStore
from core.llm_service import LLMService
from utils.logger import logger
import asyncio

class ChatInterface:
    def __init__(self):
        self.pdf_processor = PDFProcessor()
        self.vector_store = VectorStore()
        self.llm_service = LLMService()
        
    async def create_interface(self):
        """创建Gradio界面"""
        with gr.Blocks(css=self._get_css()) as demo:
            # 状态变量
            pdf_state = gr.State(None)
            text_splitter_state = gr.State(None)
            vectorstore_state = gr.State(None)
            retriever_state = gr.State(None)
            
            # 标题
            gr.Markdown("# 📚 RAG 智能问答系统-Demo", elem_classes="header")
            
            # 文件上传区域
            with gr.Column(elem_classes="file-upload"):
                file = gr.File(
                    label="上传PDF文件",
                    file_types=[".pdf"],
                    elem_classes="file-upload"
                )
            
            # 聊天区域
            with gr.Column(elem_classes="chat-container"):
                chatbot = gr.Chatbot(
                    height=500,
                    bubble_full_width=False,
                    show_label=False,
                    avatar_images=None,
                    elem_classes="chatbot"
                )
                
                with gr.Row():
                    msg = gr.Textbox(
                        label="输入问题",
                        placeholder="请输入您的问题...",
                        scale=12,
                        container=False
                    )
                    send_btn = gr.Button("发送", scale=1, variant="primary")

                with gr.Row(elem_classes="button-row"):
                    clear = gr.Button("清空对话", variant="secondary")
                    regenerate = gr.Button("重新生成", variant="secondary")
            
            # 处理文件上传
            file.upload(
                self._process_file,
                [file],
                [pdf_state, text_splitter_state, vectorstore_state, retriever_state],
            )
            
            # 发送消息-方式1 : 按回车键提交
            msg.submit(
                self._respond,
                [msg, chatbot, pdf_state, text_splitter_state, vectorstore_state, retriever_state],
                [msg, chatbot]
            )
            # 发送消息-方式2 : 点击发送按钮
            send_btn.click(
                self._respond,
                [msg, chatbot, pdf_state, text_splitter_state, vectorstore_state, retriever_state],
                [msg, chatbot]
            )
            
            # 清空对话
            clear.click(lambda: None, None, chatbot, queue=False)
            
            # 重新生成
            regenerate.click(
                self._regenerate,
                [chatbot, pdf_state, text_splitter_state, vectorstore_state, retriever_state],
                [chatbot]
            )
            
        return demo
    
    async def _process_file(self, file):
        """处理上传的PDF文件"""
        try:
            if file is None:
                return None, None, None, None
                
            # 处理PDF文件
            chunks = await self.pdf_processor.process_pdf(file.name)
            
            # 创建向量存储和检索器
            retriever = await self.vector_store.get_vectorstore(chunks)
            
            # 直接返回 retriever，不需要再次调用 as_retriever()
            return file.name, self.pdf_processor.text_splitter, None, retriever
        except Exception as e:
            logger.error(f"文件处理错误: {str(e)}")
            raise
    
    async def _respond(self, message, history, pdf_path, text_splitter, vectorstore, retriever):
        """处理用户消息"""
        try:
            if not message.strip():
                return "", history
                
            if pdf_path is None:
                # 普通对话模式
                response = await self.llm_service.generate_response(message, "", history)
            else:
                # RAG对话模式
                try:
                    # 使用 get_relevant_documents 替代 ainvoke
                    docs = await asyncio.to_thread(
                        retriever.get_relevant_documents,
                        message
                    )
                    context = "\n\n".join(doc.page_content for doc in docs)
                    
                    # 生成回答
                    response = await self.llm_service.generate_response(message, context, history)
                except Exception as e:
                    logger.error(f"检索文档失败: {str(e)}", exc_info=True)
                    response = "抱歉，检索相关文档时出现错误，请稍后重试。"

            history.append((message, response))
            return "", history
            
        except Exception as e:
            logger.error(f"响应生成错误: {str(e)}", exc_info=True)
            error_message = "抱歉，处理您的问题时出现错误，请稍后重试。"
            history.append((message, error_message))
            return "", history
            
    async def _regenerate(self, history, pdf_path, text_splitter, vectorstore, retriever):
        """重新生成回答"""
        try:
            if not history:
                return history
                
            last_user_message = history[-1][0]
            history = history[:-1]
            
            if pdf_path is None:
                response = await self.llm_service.generate_response(last_user_message, "", history)
            else:
                # 使用 invoke 方法替代 get_relevant_documents
                docs = await retriever.ainvoke(last_user_message)
                context = "\n\n".join(doc.page_content for doc in docs)
                response = await self.llm_service.generate_response(last_user_message, context, history)
                
            history.append((last_user_message, response))
            return history
        except Exception as e:
            logger.error(f"重新生成错误: {str(e)}")
            return history
            
    def _get_css(self):
        """获取CSS样式"""
        return """
        .container { max-width: 1200px; margin: auto; padding: 20px; }
        .header { text-align: center; margin-bottom: 30px; }
        .file-upload { 
            border: 2px dashed #ddd; 
            padding: 20px; 
            border-radius: 10px;
            background: #f9f9f9;
            margin-bottom: 20px;
        }
        .chat-container {
            border: 1px solid #eee;
            border-radius: 10px;
            padding: 20px;
            background: white;
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
        }
        .button-row { display: flex; gap: 10px; margin-top: 10px; }
        .chatbot .user {
            background-color: #f0f0f0 !important;
            border-radius: 8px !important;
            padding: 12px 16px !important;
            margin: 8px 0 !important;
        }
        .chatbot .assistant {
            background-color: #e3f2fd !important;
            border-radius: 8px !important;
            padding: 12px 16px !important;
            margin: 8px 0 !important;
        }
        """ 
