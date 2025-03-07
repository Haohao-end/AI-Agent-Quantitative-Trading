from langchain_community.document_loaders import PyMuPDFLoader
# RecursiveCharacterTextSplitter 用于递归地将文本分割成更小的部分
from langchain.text_splitter import RecursiveCharacterTextSplitter
from app_config.config import settings
from utils.logger import logger
# asyncio 提供异步I/O操作的支持，允许编写并发代码
import asyncio
# ThreadPoolExecutor 用于管理线程池，支持多线程并发执行
from concurrent.futures import ThreadPoolExecutor

class PDFProcessor:
    """
    PDF文档处理器类
    
    该类负责处理PDF文件，包括文档加载和文本分块。
    使用异步操作和线程池来优化性能。
    """
    
    def __init__(self):
        """
        创建一个文本分割器实例和线程池执行器：
        - 文本分割器：用于将PDF文档分割成较小的文本块
        - 线程池：用于处理IO密集型操作
        """
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=settings.CHUNK_SIZE,  # 每个文本块的最大字符数
            chunk_overlap=settings.CHUNK_OVERLAP  # 相邻文本块之间的重叠字符数
        )
        # 创建线程池，用于管理线程池，支持多线程并发执行
        self.executor = ThreadPoolExecutor(max_workers=settings.WORKERS)  
        
    async def process_pdf(self, pdf_path: str):
        """
        异步处理PDF文件

        该方法使用线程池执行器来异步处理PDF文件，避免阻塞主事件循环
        """
        try:
            # 获取当前事件循环
            loop = asyncio.get_event_loop()  
            # 在线程池中执行IO密集型操作
            chunks = await loop.run_in_executor(
                self.executor,
                self._process_pdf_sync,
                pdf_path
            )
            return chunks
        except Exception as e:
            logger.error(f"PDF处理错误: {str(e)}")
            raise
            
    def _process_pdf_sync(self, pdf_path: str):
        """
        同步处理PDF文件
        """
        loader = PyMuPDFLoader(pdf_path)  # 创建PDF加载器实例
        data = loader.load()  # 加载PDF文件内容
        return self.text_splitter.split_documents(data)  # 将文档分割成块并返回 