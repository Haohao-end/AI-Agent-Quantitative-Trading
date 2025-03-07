from langchain_ollama import OllamaEmbeddings
# FAISS用于存储和检索向量
from langchain_community.vectorstores import FAISS
# BaseRetriever是检索器的基类,提供了检索文档的基本接口
from langchain.schema import BaseRetriever
from app_config.config import settings
from utils.logger import logger
import os
import asyncio
# BaseModel用于数据验证,Field用于字段定义
from pydantic import BaseModel, Field
from typing import Any

class AsyncVectorStoreRetriever(BaseRetriever, BaseModel):
    """异步向量存储检索器"""
    
    retriever: Any = Field(description="原始向量存储检索器")
    
    class Config:
        arbitrary_types_allowed = True
    
    def __init__(self, vectorstore_retriever, **kwargs):
        """初始化检索器
        Args:
            vectorstore_retriever: 原始向量存储检索器
        """
        super().__init__(retriever=vectorstore_retriever, **kwargs)
        
    async def _aget_relevant_documents(self, query, **kwargs):
        """异步获取相关文档"""
        # 将同步检索操作包装成异步操作
        # 使用to_thread在单独线程中执行，避免阻塞事件循环
        return await asyncio.to_thread(
            self.retriever.get_relevant_documents,
            query,
            **kwargs
        )
        
    def _get_relevant_documents(self, query, **kwargs):
        """同步获取相关文档"""
        # 直接调用原始检索器的方法
        return self.retriever.get_relevant_documents(query, **kwargs)



class VectorStore:
    def __init__(self):
        """初始化向量存储类"""
        try:
            self.embeddings = OllamaEmbeddings(
                model=settings.OLLAMA_MODEL
            )
            logger.info(f"已加载embedding模型: {settings.OLLAMA_MODEL}")
        except Exception as e:
            logger.error(f"加载embedding模型失败: {str(e)}")
            raise
            
    async def get_vectorstore(self, documents):
        """创建或获取向量存储"""
        try:
            # 确保向量存储目录存在
            os.makedirs(settings.VECTOR_DB_PATH, exist_ok=True)
            
            index_path = os.path.join(settings.VECTOR_DB_PATH, "index.faiss")

            if os.path.exists(index_path):
                try:
                    # 如果存在，则加载现有的向量存储
                    vectorstore = FAISS.load_local(
                        settings.VECTOR_DB_PATH,
                        self.embeddings
                    )
                    logger.info("已加载现有的向量存储")
                except Exception as e:
                    logger.warning(f"加载现有向量存储失败，将创建新的向量存储: {str(e)}")
                    vectorstore = await self._create_vectorstore(documents)
            else:
                vectorstore = await self._create_vectorstore(documents)

            # 创建基础检索器，用于从向量数据库中检索最相似的文档
            # k : 表示检索时返回的最相似文档数量
            base_retriever = vectorstore.as_retriever(
                search_kwargs={"k": 2}
            )
            
            # 包装为异步检索器，提高系统的并发处理能力
            return AsyncVectorStoreRetriever(base_retriever)

        except Exception as e:
            logger.error(f"向量存储错误: {str(e)}", exc_info=True)
            raise

    async def _create_vectorstore(self, documents):
        """创建新的向量存储"""
        try:
            # 创建向量存储
            # 将同步操作转换为异步操作，避免阻塞事件循环
            """代码逻辑：
            (1) 使用embeddings模型将文档文本转换为向量;
            (2) 创建FAISS索引存储这些向量;
            (3) 建立文档内容和向量之间的映射;
            """
            vectorstore = await asyncio.to_thread(
                FAISS.from_documents,
                documents,
                self.embeddings
            )
            
            # 保存向量存储
            vectorstore.save_local(settings.VECTOR_DB_PATH)
            logger.info("已创建并保存新的向量存储")
            return vectorstore
            
        except Exception as e:
            logger.error(f"创建向量存储失败: {str(e)}", exc_info=True)
            raise 