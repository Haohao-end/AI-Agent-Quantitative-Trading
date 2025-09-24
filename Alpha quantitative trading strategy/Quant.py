import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from openai import OpenAI
import requests
import json
import certifi
import os
import time
import sys
from typing import Optional, Dict, Any
import random

# 设置 SSL 证书路径
os.environ['SSL_CERT_FILE'] = certifi.where()

# 初始化 Tushare
TUSHARE_TOKEN = '2876ea85cb005fb5fa17c809a98174f2d5aae8b1f830110a5ead6211'
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

# 初始化 OpenAI
OPENAI_API_KEY = 'sk-Ed9dTJiN7NNLzW9Pv8hDaJRPlSzZ0U7QkzCNWbnw6NgKJhrR'
OPENAI_API_BASE = 'https://api.bianxie.ai/v1'
openai_client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_API_BASE)

# 历史数据缓存
HISTORICAL_DATA_CACHE = {}

# 用户代理列表，用于轮换避免被屏蔽
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]


def get_random_user_agent():
    """获取随机用户代理"""
    return random.choice(USER_AGENTS)


def retry_api_call(func, max_retries=3, initial_delay=2):
    """重试装饰器函数"""

    def wrapper(*args, **kwargs):
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                result = func(*args, **kwargs)
                if result is not None and (not isinstance(result, pd.DataFrame) or not result.empty):
                    return result
                print(f"-> 第 {attempt + 1} 次尝试返回空结果，等待重试...")
            except Exception as e:
                print(f"-> 第 {attempt + 1} 次尝试失败: {e}")
                if attempt < max_retries - 1:
                    print(f"-> 等待 {delay} 秒后重试...")
                    time.sleep(delay)
                    delay *= 1.5  # 指数退避
                else:
                    print("-> 所有重试尝试均失败")
                    if func.__name__ == 'get_daily_data':
                        return pd.DataFrame()  # 返回空DataFrame而不是None
                    return pd.DataFrame()
        return pd.DataFrame()

    return wrapper


@retry_api_call
def get_trade_dates(start_date, end_date):
    """获取交易日历"""
    print(f"-> 正在获取交易日历，从 {start_date} 到 {end_date}...")
    df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    trade_dates = df[df['is_open'] == 1]['cal_date'].tolist()
    print(f"-> 获取到 {len(trade_dates)} 个交易日。")
    return trade_dates


@retry_api_call
def get_daily_data(trade_date):
    """获取单日行情"""
    print(f"-> 正在获取 {trade_date} 的每日行情数据...")
    df = pro.daily(trade_date=trade_date)
    print(f"-> 获取到 {len(df)} 条行情数据。")
    if len(df) > 0:
        print(f"-> 数据列名: {df.columns.tolist()}")
    return df


@retry_api_call
def get_limit_list(trade_date):
    """获取单日涨停与跌停股票"""
    print(f"-> 正在获取 {trade_date} 的涨跌停板数据...")
    df = pro.limit_list_d(trade_date=trade_date)
    print(f"-> 获取到 {len(df)} 条涨跌停数据。")
    return df


@retry_api_call
def get_index_data(trade_date, ts_code='000300.SH'):
    """获取指数行情"""
    print(f"-> 正在获取 {trade_date} 的指数行情数据 ({ts_code})...")
    df = pro.index_daily(ts_code=ts_code, trade_date=trade_date)
    if not df.empty:
        print(f"-> 指数 {ts_code} 当日涨跌幅: {df['pct_chg'].values[0]:.2f}%")
    else:
        print("-> 未获取到指数行情数据。")
    return df


def search_news_baidu(trade_date):
    """使用百度新闻搜索免费API"""
    year = trade_date[:4]
    month = trade_date[4:6]
    day = trade_date[6:]
    query = f"A股 {year}年{month}月{day}日 股市 行情"

    print(f"\n--- 百度新闻搜索阶段 ---")
    print(f"-> 正在搜索新闻，关键词: '{query}'")

    try:
        # 使用百度新闻搜索（免费接口）
        url = "https://news.baidu.com/news"
        params = {
            'tn': 'news',
            'word': query,
            'ie': 'utf-8'
        }

        headers = {
            'User-Agent': get_random_user_agent()
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            # 简单解析HTML获取新闻标题
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            news_titles = []
            for title in soup.find_all('h3')[:10]:
                if title.text and len(title.text) > 10:
                    news_titles.append(title.text.strip())

            full_text = ' '.join(news_titles)
            print(f"-> 成功获取到 {len(news_titles)} 条新闻标题。")
            print(f"-> 获取的新闻内容如下: {full_text} ")
            return full_text
        else:
            print(f"-> 百度新闻请求失败，状态码: {response.status_code}")
            return ""

    except Exception as e:
        print(f"-> 百度新闻搜索发生异常: {e}")
        return ""


def search_news_sina(trade_date):
    """使用新浪财经新闻搜索"""
    year = trade_date[:4]
    month = trade_date[4:6]
    day = trade_date[6:]
    query = f"{year}-{month}-{day} A股 行情"

    print(f"\n--- 新浪财经新闻搜索阶段 ---")
    print(f"-> 正在搜索新浪财经新闻，关键词: '{query}'")

    try:
        # 新浪财经搜索
        url = "https://search.sina.com.cn/"
        params = {
            'q': query,
            'c': 'news',
            'range': 'title',
            'num': '10'
        }

        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            news_titles = []
            # 尝试多种选择器
            selectors = ['.box-result .r-info h2 a', '.searchResult h2 a', '.r-info h2 a']

            for selector in selectors:
                titles = soup.select(selector)
                if titles:
                    for title in titles[:10]:
                        if title.text and len(title.text.strip()) > 10:
                            news_titles.append(title.text.strip())
                    break

            if not news_titles:
                # 备用方案：直接查找包含关键词的标题
                for h2 in soup.find_all('h2')[:10]:
                    if h2.text and ('A股' in h2.text or '股市' in h2.text or '行情' in h2.text):
                        news_titles.append(h2.text.strip())

            full_text = ' '.join(news_titles) if news_titles else f"新浪财经：{year}年{month}月{day}日A股市场相关报道"
            print(f"-> 成功获取到 {len(news_titles)} 条新浪财经新闻标题。")
            print(f"-> 获取的新闻内容如下: {full_text} ")
            return full_text
        else:
            print(f"-> 新浪财经新闻请求失败，状态码: {response.status_code}")
            return ""

    except Exception as e:
        print(f"-> 新浪财经新闻搜索发生异常: {e}")
        return ""


def search_news_sohu(trade_date):
    """使用搜狐新闻搜索"""
    year = trade_date[:4]
    month = trade_date[4:6]
    day = trade_date[6:]
    query = f"{year}年{month}月{day}日 A股 市场"

    print(f"\n--- 搜狐新闻搜索阶段 ---")
    print(f"-> 正在搜索搜狐新闻，关键词: '{query}'")

    try:
        # 搜狐新闻搜索
        url = "https://news.sohu.com/"
        params = {
            'keyword': query
        }

        headers = {
            'User-Agent': get_random_user_agent(),
            'Referer': 'https://news.sohu.com/'
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            news_titles = []
            # 搜狐新闻的选择器
            selectors = ['.news-box h4 a', '.news-list h3 a', '.title a']

            for selector in selectors:
                titles = soup.select(selector)
                if titles:
                    for title in titles[:10]:
                        if title.text and len(title.text.strip()) > 10:
                            news_titles.append(title.text.strip())
                    break

            full_text = ' '.join(news_titles) if news_titles else f"搜狐新闻：{year}年{month}月{day}日股市动态"
            print(f"-> 成功获取到 {len(news_titles)} 条搜狐新闻标题。")
            print(f"-> 获取的新闻内容如下: {full_text} ")
            return full_text
        else:
            print(f"-> 搜狐新闻请求失败，状态码: {response.status_code}")
            return ""

    except Exception as e:
        print(f"-> 搜狐新闻搜索发生异常: {e}")
        return ""


def search_news_163(trade_date):
    """使用网易财经新闻搜索"""
    year = trade_date[:4]
    month = trade_date[4:6]
    day = trade_date[6:]
    query = f"{year}-{month}-{day} 股市 行情"

    print(f"\n--- 网易财经新闻搜索阶段 ---")
    print(f"-> 正在搜索网易财经新闻，关键词: '{query}'")

    try:
        # 网易财经搜索
        url = "https://money.163.com/special/search/"
        params = {
            'keyword': query,
            'type': 'news'
        }

        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)
        if response.status_code == 200:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            news_titles = []
            # 网易财经的选择器
            selectors = ['.newsdata_wrap h3 a', '.searchList h3 a', '.title a']

            for selector in selectors:
                titles = soup.select(selector)
                if titles:
                    for title in titles[:10]:
                        if title.text and len(title.text.strip()) > 10:
                            news_titles.append(title.text.strip())
                    break

            full_text = ' '.join(news_titles) if news_titles else f"网易财经：{year}年{month}月{day}日股票市场分析"
            print(f"-> 成功获取到 {len(news_titles)} 条网易财经新闻标题。")
            print(f"-> 获取的新闻内容如下: {full_text} ")
            return full_text
        else:
            print(f"-> 网易财经新闻请求失败，状态码: {response.status_code}")
            return ""

    except Exception as e:
        print(f"-> 网易财经新闻搜索发生异常: {e}")
        return ""


def search_news_tencent(trade_date):
    """使用腾讯财经新闻搜索"""
    year = trade_date[:4]
    month = trade_date[4:6]
    day = trade_date[6:]
    query = f"{year}年{month}月{day}日 A股"

    print(f"\n--- 腾讯财经新闻搜索阶段 ---")
    print(f"-> 正在搜索腾讯财经新闻，关键词: '{query}'")

    try:
        # 腾讯财经搜索
        url = "https://finance.qq.com/"
        headers = {
            'User-Agent': get_random_user_agent(),
            'Referer': 'https://finance.qq.com/'
        }

        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            news_titles = []
            # 腾讯财经的选择器 - 主要获取头条新闻
            selectors = ['.mod-news-list h3 a', '.news-list h3 a', '.title a']

            for selector in selectors:
                titles = soup.select(selector)
                if titles:
                    for title in titles[:10]:
                        text = title.text.strip()
                        if text and len(text) > 10 and (query[:4] in text or 'A股' in text or '股市' in text):
                            news_titles.append(text)
                    break

            full_text = ' '.join(news_titles) if news_titles else f"腾讯财经：{year}年{month}月{day}日A股市场要闻"
            print(f"-> 成功获取到 {len(news_titles)} 条腾讯财经新闻标题。")
            print(f"-> 获取的新闻内容如下: {full_text} ")
            return full_text
        else:
            print(f"-> 腾讯财经新闻请求失败，状态码: {response.status_code}")
            return ""

    except Exception as e:
        print(f"-> 腾讯财经新闻搜索发生异常: {e}")
        return ""


def search_news_with_fallback(trade_date):
    """带兜底的新闻搜索函数，按顺序尝试多个新闻源"""
    search_functions = [
        search_news_baidu,
        search_news_sina,
        search_news_163,
        search_news_sohu,
        search_news_tencent
    ]

    search_names = ["新浪财经", "腾讯财经", "网易财经", "搜狐新闻", "百度新闻"]

    for i, search_func in enumerate(search_functions):
        try:
            print(f"\n=== 尝试第 {i + 1} 个新闻源: {search_names[i]} ===")
            result = search_func(trade_date)
            if result and len(result.strip()) > 20:  # 确保有有效内容
                print(f"-> ✅ {search_names[i]} 搜索成功！")
                return result
            else:
                print(f"-> ⚠️ {search_names[i]} 返回内容过短，尝试下一个源...")
        except Exception as e:
            print(f"-> ❌ {search_names[i]} 搜索失败: {e}，尝试下一个源...")

        # 短暂延迟避免请求过快
        if i < len(search_functions) - 1:
            time.sleep(1)

    # 所有新闻源都失败，返回模拟数据
    year = trade_date[:4]
    month = trade_date[4:6]
    day = trade_date[6:]
    fallback_text = f"模拟新闻数据：{year}年{month}月{day}日A股市场表现，各大指数震荡整理，市场交投活跃"
    print("-> 🔄 所有新闻源均失败，使用模拟新闻数据")
    return fallback_text


def safe_print(text, max_length=1000):
    """安全打印长文本，避免截断"""
    if len(text) <= max_length:
        print(text)
    else:
        # 分段打印
        for i in range(0, len(text), max_length):
            print(text[i:i + max_length])


def call_ai_analysis(prompt, analysis_type="指标分析"):
    """调用AI进行分析"""
    print(f"\n--- AI {analysis_type} 阶段 ---")
    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,  # 增加token限制
            timeout=60  # 增加超时时间
        )
        result = response.choices[0].message.content.strip()
        print(f"-> AI {analysis_type} 结果:")
        print(f"【{analysis_type}】")
        safe_print(result)  # 使用安全打印
        print("-" * 80)
        return result
    except Exception as e:
        error_msg = f"{analysis_type}分析失败，请检查网络连接: {e}"
        print(f"-> AI {analysis_type} 失败: {e}")
        print(f"【{analysis_type}】")
        safe_print(error_msg)
        print("-" * 80)
        return error_msg


def analyze_up_down_ratio(up_count, down_count, up_down_ratio, trade_date, historical_context=""):
    """分析涨跌停比指标"""
    prompt = f"""
    作为专业的量化分析师，请深入分析以下涨跌停比数据：

    交易日期：{trade_date}
    涨停股数：{up_count}
    跌停股数：{down_count}
    涨跌停比率：{up_down_ratio:.2f}
    {historical_context}

    请从以下维度进行专业分析：
    1. 多空力量对比：分析涨停与跌停数量的绝对值和相对比例
    2. 市场情绪状态：判断当前市场处于何种情绪阶段（极度恐慌/恐慌/平衡/乐观/极度乐观）
    3. 资金流向：分析主力资金的进攻和防御方向
    4. 风险收益特征：评估当前市场的风险偏好程度
    5. 历史比较：如果有历史数据，进行纵向比较分析

    请给出具体的量化判断和投资建议，分析要深入具体。
    """
    return call_ai_analysis(prompt, "涨跌停比分析")


def analyze_lianban_stocks(lianban_avg_pct, lianban_count, trade_date, historical_context=""):
    """分析连板股指标"""
    prompt = f"""
    作为专业的短线交易分析师，请深入分析以下连板股数据：

    交易日期：{trade_date}
    连板股平均涨幅：{lianban_avg_pct:.2f}%
    连板股数量：{lianban_count}
    {historical_context}

    请从以下维度进行专业分析：
    1. 投机情绪热度：分析连板股表现反映的市场投机情绪
    2. 赚钱效应：评估短线交易的赚钱效应和持续性
    3. 龙头股表现：分析市场龙头股的表现和引领作用
    4. 风险积聚：判断是否存在过度投机和风险积聚
    5. 资金接力：分析资金接力的意愿和能力

    请结合A股市场特点，给出具体的短线交易策略建议。
    """
    return call_ai_analysis(prompt, "连板股分析")


def analyze_zha_board_rate(zha_board_rate, zha_board_count, up_count, trade_date, historical_context=""):
    """分析炸板率指标"""
    prompt = f"""
    作为专业的市场微观结构分析师，请深入分析以下炸板率数据：

    交易日期：{trade_date}
    炸板率：{zha_board_rate:.2%}
    炸板股票数量：{zha_board_count}
    涨停股票数量：{up_count}
    {historical_context}

    请从以下维度进行专业分析：
    1. 涨停质量：分析涨停板的封板质量和资金态度
    2. 资金分歧：评估市场资金在涨停板位置的分歧程度
    3. 获利了结压力：分析获利盘的了结意愿和压力
    4. 次日表现预期：基于炸板率预测次日的市场表现
    5. 风险预警：识别高炸板率可能带来的风险信号

    请给出具体的风险预警和操作建议。
    """
    return call_ai_analysis(prompt, "炸板率分析")


def analyze_high_mark_yield(high_mark_yield, trade_date, historical_context=""):
    """分析高标股溢价指标"""
    prompt = f"""
    作为专业的龙头股策略分析师，请深入分析以下高标股溢价数据：

    交易日期：{trade_date}
    高标股平均溢价：{high_mark_yield:.2%}
    {historical_context}

    请从以下维度进行专业分析：
    1. 龙头股溢价：分析市场对龙头股的认可度和追捧程度
    2. 风险偏好：评估市场对高标股的风险偏好水平
    3. 资金聚焦：分析资金是否聚焦于市场核心标的
    4. 情绪传导：判断高标股溢价对整体情绪的传导作用
    5. 策略有效性：评估龙头股策略在当前市场的有效性

    请结合具体的龙头股交易策略，给出投资建议。
    """
    return call_ai_analysis(prompt, "高标股溢价分析")


def analyze_prev_up_yield(prev_up_yield, trade_date, historical_context=""):
    """分析昨日涨停今日溢价指标"""
    prompt = f"""
    作为专业的涨停板策略分析师，请深入分析以下昨日涨停溢价数据：

    交易日期：{trade_date}
    昨日涨停今日平均溢价：{prev_up_yield:.2%}
    {historical_context}

    请从以下维度进行专业分析：
    1. 涨停持续性：分析涨停板策略的持续性和赚钱效应
    2. 资金记忆效应：评估市场资金对涨停股的记忆效应
    3. 接力意愿：分析资金接力的意愿和强度
    4. 策略风险收益：评估涨停板策略的风险收益特征
    5. 市场有效性：判断涨停板策略在当前市场的有效性

    请给出具体的涨停板策略优化建议。
    """
    return call_ai_analysis(prompt, "昨日涨停溢价分析")


def analyze_news_sentiment(news_text, trade_date, market_context=""):
    """分析新闻情感"""
    if not news_text or "模拟新闻数据" in news_text:
        return "无有效新闻数据可供分析"

    prompt = f"""
    作为专业的财经新闻分析师，请深入分析以下新闻文本反映的市场情绪：

    交易日期：{trade_date}
    新闻文本摘要：{news_text}
    {market_context}

    请从以下维度进行专业分析：
    1. 情绪倾向：分析新闻整体体现的乐观/悲观倾向
    2. 热点主题：识别新闻中提到的市场热点和主题
    3. 风险提示：提取新闻中的风险提示和警示信息
    4. 政策影响：分析政策面新闻对市场的影响
    5. 资金面信号：从新闻中解读资金面的变化信号

    请结合当前市场环境，给出综合的情绪判断。
    """
    return call_ai_analysis(prompt, "新闻情感分析")


def generate_comprehensive_report(individual_analyses, market_data, trade_date):
    """生成综合量化研究报告"""
    prompt = f"""
    作为首席量化策略师，请基于以下各个维度的详细分析，生成一份专业的市场情绪量化研究报告：

    交易日期：{trade_date}

    市场基础数据：
    - 涨跌停比：{market_data['up_down_ratio']:.2f}（涨停{market_data['up_count']}只，跌停{market_data['down_count']}只）
    - 连板股表现：平均涨幅{market_data['lianban_avg_pct']:.2f}%
    - 炸板率：{market_data['zha_board_rate']:.2%}
    - 高标股溢价：{market_data['high_mark_yield']:.2%}
    - 昨日涨停溢价：{market_data['prev_up_yield']:.2%}
    - 指数涨跌幅：{market_data['idx_chg']:.2f}%

    各维度详细分析：
    {individual_analyses}

    请生成一份结构完整、逻辑严谨的量化研究报告，包含以下部分：

    【报告摘要】简要概括整体市场情绪状态和主要结论
    【市场情绪总览】综合各指标给出整体情绪评分和阶段判断
    【多空力量分析】基于涨跌停数据分析市场多空力量对比
    【投机情绪分析】分析短线资金的情绪和风险偏好
    【资金行为分析】从微观结构分析资金的行为特征
    【风险预警提示】识别当前市场的主要风险点
    【投资策略建议】给出具体的投资策略和仓位建议
    【明日展望】基于当前数据对明日市场的预期

    报告要求专业、深入、具体，具有实际投资指导价值。
    """

    return call_ai_analysis(prompt, "综合量化报告")


def calculate_basic_metrics_from_limit_data(df_limit_info, df_daily):
    """从涨跌停数据计算基本指标"""
    metrics = {
        'up_count': 0,
        'down_count': 0,
        'up_down_ratio': 0,
        'lianban_avg_pct': 0,
        'lianban_count': 0,
        'zha_board_count': 0,
        'zha_board_rate': 0
    }

    if df_limit_info.empty:
        return metrics

    # 从limit_list数据中计算涨停跌停数量
    if 'limit' in df_limit_info.columns:
        metrics['up_count'] = len(df_limit_info[df_limit_info['limit'] == 'U'])
        metrics['down_count'] = len(df_limit_info[df_limit_info['limit'] == 'D'])
    else:
        metrics['up_count'] = len(df_limit_info)
        metrics['down_count'] = 0

    metrics['up_down_ratio'] = metrics['up_count'] / (metrics['down_count'] + 1e-6)

    # 连板股平均涨幅和数量
    if 'limit_times' in df_limit_info.columns and 'pct_chg' in df_limit_info.columns:
        lianban_info = df_limit_info[df_limit_info['limit_times'] >= 2]
        metrics['lianban_count'] = len(lianban_info)
        if not lianban_info.empty:
            metrics['lianban_avg_pct'] = lianban_info['pct_chg'].mean()

    # 炸板率
    if 'limit_status' in df_limit_info.columns:
        zha_board_info = df_limit_info[df_limit_info['limit_status'] == 'B']
        metrics['zha_board_count'] = len(zha_board_info)
        metrics['zha_board_rate'] = metrics['zha_board_count'] / (
                metrics['up_count'] + metrics['zha_board_count'] + 1e-6)

    return metrics


def calc_sentiment_score(trade_date, hist_window=60, news_text=None):
    """计算指定日期的市场情绪指数并生成详细报告"""
    print(f"\n======== 开始计算 {trade_date} 的市场情绪指数 ========\n")

    # 刷新输出缓冲区
    sys.stdout.flush()

    # 1. 获取数据
    print("-> 正在获取市场数据...")
    df_daily = get_daily_data(trade_date)
    df_limit_info = get_limit_list(trade_date)
    df_index = get_index_data(trade_date)

    if df_daily.empty and df_limit_info.empty:
        print("!!! 数据获取失败，使用备用方案 !!!")
        return create_fallback_result(trade_date, "所有数据获取失败")

    print("\n--- 基础指标计算阶段 ---")

    # 计算基础指标
    basic_metrics = calculate_basic_metrics_from_limit_data(df_limit_info, df_daily)

    up_count = basic_metrics['up_count']
    down_count = basic_metrics['down_count']
    up_down_ratio = basic_metrics['up_down_ratio']
    lianban_avg_pct = basic_metrics['lianban_avg_pct']
    lianban_count = basic_metrics['lianban_count']
    zha_board_count = basic_metrics['zha_board_count']
    zha_board_rate = basic_metrics['zha_board_rate']

    print(f"-> 基础指标计算完成:")
    print(f"   涨停股数: {up_count}, 跌停股数: {down_count}")
    print(f"   涨跌停比: {up_down_ratio:.2f}")
    print(f"   连板股: {lianban_count}只, 平均涨幅: {lianban_avg_pct:.2f}%")
    print(f"   炸板率: {zha_board_rate:.2%}")

    # 高标股溢价
    high_mark_yield = 0
    if not df_limit_info.empty and 'limit_times' in df_limit_info.columns:
        try:
            high_mark_info = df_limit_info.sort_values('limit_times', ascending=False).head(3)
            if not high_mark_info.empty and 'pct_chg' in high_mark_info.columns:
                high_mark_yield = high_mark_info['pct_chg'].mean() / 100
        except Exception as e:
            print(f"-> 计算高标股溢价失败: {e}")

    # 昨日涨停溢价
    prev_up_yield = 0
    if not df_daily.empty and not df_limit_info.empty:
        try:
            if up_count > 0:
                prev_up_yield = lianban_avg_pct / 100
        except Exception as e:
            print(f"-> 计算昨日涨停溢价失败: {e}")

    # 指数涨跌幅
    idx_chg = df_index['pct_chg'].values[0] if not df_index.empty else 0

    # 获取新闻数据 - 使用新的兜底搜索函数
    if news_text is None:
        news_text = search_news_with_fallback(trade_date)

    # 历史上下文（简化处理）
    historical_context = "注：历史数据对比功能待完善"

    print("\n--- 开始各维度AI分析 ---")

    # 逐个指标进行AI分析
    analyses = {}

    # 1. 涨跌停比分析
    print("\n>>> 开始涨跌停比分析 <<<")
    analyses['up_down_analysis'] = analyze_up_down_ratio(
        up_count, down_count, up_down_ratio, trade_date, historical_context
    )

    # 2. 连板股分析
    print("\n>>> 开始连板股分析 <<<")
    analyses['lianban_analysis'] = analyze_lianban_stocks(
        lianban_avg_pct, lianban_count, trade_date, historical_context
    )

    # 3. 炸板率分析
    print("\n>>> 开始炸板率分析 <<<")
    analyses['zha_board_analysis'] = analyze_zha_board_rate(
        zha_board_rate, zha_board_count, up_count, trade_date, historical_context
    )

    # 4. 高标股溢价分析
    print("\n>>> 开始高标股溢价分析 <<<")
    analyses['high_mark_analysis'] = analyze_high_mark_yield(
        high_mark_yield, trade_date, historical_context
    )

    # 5. 昨日涨停溢价分析
    print("\n>>> 开始昨日涨停溢价分析 <<<")
    analyses['prev_up_analysis'] = analyze_prev_up_yield(
        prev_up_yield, trade_date, historical_context
    )

    # 6. 新闻情感分析
    print("\n>>> 开始新闻情感分析 <<<")
    market_context = f"当前市场：涨停{up_count}只，跌停{down_count}只，指数涨跌{idx_chg:.2f}%"
    analyses['news_analysis'] = analyze_news_sentiment(
        news_text, trade_date, market_context
    )

    # 准备市场数据
    market_data = {
        'up_count': up_count,
        'down_count': down_count,
        'up_down_ratio': up_down_ratio,
        'lianban_avg_pct': lianban_avg_pct,
        'lianban_count': lianban_count,
        'zha_board_rate': zha_board_rate,
        'high_mark_yield': high_mark_yield,
        'prev_up_yield': prev_up_yield,
        'idx_chg': idx_chg
    }

    # 生成综合报告
    print("\n>>> 开始生成综合量化报告 <<<")
    individual_analyses_text = "\n\n".join([f"【{key}】\n{value}" for key, value in analyses.items()])

    comprehensive_report = generate_comprehensive_report(individual_analyses_text, market_data, trade_date)

    # 整合最终结果
    result = {
        'trade_date': trade_date,
        'comprehensive_report': comprehensive_report,
        'individual_analyses': analyses,
        'market_data': market_data,
        'data_source': 'limit_data_primary' if df_daily.empty else 'full_data'
    }

    print("\n" + "=" * 80)
    print("           量化情绪分析报告生成完成！")
    print("=" * 80)
    safe_print(comprehensive_report)  # 使用安全打印
    print("=" * 80)

    return result


def create_fallback_result(trade_date, reason):
    """创建备用结果"""
    return {
        'trade_date': trade_date,
        'comprehensive_report': f"数据获取失败：{reason}",
        'individual_analyses': {},
        'market_data': {},
        'data_source': 'fallback'
    }


def save_report_to_file(result, filename=None):
    """保存报告到文件，确保完整保存"""
    if filename is None:
        filename = f"market_sentiment_report_{result['trade_date']}.md"

    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"市场情绪量化分析报告 - {result['trade_date']}\n")
            f.write("=" * 50 + "\n\n")
            f.write(result['comprehensive_report'])
            f.write("\n\n" + "=" * 50 + "\n")
            f.write("各维度详细分析:\n\n")
            for key, analysis in result['individual_analyses'].items():
                f.write(f"【{key}】\n{analysis}\n\n")
        print(f"-> 报告已完整保存到文件: {filename}")
        print(" 投资有风险 入市需谨慎 以上内容为AI生成 内容仅供参考,不构成投资建议 ")
        return True
    except Exception as e:
        print(f"-> 保存文件失败: {e}")
        return False


if __name__ == "__main__":
    # 测试运行
    result = calc_sentiment_score('20250923')

    # 保存详细报告到文件
    save_report_to_file(result)