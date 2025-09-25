import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from openai import OpenAI
import certifi
import os
import time
import sys
# 设置 SSL 证书路径
os.environ['SSL_CERT_FILE'] = certifi.where()

# 初始化 Tushare
TUSHARE_TOKEN = '你的TushareToken'
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

# 初始化 OpenAI
OPENAI_API_KEY = '你的OpenAI Key'
OPENAI_API_BASE = '访问地址 默认为:https://api.openai.com/v1 或者使用中转接口'
openai_client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_API_BASE.strip())

# 历史数据缓存
HISTORICAL_DATA_CACHE = {}

def retry_api_call(func, max_retries=3, initial_delay=2):
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
                    delay *= 1.5
                else:
                    print("-> 所有重试尝试均失败")
                    if func.__name__ == 'get_daily_data':
                        return pd.DataFrame()
                    return pd.DataFrame()
        return pd.DataFrame()
    return wrapper


@retry_api_call
def get_trade_dates(start_date, end_date):
    print(f"-> 正在获取交易日历，从 {start_date} 到 {end_date}...")
    df = pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
    trade_dates = df[df['is_open'] == 1]['cal_date'].tolist()
    print(f"-> 获取到 {len(trade_dates)} 个交易日。")
    return trade_dates


@retry_api_call
def get_daily_data(trade_date):
    print(f"-> 正在获取 {trade_date} 的每日行情数据...")
    df = pro.daily(trade_date=trade_date)
    print(f"-> 获取到 {len(df)} 条行情数据。")
    if len(df) > 0:
        print(f"-> 数据列名: {df.columns.tolist()}")
    return df


@retry_api_call
def get_limit_list(trade_date):
    print(f"-> 正在获取 {trade_date} 的涨跌停板数据...")
    df = pro.limit_list_d(trade_date=trade_date)
    print(f"-> 获取到 {len(df)} 条涨跌停数据。")
    return df


# ts_code默认是上证指数 000001.SH 可以换成沪深300 000300.SH
@retry_api_call
def get_index_data(trade_date, ts_code='000001.SH'):
    print(f"-> 正在获取 {trade_date} 的指数行情数据 ({ts_code})...")
    df = pro.index_daily(ts_code=ts_code, trade_date=trade_date)
    if not df.empty:
        print(f"-> 指数 {ts_code} 当日涨跌幅: {df['pct_chg'].values[0]:.2f}%")
    else:
        print("-> 未获取到指数行情数据。")
    return df


# ========== 简化版情绪打分 用于历史归一化 ==========
def calc_sentiment_score_simple(trade_date):
    """简化版，仅用于历史分位归一化"""
    df_limit = get_limit_list(trade_date)
    if df_limit.empty:
        return {
            'up_down_ratio': 0,
            'lianban_avg_pct': 0,
            'zha_board_rate': 0,
            'high_mark_yield': 0,
            'prev_up_yield': 0,
            'sentiment_score': 0
        }

    # 注意：Tushare 的 limit_list_d 返回字段为 'limit' 而非 'limit_status'
    # 'U'=涨停, 'D'=跌停, 'B'=炸板（需确认）
    # 实际字段可能为 'limit_type' 或 'limit'，此处按文档处理

    up_limit = df_limit[df_limit['limit'] == 'U']
    down_limit = df_limit[df_limit['limit'] == 'D']
    up_count = len(up_limit)
    down_count = len(down_limit)
    up_down_ratio = up_count / (down_count + 1e-6)

    lianban = up_limit[up_limit['limit_times'] >= 2]
    lianban_avg_pct = lianban['pct_chg'].mean() if not lianban.empty else 0

    # 炸板：Tushare 中可能没有直接 'B' 标记，此处假设无炸板数据则为0
    # 若后续有炸板字段，可替换逻辑
    zha_board_count = 0
    zha_board_rate = zha_board_count / (up_count + zha_board_count + 1e-6)

    # 简化打分（仅用于归一化，不用于最终输出）
    sentiment_score = up_down_ratio * 30 + lianban_avg_pct * 10 + (1 - zha_board_rate) * 20

    return {
        'up_down_ratio': up_down_ratio,
        'lianban_avg_pct': lianban_avg_pct,
        'zha_board_rate': zha_board_rate,
        'high_mark_yield': 0,
        'prev_up_yield': 0,
        'sentiment_score': sentiment_score
    }


def safe_print(text, max_length=1000):
    if len(text) <= max_length:
        print(text)
    else:
        for i in range(0, len(text), max_length):
            print(text[i:i + max_length])


def call_ai_analysis(prompt, analysis_type="指标分析"):
    print(f"\n--- AI {analysis_type} 阶段 ---")
    full_response = ""
    try:
        stream = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
            timeout=60,
            stream=True  # 启用流式输出
        )
        print(f"-> AI {analysis_type} 结果:")
        print(f"【{analysis_type}】")
        sys.stdout.flush()

        for chunk in stream:
            delta = chunk.choices[0].delta
            if delta.content is not None:
                content = delta.content
                full_response += content
                print(content, end='', flush=True)  # 逐字打印，不换行
        print()  # 打印完后换行
        print("-" * 80)
        return full_response.strip()
    except Exception as e:
        error_msg = f"{analysis_type}分析失败，请检查网络连接: {e}"
        print(f"\n-> AI {analysis_type} 失败: {e}")
        print(f"【{analysis_type}】")
        safe_print(error_msg)
        print("-" * 80)
        return error_msg

# ========== AI 分析函数（保持不变） ==========
def analyze_up_down_ratio(up_count, down_count, up_down_ratio, trade_date, historical_context=""):
    prompt = f"""
    作为专业的量化分析师，请深入分析以下涨跌停比数据：

    交易日期：{trade_date}
    涨停股数：{up_count}
    跌停股数：{down_count}
    涨跌停比率：{up_down_ratio:.2f}
    {historical_context}

    请从以下维度进行专业分析：
    1. 多空力量对比
    2. 市场情绪状态
    3. 资金流向
    4. 风险收益特征
    5. 历史比较

    请给出具体的量化判断和投资建议。
    """
    return call_ai_analysis(prompt, "涨跌停比分析")


def analyze_lianban_stocks(lianban_avg_pct, lianban_count, trade_date, historical_context=""):
    prompt = f"""
    作为专业的短线交易分析师，请深入分析以下连板股数据：

    交易日期：{trade_date}
    连板股平均涨幅：{lianban_avg_pct:.2f}%
    连板股数量：{lianban_count}
    {historical_context}

    请从以下维度进行专业分析：
    1. 投机情绪热度
    2. 赚钱效应
    3. 龙头股表现
    4. 风险积聚
    5. 资金接力

    请结合A股市场特点，给出具体的短线交易策略建议。
    """
    return call_ai_analysis(prompt, "连板股分析")


def analyze_zha_board_rate(zha_board_rate, zha_board_count, up_count, trade_date, historical_context=""):
    prompt = f"""
    作为专业的市场微观结构分析师，请深入分析以下炸板率数据：

    交易日期：{trade_date}
    炸板率：{zha_board_rate:.2%}
    炸板股票数量：{zha_board_count}
    涨停股票数量：{up_count}
    {historical_context}

    请从以下维度进行专业分析：
    1. 涨停质量
    2. 资金分歧
    3. 获利了结压力
    4. 次日表现预期
    5. 风险预警

    请给出具体的风险预警和操作建议。
    """
    return call_ai_analysis(prompt, "炸板率分析")


def analyze_high_mark_yield(high_mark_yield, trade_date, historical_context=""):
    prompt = f"""
    作为专业的龙头股策略分析师，请深入分析以下高标股溢价数据：

    交易日期：{trade_date}
    高标股平均溢价：{high_mark_yield:.2%}
    {historical_context}

    请从以下维度进行专业分析：
    1. 龙头股溢价
    2. 风险偏好
    3. 资金聚焦
    4. 情绪传导
    5. 策略有效性

    请结合具体的龙头股交易策略，给出投资建议。
    """
    return call_ai_analysis(prompt, "高标股溢价分析")


def analyze_prev_up_yield(prev_up_yield, trade_date, historical_context=""):
    prompt = f"""
    作为专业的涨停板策略分析师，请深入分析以下昨日涨停溢价数据：

    交易日期：{trade_date}
    昨日涨停今日平均溢价：{prev_up_yield:.2%}
    {historical_context}

    请从以下维度进行专业分析：
    1. 涨停持续性
    2. 资金记忆效应
    3. 接力意愿
    4. 策略风险收益
    5. 市场有效性

    请给出具体的涨停板策略优化建议。
    """
    return call_ai_analysis(prompt, "昨日涨停溢价分析")


def analyze_news_sentiment(news_text, trade_date, market_context=""):
    if not news_text:
        return "无有效新闻数据可供分析"
    prompt = f"""
    作为专业的财经新闻分析师，请深入分析以下新闻文本反映的市场情绪：

    交易日期：{trade_date}
    新闻文本摘要：{news_text}
    {market_context}

    请从以下维度进行专业分析：
    1. 情绪倾向
    2. 热点主题
    3. 风险提示
    4. 政策影响
    5. 资金面信号

    请结合当前市场环境，给出综合的情绪判断。
    """
    return call_ai_analysis(prompt, "新闻情感分析")


def generate_comprehensive_report(individual_analyses, market_data, trade_date):
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

    【报告摘要】
    【市场情绪总览】
    【多空力量分析】
    【投机情绪分析】
    【资金行为分析】
    【风险预警提示】
    【投资策略建议】
    【明日展望】

    报告要求专业、深入、具体，具有实际投资指导价值。
    """
    return call_ai_analysis(prompt, "综合量化报告")


def calculate_zha_board_count(df_daily):
    """
    根据 daily 行情数据计算炸板股数量
    条件：high == 涨停价 且 close < 涨停价
    涨停价 = round(pre_close * 1.1, 2)
    """
    if df_daily.empty:
        return 0

    # 过滤正常交易股票（排除新股、退市等异常）
    df = df_daily.copy()
    df = df[(df['pre_close'] > 0) & (df['high'] > 0)]

    # 计算理论涨停价（简化处理，忽略ST/*ST）
    df['up_limit'] = (df['pre_close'] * 1.1).round(2)

    # 判断是否炸板：最高价触及涨停，但收盘未封住
    zha_board_mask = (df['high'] == df['up_limit']) & (df['close'] < df['up_limit'])
    zha_board_count = zha_board_mask.sum()

    return int(zha_board_count)


def calculate_basic_metrics_from_limit_data(df_limit_info, df_daily):
    metrics = {
        'up_count': 0,
        'down_count': 0,
        'up_down_ratio': 0,
        'lianban_avg_pct': 0,
        'lianban_count': 0,
        'zha_board_count': 0,
        'zha_board_rate': 0.0
    }
    if df_limit_info.empty:
        # 若 limit_list_d 无数据，fallback 到 daily 数据估算涨停数（可选）
        pass
    else:
        metrics['up_count'] = len(df_limit_info[df_limit_info['limit'] == 'U'])
        metrics['down_count'] = len(df_limit_info[df_limit_info['limit'] == 'D'])
        metrics['up_down_ratio'] = metrics['up_count'] / (metrics['down_count'] + 1e-6)

        if 'limit_times' in df_limit_info.columns and 'pct_chg' in df_limit_info.columns:
            lianban_info = df_limit_info[(df_limit_info['limit'] == 'U') & (df_limit_info['limit_times'] >= 2)]
            metrics['lianban_count'] = len(lianban_info)
            if not lianban_info.empty:
                metrics['lianban_avg_pct'] = lianban_info['pct_chg'].mean()

    # === 新增：真实炸板率计算 ===
    zha_board_count = calculate_zha_board_count(df_daily)
    up_count_real = metrics['up_count']  # 或可从 daily 中统计 (close == up_limit)
    total_board_attempts = up_count_real + zha_board_count
    metrics['zha_board_count'] = zha_board_count
    metrics['zha_board_rate'] = zha_board_count / (total_board_attempts + 1e-6)

    return metrics

# ========== 主函数：calc_sentiment_score（整合归一化） ==========
def calc_sentiment_score(trade_date, hist_window=60, news_text=None):
    print(f"\n======== 开始计算 {trade_date} 的市场情绪指数 ========\n")
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
    basic_metrics = calculate_basic_metrics_from_limit_data(df_limit_info, df_daily)

    up_count = basic_metrics['up_count']
    down_count = basic_metrics['down_count']
    up_down_ratio = basic_metrics['up_down_ratio']
    lianban_avg_pct = basic_metrics['lianban_avg_pct']
    lianban_count = basic_metrics['lianban_count']
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
            high_mark_info = df_limit_info[df_limit_info['limit'] == 'U'].sort_values('limit_times', ascending=False).head(3)
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

    idx_chg = df_index['pct_chg'].values[0] if not df_index.empty else 0

    # 获取新闻
    # 基于真实行情数据生成新闻风格摘要（无需外部新闻）
    news_text = (
        f"{trade_date[:4]}年{trade_date[4:6]}月{trade_date[6:]}日，A股市场收盘："
        f"上证指数{'上涨' if idx_chg >= 0 else '下跌'}{abs(idx_chg):.2f}%，"
        f"涨停{up_count}家，跌停{down_count}家，"
        f"连板股{lianban_count}只，炸板率{zha_board_rate:.1%}。"
    )

    # ================================
    # 8. 指标归一化 (以 hist_window 为历史窗口)
    # ================================
    print("\n--- 正在计算历史归一化（用于内部参考）---")
    try:
        start_hist = (datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=hist_window * 2)).strftime('%Y%m%d')
        hist_dates = get_trade_dates(start_hist, trade_date)
        hist_dates = [d for d in hist_dates if d < trade_date][-hist_window:]

        hist_scores = []
        for d in hist_dates:
            try:
                score = calc_sentiment_score_simple(d)
                hist_scores.append(score)
            except Exception as e:
                continue

        def norm(x, arr):
            arr = np.array(arr)
            if len(arr) == 0 or arr.max() == arr.min():
                return 50.0
            return (x - arr.min()) / (arr.max() - arr.min() + 1e-6) * 100

        if hist_scores:
            up_down_score = norm(up_down_ratio, [h['up_down_ratio'] for h in hist_scores])
            lianban_score = norm(lianban_avg_pct, [h['lianban_avg_pct'] for h in hist_scores])
            zha_board_score = 100 - norm(zha_board_rate, [h['zha_board_rate'] for h in hist_scores])
            high_mark_score = norm(high_mark_yield, [h['high_mark_yield'] for h in hist_scores])
            prev_up_score = norm(prev_up_yield, [h['prev_up_yield'] for h in hist_scores])
            print(f"-> 归一化完成（内部参考）：涨跌比{up_down_score:.1f}, 连板{lianban_score:.1f}, 炸板{zha_board_score:.1f}")
        else:
            print("-> 无足够历史数据用于归一化")
    except Exception as e:
        print(f"-> 历史归一化计算失败（不影响主流程）: {e}")

    # ================================
    # AI 多维度分析（保持不变）
    # ================================
    print("\n--- 开始各维度AI分析 ---")
    analyses = {}

    analyses['up_down_analysis'] = analyze_up_down_ratio(up_count, down_count, up_down_ratio, trade_date)
    analyses['lianban_analysis'] = analyze_lianban_stocks(lianban_avg_pct, lianban_count, trade_date)
    analyses['zha_board_analysis'] = analyze_zha_board_rate(zha_board_rate, 0, up_count, trade_date)
    analyses['high_mark_analysis'] = analyze_high_mark_yield(high_mark_yield, trade_date)
    analyses['prev_up_analysis'] = analyze_prev_up_yield(prev_up_yield, trade_date)

    market_context = f"当前市场：涨停{up_count}只，跌停{down_count}只，指数涨跌{idx_chg:.2f}%"
    analyses['news_analysis'] = analyze_news_sentiment(news_text, trade_date, market_context)

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

    individual_analyses_text = "\n\n".join([f"【{key}】\n{value}" for key, value in analyses.items()])
    comprehensive_report = generate_comprehensive_report(individual_analyses_text, market_data, trade_date)

    result = {
        'trade_date': trade_date,
        'comprehensive_report': comprehensive_report,
        'individual_analyses': analyses,
        'market_data': market_data,
        'data_source': 'full_data'
    }

    print("\n" + "=" * 80)
    print("           量化情绪分析报告生成完成！")
    print("=" * 80)
    safe_print(comprehensive_report)
    print("=" * 80)

    return result


def create_fallback_result(trade_date, reason):
    return {
        'trade_date': trade_date,
        'comprehensive_report': f"数据获取失败：{reason}",
        'individual_analyses': {},
        'market_data': {},
        'data_source': 'fallback'
    }


def save_report_to_file(result, filename=None):
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
        print("投资有风险 入市需谨慎 以上内容为AI生成 内容仅供参考,不构成投资建议 ")
        return True
    except Exception as e:
        print(f"-> 保存文件失败: {e}")
        return False


if __name__ == "__main__":
    result = calc_sentiment_score('20250923')
    save_report_to_file(result)

