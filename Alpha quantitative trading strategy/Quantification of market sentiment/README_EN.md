
# Alpha Quantitative Trading Strategy: Market Sentiment Quantification

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/downloads/)

[中文](README_ZH.md) | **English**

This project provides a quantitative system for scoring market sentiment in the A-share market (Chinese stock market). It uses historical and real-time stock data to calculate a sentiment index (0-100 score) based on key indicators like limit-up/down ratios, continuous limit-up stock performance, and more. The system helps traders and investors gauge market emotions, adjust positions dynamically, and manage risks effectively.

The core logic is derived from alpha quantitative strategies, emphasizing data-driven insights into market psychology. This tool is particularly useful for short-term trading, position management, and risk warnings in volatile markets like A-shares.

## Table of Contents

- [Overview](#overview)
- [Strategy Logic and Sources](#strategy-logic-and-sources)
- [Key Sentiment Indicators](#key-sentiment-indicators)
- [Analysis Dimensions](#analysis-dimensions)
- [Data Requirements and Acquisition](#data-requirements-and-acquisition)
- [Calculation Process](#calculation-process)
- [Applicable Scenarios and Practical Use](#applicable-scenarios-and-practical-use)
- [Common Errors and Optimization Suggestions](#common-errors-and-optimization-suggestions)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Output Examples](#output-examples)
- [Contributing](#contributing)
- [License](#license)

## Overview

Market sentiment refers to the collective psychological expectations and risk preferences of market participants regarding future trends. In high-sentiment periods, investors are more risk-tolerant and chase gains; in low-sentiment periods, caution prevails. Traditionally subjective, sentiment can now be quantified using AI, big data, and APIs like Tushare.

This project implements an **Alpha Quantitative Trading Strategy** focused on **Market Sentiment Quantification**. It generates a daily sentiment score (0-100), identifies sentiment cycles (e.g., freezing point, recovery, climax, ebb), and provides divergence warnings (e.g., high sentiment but falling indices). The output includes detailed AI-driven analyses and comprehensive reports, aiding in warehouse management and risk control.

Key benefits:
- **Quantifies Emotions**: Transforms qualitative market mood into actionable scores.
- **Risk Management**: Warns of over-optimism or pessimism.
- **Strategy Integration**: Serves as an input for quantitative selection, timing, and hedging.

## Strategy Logic and Sources

### 1. Essence and Quantitative Significance of Market Sentiment
- **Essence**: Sentiment drives irrational behaviors like bubbles or panics.
- **Quantitative Value**: By modeling indicators, we create a 0-100 score for dynamic position adjustments and risk alerts.
- **Sources**: Inspired by A-share trading patterns, combining Tushare data with AI analysis (e.g., via OpenAI for interpretive reports).

### 2. Core Emotions Index
- **Score Calculation**: Normalize and weight indicators to form a composite score.
- **Cycle Stages**: Based on historical quantiles:
  - Freezing Point (Low): Score < 30 – Oversold, potential bottom.
  - Recovery: Score 30-50 – Warming up, entry opportunities.
  - Climax (High): Score 50-80 – Overheated, reduce positions.
  - Ebb: Score >80 – Fading, exit signals.
- **Divergence Warnings**: E.g., High sentiment but index decline signals potential reversals.

## Key Sentiment Indicators

- **Limit-Up/Down Ratio**: Measures bullish/bearish strength (high ratio = optimistic).
- **Average Return of Continuous Limit-Up Stocks**: Reflects speculative heat.
- **Bomb Board Rate**: High rate indicates weakening chase willingness.
- **High-Standard Stock Premium**: Next-day open premium of top performers.
- **Previous Limit-Up Premium**: Today's return on yesterday's limit-ups, showing continuity.

## Analysis Dimensions

The system analyzes market sentiment from multiple professional dimensions using AI (e.g., GPT-4o) for in-depth interpretations:

1. **Bull-Bear Power Comparison**: Via limit-up/down ratio – Assesses multi-empty forces, fund flows, and risk-reward.
2. **Speculative Emotion Heat**: Via continuous limit-up stocks – Evaluates earning effects, leading stock performance, and risk accumulation.
3. **Fund Disagreement and Quality**: Via bomb board rate – Analyzes limit-up quality, profit-taking pressure, and next-day expectations.
4. **Leading Stock Premium**: Via high-standard yields – Examines risk preferences, fund focus, and emotion transmission.
5. **Limit-Up Continuity**: Via previous limit-up yields – Studies fund memory, relay willingness, and strategy risks.
6. **News Sentiment**: Summarizes market news for overall mood, hotspots, risks, policies, and fund signals.
7. **Comprehensive Report**: Integrates all, providing abstract, overview, multi-empty analysis, speculative insights, fund behavior, risk alerts, strategies, and outlook.

Historical normalization (e.g., 60-day window) ensures scores are contextualized via quantiles.

## Data Requirements and Acquisition

- **Required Data**:
  - Daily stock quotes (gains/losses, opens/closes, limit statuses).
  - Limit-up/down lists, continuous limit-ups, bomb boards.
  - Index data (e.g., SSE Composite).
- **Acquisition**: Uses Tushare Pro API (requires token). Fallbacks for failures. No external news scraping; simulates summaries from data.

## Calculation Process

1. Fetch data (daily quotes, limits, index).
2. Compute basic metrics (ratios, averages, rates).
3. Normalize via historical window.
4. AI Analyses: Individual dimensions + comprehensive report.
5. Output: Score, cycles, warnings, reports.

## Applicable Scenarios and Practical Use

- **Position Management**: High sentiment → Reduce; Low → Increase.
- **Risk Alerts**: Divergences signal systemic risks.
- **Strategy Aid**: Input for stock selection/timing.
- **Markets**: Primarily A-shares; adaptable to HK/US with adjustments.

## Installation

1. Clone the repo:
   ```
   git clone https://github.com/Haohao-end/AI-Agent-Quantitative-Trading.git
   cd alpha-quant-strategy
   ```
2. Install dependencies:
   ```
   pip install -r requirements.txt  # Assuming you add one with: tushare, pandas, numpy, openai, requests, beautifulsoup4
   ```
3. Set environment variables or edit code: TUSHARE_TOKEN, OPENAI_API_KEY, OPENAI_API_BASE.

## Usage

Run the script:
```
python "Quantification of market sentiment.py"
```
- Default date: '20250923' (modifiable in code).
- Outputs: Console logs, Markdown report (`market_sentiment_report.md`).

For custom dates: Edit `calc_sentiment_score('YYYYMMDD')` in `__main__`.

## Project Structure

The project folder `Alpha quantitative trading strategy` contains:

- **[Quantification of market sentiment.py](Quantification%20of%20market%20sentiment.py)**: The main Python source code for sentiment calculation and AI analysis.
- **[README.md](README.md)**: This documentation file.
- **[market_sentiment_report.md](market_sentiment_report.md)**: Generated report from code execution (e.g., for date 20250923).
- **[output result on the console.md](output%20result%20on%20the%20console.md)**: Captured console output from a sample run.

## Output Examples

- **Generated Report**: See [market_sentiment_report.md](market_sentiment_report.md) for a sample comprehensive sentiment report.
- **Console Output**: Detailed logs and AI responses in [output result on the console.md](output%20result%20on%20the%20console.md).

## Contributing

Pull requests welcome! For major changes, open an issue first. Ensure code follows PEP8 and includes tests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details (add one if needed).
