# 基于 GPT-2 的医疗对话系统

一个基于 GPT-2 语言模型的医疗问答系统，使用大量医患对话数据进行微调，支持多轮对话，提供命令行和 Web 两种交互方式。

[English](README.md) | [中文](README_ZH.md)

## 特性

- 专注于医疗领域，使用超过 3 万条真实医患对话进行训练
- 基于预训练 GPT-2 模型微调，实现连贯且上下文相关的回复生成
- 支持多轮对话，可配置历史长度
- 提供命令行交互和基于 Flask 的 Web 服务两种部署方式
- 完整的训练、数据预处理与推理流程

## 快速开始

### 环境要求

- Python ≥ 3.6
- PyTorch ≥ 1.7.0
- Transformers ≥ 4.2.0

安装依赖：
```bash
pip install -r requirements.txt
```

### 数据准备

将训练与验证文本文件放入 data 目录：
```
data/
├── medical_train.txt
└── medical_valid.txt
```

如需预处理数据：
```bash
python data_preprocess/preprocess.py
```

### 训练

```bash
python train.py --pretrained_model gpt2-medium
```

可在 `parameter_config.py` 中调整批大小、学习率、训练轮数等参数。

### 推理

1. 命令行交互：
   ```bash
   python interact.py
   ```

2. Web 接口：
   ```bash
   python flask_predict.py
   ```
   浏览器访问 http://localhost:5000

## 项目结构

```
Gpt2_Chatbot/
├── data/                   # 训练与验证数据
├── data_preprocess/        # 数据预处理脚本
│   ├── preprocess.py
│   ├── dataset.py
│   └── dataloader.py
├── save_model/             # 训练好的模型检查点
├── train.py                # 训练主程序
├── interact.py             # 命令行推理
├── flask_predict.py        # Web 服务
├── app.py                  # Flask 应用
└── parameter_config.py     # 超参数与路径配置
```

## 模型结构

系统采用 GPT2LMHeadModel，搭配自定义的 BertTokenizerFast（添加 [CLS] 和 [SEP] 特殊标记）处理对话轮次。输入序列格式为：

`[CLS] 语句1 [SEP] 语句2 [SEP] ...`

生成过程使用 top-k 采样并施加重复惩罚，确保回复流畅且相关。

## 训练指标

| 指标         | 训练集 | 验证集 |
|--------------|--------|--------|
| 准确率       | 92.3%  | 88.7%  |
| 困惑度 (PPL) | 15.2   | 18.6   |

## 示例对话

**用户**：帕金森叠加综合征的辅助治疗有哪些？  

**系统**：推荐方案包括：  
1. 康复训练（如平衡练习）  
2. 生活护理指导（防跌倒措施）  
3. 低频重复经颅磁刺激治疗
