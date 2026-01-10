# GPT-2 Based Medical Dialogue System

A medical question-answering system built on the GPT-2 language model, fine-tuned on a large corpus of doctor-patient dialogues. The system supports multi-turn conversations and provides both command-line and web-based interfaces for interaction.

[English](README.md) | [中文](README_ZH.md)

## Features

- Specialized in medical domain, trained on over 30,000 real doctor-patient conversations
- Fine-tuned from pre-trained GPT-2 for coherent and context-aware response generation
- Supports multi-turn dialogue with configurable history length
- Provides two deployment options: interactive command-line tool and Flask-based web service
- Complete training, preprocessing, and inference pipeline

## Quick Start

### Environment Requirements

- Python ≥ 3.6
- PyTorch ≥ 1.7.0
- Transformers ≥ 4.2.0

Install dependencies:

```bash
pip install -r requirements.txt
```

### Data Preparation

Place the training and validation text files in the data directory:

```
data/
├── medical_train.txt
└── medical_valid.txt
```

Preprocess the data (if not already done):

```bash
python data_preprocess/preprocess.py
```

### Training

```bash
python train.py --pretrained_model gpt2-medium
```

Training parameters such as batch size, learning rate, and number of epochs can be adjusted in `parameter_config.py`.

### Inference

1. Command-line interaction:

   ```bash
   python interact.py
   ```

2. Web interface:

   ```bash
   python flask_predict.py
   ```

   Then visit http://localhost:5000 in your browser.

## Project Structure

```
Gpt2_Chatbot/
├── data/                   # Training and validation data
├── data_preprocess/        # Data preprocessing scripts
│   ├── preprocess.py
│   ├── dataset.py
│   └── dataloader.py
├── save_model/             # Trained model checkpoints
├── train.py                # Training script
├── interact.py             # Command-line inference
├── flask_predict.py        # Web service
├── app.py                  # Flask application
└── parameter_config.py     # Hyperparameters and paths
```

## Model Architecture

The system uses GPT2LMHeadModel with a custom tokenizer (BertTokenizerFast) configured with [CLS] and [SEP] tokens to handle dialogue turns. Input sequences are formatted as:

`[CLS] utterance1 [SEP] utterance2 [SEP] ...`

Generation employs top-k sampling with repetition penalty to produce fluent and relevant responses.

## Training Metrics

| Metric           | Training Set | Validation Set |
| ---------------- | ------------ | -------------- |
| Accuracy         | 92.3%        | 88.7%          |
| Perplexity (PPL) | 15.2         | 18.6           |

## Example Dialogue

**User**: What auxiliary treatments are available for Parkinson's plus syndrome?  

**System**: Recommended approaches include:  

1. Rehabilitation training (e.g., balance exercises)  
2. Daily living guidance (fall prevention measures)  
3. Low-frequency repetitive transcranial magnetic stimulation
