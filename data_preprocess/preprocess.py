from transformers import BertTokenizerFast # 分词工具
import pickle # 保存pkl文件的命令
from tqdm import tqdm # 加在进度条
import os


def data_preprocess(train_txt_path, train_pkl_path):
    """
    对原始语料进行tokenize，将每段对话处理成如下形式："[CLS]utterance1[SEP]utterance2[SEP]utterance3[SEP]"
    """

    # 初始化tokenizer，使用BertTokenizerFast从预训练的中文Bert模型（bert-base-chinese）创建一个tokenizer对象
    # tokenizer = BertTokenizerFast.from_pretrained('/Users/ligang/PycharmProjects/llm/prompt_tasks/bert-base-chinese',
    #                                               sep_token="[SEP]",
    #                                               pad_token="[PAD]",
    #                                               cls_token="[CLS]")
    # tokenizer = BertTokenizerFast.from_pretrained('/Users/ligang/PycharmProjects/llm/prompt_tasks/bert-base-chinese')
    tokenizer = BertTokenizerFast('/Users/ligang/PycharmProjects/llm/Gpt2_Chatbot/vocab/vocab.txt',
                                  sep_token="[SEP]",
                                  pad_token="[PAD]",
                                  cls_token="[CLS]")


    print(f'tokenizer.vocab_size-->{tokenizer.vocab_size}')

    sep_id = tokenizer.sep_token_id  # 获取分隔符[SEP]的token ID
    cls_id = tokenizer.cls_token_id  # 获取起始符[CLS]的token ID
    print(f'sep_id-->{sep_id}')
    print(f'cls_id-->{cls_id}')
    #

    # 读取训练数据集
    with open(train_txt_path, 'rb') as f:
        data = f.read().decode("utf-8")  # 以UTF-8编码读取文件内容
    # print(data)
    # # # 根据换行符区分不同的对话段落，需要区分Windows和Linux\mac环境下的换行符
    if "\r\n" in data:
        train_data = data.split("\r\n\r\n")
    else:
        train_data = data.split("\n\n")
    #
    print(len(train_data))  # 打印对话段落数量
    # print(train_data[:1])
    # # # 开始进行tokenize
    # # # 保存所有的对话数据,每条数据的格式为："[CLS]seq1[SEP]seq2[SEP]seq3[SEP]"
    dialogue_len = []  # 记录所有对话tokenize分词之后的长度，用于统计中位数与均值
    dialogue_list = []  # 记录所有对话
    # # #
    for index, dialogue in enumerate(tqdm(train_data)):
        # print(f'dialogue-->{dialogue}')
        if "\r\n" in dialogue:
            sequences = dialogue.split("\r\n")
        else:
            sequences = dialogue.split("\n")
        # print(f'sequences--》{sequences}')
    #
        input_ids = [cls_id]  # 每个dialogue以[CLS]seq1[sep]seq2[sep]开头
        for sequence in sequences:
            # print(f'sequence-->{sequence}')
            # print(f'tokenizer.encode(sequence, add_special_tokens=False)-->{tokenizer.encode(sequence, add_special_tokens=False)}')
            # print(f'tokenizer.encode(sequence)-->{tokenizer.encode(sequence)}')
            # break
            input_ids += tokenizer.encode(sequence, add_special_tokens=False)  # 将每个对话句子进行tokenize，并将结果拼接到input_ids列表中
            # input_ids += tokenizer.encode(sequence)  # 将每个对话句子进行tokenize，并将结果拼接到input_ids列表中
            input_ids.append(sep_id)  # 每个seq之后添加[SEP]，表示seqs会话结束

        # print(f'input_ids-->{input_ids}')
        # break
    # #
        dialogue_len.append(len(input_ids))  # 将对话的tokenize后的长度添加到对话长度列表中
        dialogue_list.append(input_ids)  # 将tokenize后的对话添加到对话列表中
    # #
    print(f'dialogue_len--->{dialogue_len}')  # 打印对话长度列表
    print(f'dialogue_list--->{dialogue_list[:2]}')  # 打印
    # # #
    # # # 保存数据
    with open(train_pkl_path, "wb") as f:
        pickle.dump(dialogue_list, f)



if __name__ == '__main__':
    train_txt_path = '../data/medical_train.txt'
    train_pkl_path = '../data/medical_train.pkl'
    data_preprocess(train_txt_path, train_pkl_path)
