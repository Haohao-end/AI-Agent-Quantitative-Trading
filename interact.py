import os
from datetime import datetime
from transformers import GPT2LMHeadModel
from transformers import BertTokenizerFast
import torch.nn.functional as F
from parameter_config import *

PAD = '[PAD]'
pad_id = 0


def top_k_top_p_filtering(logits, top_k=0, filter_value=-float('Inf')):
    """
    使用top-k和/或nucleus（top-p）筛选来过滤logits的分布
        参数:
            logits: logits的分布，形状为（词汇大小）
            top_k > 0: 保留概率最高的top k个标记（top-k筛选）。
            top_p > 0.0: 保留累积概率大于等于top_p的top标记（nucleus筛选）。

    """
    assert logits.dim() == 1  # batch size 1 for now - could be updated for more but the code would be less clear
    top_k = min(top_k, logits.size(-1))  # Safety check：确保top_k不超过logits的最后一个维度大小

    if top_k > 0:
        # 移除概率小于top-k中的最后一个标记的所有标记
        # torch.topk()返回最后一维中最大的top_k个元素，返回值为二维(values, indices)
        # ...表示其他维度由计算机自行推断
        # print(f'torch.topk(logits, top_k)--->{torch.topk(logits, top_k)}')
        # print(f'torch.topk(logits, top_k)[0]-->{torch.topk(logits, top_k)[0]}')
        # print(f'torch.topk(logits, top_k)[0][..., -1, None]-->{torch.topk(logits, top_k)[0][..., -1, None]}')
        # print(f'torch.topk(logits, top_k)[0][-1]-->{torch.topk(logits, top_k)[0][-1]}')
        indices_to_remove = logits < torch.topk(logits, top_k)[0][..., -1, None]
        # print(f'indices_to_remove--->{indices_to_remove}')
        logits[indices_to_remove] = filter_value  # 对于topk之外的其他元素的logits值设为负无穷
        # print(f'logits--->{logits}')
    return logits


def main():
    pconf = ParameterConfig()
    # 当用户使用GPU,并且GPU可用时
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    print('using device:{}'.format(device))
    os.environ["CUDA_VISIBLE_DEVICES"] = '0'
    tokenizer = BertTokenizerFast(vocab_file=pconf.vocab_path,
                                  sep_token="[SEP]",
                                  pad_token="[PAD]",
                                  cls_token="[CLS]")
    model = GPT2LMHeadModel.from_pretrained('./save_model/epoch97')
    model = model.to(device)
    model.eval()
    history = []
    print('开始和chatbot聊天')

    while True:
        try:
            text = input("user:")
            text_ids = tokenizer.encode(text, add_special_tokens=False)
            # print(f'text_ids---》{text_ids}')
            history.append(text_ids)
            # print(f'history--->{history}')
            input_ids = [tokenizer.cls_token_id]  # 每个input以[CLS]为开头
            # pconf.max_history_len=
            for history_id, history_utr in enumerate(history[-pconf.max_history_len:]):
                # print(f'history_utr--->{history_utr}')
                input_ids.extend(history_utr)
                input_ids.append(tokenizer.sep_token_id)
                # print(f'input_ids---》{input_ids}')
            # print(f'历史对话结束--》{input_ids}')
            input_ids = torch.tensor(input_ids).long().to(device)
            input_ids = input_ids.unsqueeze(0)
            # print(f'符合模型的输入--》{input_ids.shape}')
            response = []  # 根据context，生成的response
            # 最多生成max_len个token：35
            for _ in range(pconf.max_len):
                # print(f'input_ids-->{input_ids}')
                outputs = model(input_ids=input_ids)
                logits = outputs.logits
                # print(f'logits---》{logits.shape}')
                # next_token_logits生成下一个单词的概率值
                next_token_logits = logits[0, -1, :]
                # print(f'next_token_logits----》{next_token_logits.shape}')

                # 对于已生成的结果generated中的每个token添加一个重复惩罚项，降低其生成概率
                # print(f'set(response)-->{set(response)}')

                for id in set(response):
                    # print(f'id--->{id}')
                    next_token_logits[id] /= pconf.repetition_penalty
                # 对于[UNK]的概率设为无穷小，也就是说模型的预测结果不可能是[UNK]这个token
                next_token_logits[tokenizer.convert_tokens_to_ids('[UNK]')] = -float('Inf')
                filtered_logits = top_k_top_p_filtering(next_token_logits, top_k=pconf.topk)

                # torch.multinomial表示从候选集合中无放回地进行抽取num_samples个元素，权重越高，抽到的几率越高，返回元素的下标
                next_token = torch.multinomial(F.softmax(filtered_logits, dim=-1), num_samples=1)
                if next_token == tokenizer.sep_token_id:  # 遇到[SEP]则表明response生成结束
                    break
                response.append(next_token.item())
                input_ids = torch.cat((input_ids, next_token.unsqueeze(0)), dim=1)
                # his_text = tokenizer.convert_ids_to_tokens(curr_input_tensor.tolist())
            history.append(response)
            text = tokenizer.convert_ids_to_tokens(response)
            print("chatbot:" + "".join(text))
        except KeyboardInterrupt:
            break


if __name__ == '__main__':
    main()
