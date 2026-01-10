import torch
import torch.nn.functional as F


def caculate_loss(logit, target, pad_idx, smoothing=True):
    if smoothing:
        logit = logit[..., :-1, :].contiguous().view(-1, logit.size(2))
        target = target[..., 1:].contiguous().view(-1)

        eps = 0.1
        n_class = logit.size(-1)
        #
        one_hot = torch.zeros_like(logit).scatter(1, target.view(-1, 1), 1)
        one_hot = one_hot * (1 - eps) + (1 - one_hot) * eps / (n_class - 1)
        log_prb = F.log_softmax(logit, dim=1)

        non_pad_mask = target.ne(pad_idx)
        loss = -(one_hot * log_prb).sum(dim=1)
        loss = loss.masked_select(non_pad_mask).mean()  # average later
    else:
        # loss = F.cross_entropy(predict_logit, target, ignore_index=pad_idx)
        logit = logit[..., :-1, :].contiguous().view(-1, logit.size(-1))
        labels = target[..., 1:].contiguous().view(-1)
        loss = F.cross_entropy(logit, labels, ignore_index=pad_idx)
    return loss


def calculate_acc(logit, labels, ignore_index=-100):
    # print(f'logit--->原始值的形状{logit.shape}')
    # print(f'labels--->原始值的形状{labels.shape}')
    # print(f' logit.size---{logit.size(-1)}')
    # print(f' logit[:, :-1, :]---{logit[:, :-1, :].shape}')
    logit = logit[:, :-1, :].contiguous().view(-1, logit.size(-1))
    # print(f'logit--->{logit.shape}')
    # print(f'labels[:, 1:]--->{labels[:, 1:].shape}')
    labels = labels[:, 1:].contiguous().view(-1)
    # print(f'labels--->{labels.shape}')
    _, logit = logit.max(dim=-1)  # 对于每条数据，返回最大的index
    # print(f'_-->{_}')
    # print(f'logit111-->{logit}')
    # print(f'logit111---》{logit.shape}')
    '''
    在 PyTorch 中，labels.ne(ignore_index) 表示将标签张量 labels 中的值不等于 ignore_index 的位置标记为 True，等于 ignore_index 的位置标记为 False。
    这个操作，以过滤掉 ignore_index 对损失的贡献
    '''
    # 进行非运算，返回一个tensor，若labels的第i个位置为pad_id，则置为0，否则为1
    non_pad_mask = labels.ne(ignore_index)
    # print(f'non_pad_mask-->{non_pad_mask}')
    '''
    在 PyTorch 中，logit.eq(labels) 表示将模型的预测输出值 logit 中等于标签张量 labels 的位置标记为 True，
    不等于标签张量 labels 的位置标记为 False。以标记出预测输出值和标签值相等的位置。
    masked_select(non_pad_mask) 表示将张量中非填充标记的位置选出来。
    '''

    # print(f'logit.eq(labels)--->{logit.eq(labels)}')
    # print(f'logit.eq(labels)--->{logit.eq(labels).shape}')
    n_correct = logit.eq(labels).masked_select(non_pad_mask).sum().item()
    # print(f'n_correct-->{n_correct}')

    n_word = non_pad_mask.sum().item()
    # print(f'non_pad_mask.sum()-->{non_pad_mask.sum()}')

    return n_correct, n_word
