from torch.utils.data import Dataset  # 导入Dataset模块，用于定义自定义数据集
import torch  # 导入torch模块，用于处理张量和构建神经网络


class MyDataset(Dataset):
    """
    自定义数据集类，继承自Dataset类
    """

    def __init__(self, input_list, max_len):
        """
        初始化函数，用于设置数据集的属性
        :param input_list: 输入列表，包含所有对话的tokenize后的输入序列
        :param max_len: 最大序列长度，用于对输入进行截断或填充
        """
        # print(f'input_list--->{len(input_list)}')
        self.input_list = input_list  # 将输入列表赋值给数据集的input_list属性
        self.max_len = max_len  # 将最大序列长度赋值给数据集的max_len属性

    def __len__(self):
        """
        获取数据集的长度
        :return: 数据集的长度
        """
        return len(self.input_list)  # 返回数据集的长度

    def __getitem__(self, index):
        """
        根据给定索引获取数据集中的一个样本
        :param index: 样本的索引
        :return: 样本的输入序列张量
        """
        input_ids = self.input_list[index]  # 获取给定索引处的输入序列
        input_ids = input_ids[:self.max_len]  # 根据最大序列长度对输入进行截断或填充
        input_ids = torch.tensor(input_ids, dtype=torch.long)  # 将输入序列转换为张量long类型
        return input_ids  # 返回样本的输入序列张量


