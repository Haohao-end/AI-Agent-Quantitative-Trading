import torch

class ParameterConfig():
    def __init__(self):
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        self.vocab_path = './vocab/vocab.txt'
        self.train_path = 'data/medical_train.pkl'
        self.valid_path = 'data/medical_valid.pkl'
        self.config_json = './config/config.json'
        self.save_model_path = 'save_model1'
        self.pretrained_model = ''
        self.save_samples_path = 'sample'
        self.ignore_index = -100
        self.max_history_len = 1# "dialogue history的最大长度"
        self.max_len = 300  # '每个utterance的最大长度,超过指定长度则进行截断,默认25'
        self.repetition_penalty = 1.0 # "重复惩罚参数，若生成的对话重复性较高，可适当提高该参数"
        self.topk = 4 #'最高k选1。默认8'
        self.batch_size = 4
        self.epochs = 4
        self.loss_step = 1 # 多少步汇报一次loss
        self.lr = 2.6e-5
        #   eps，为了增加数值计算的稳定性而加到分母里的项，其为了防止在实现中除以零
        self.eps = 1.0e-09
        self.max_grad_norm = 2.0
        self.gradient_accumulation_steps = 4
        # 默认.warmup_steps = 4000
        self.warmup_steps = 100 # 使用Warmup预热学习率的方式,即先用最初的小学习率训练，然后每个step增大一点点，直到达到最初设置的比较大的学习率时（注：此时预热学习率完成），采用最初设置的学习率进行训练（注：预热学习率完成后的训练过程，学习率是衰减的），有助于使模型收敛速度变快，效果更佳。


if __name__ == '__main__':
    pc = ParameterConfig()
    print(pc.train_path)
    print(pc.device)

    print(torch.cuda.device_count())
