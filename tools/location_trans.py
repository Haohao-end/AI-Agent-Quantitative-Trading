
def transform_location(chinese_city):
    # 中文到英文的城市名映射表
    city_dict = {
        '北京': 'Beijing',
        '上海': 'Shanghai',
        '广州': 'Guangzhou',
        '深圳': 'Shenzhen',
        '成都': 'Chengdu',
        '杭州': 'Hangzhou',
        '巴塞尔': 'Basel',
        '苏黎世': 'Zurich',
        # 添加更多的城市映射...
    }

    # Check if the input is in Chinese
    if all('\u4e00' <= char <= '\u9fff' for char in chinese_city):
        return city_dict.get(chinese_city, "城市名称未找到")
    else:
        return chinese_city