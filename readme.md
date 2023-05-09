## 将file_path修改成指定的电梯清单.xlsx路径
通过key:word的形式添加需要修改的物模型（物模型需可读写）
而对于excel表格，必须将cpdid内容放在表格的k列，也就是第十列，这样才能达到批量修改参数的目的

代码中的productkey和items分别对应产品和物模型

所有接口函数传入的cpdid设备列表元素均是**str类型**