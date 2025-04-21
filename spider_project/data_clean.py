# _*_ coding : utf-8 _*_
# @Time : 2025/3/31 21:47
# @Author : 张振哲
# @File : data_clean
# @Project : spider_project

import pandas as pd


def remove_duplicate_rows(input_file, output_file):
    """
    读取Excel文件，删除完全重复的行，保存结果到新文件
    :param input_file: 输入文件名（xlsx格式）
    :param output_file: 输出文件名（xlsx格式）
    """
    # 读取Excel文件
    df = pd.read_excel(input_file)

    # 记录原始行数
    original_rows = df.shape[0]

    # 删除完全重复的行（保留第一次出现的行）
    df.drop_duplicates(inplace=True)

    # 保存处理后的数据到新文件
    df.to_excel(output_file, index=False)

    # 统计信息
    removed_rows = original_rows - df.shape[0]
    print(f"处理完成！共删除 {removed_rows} 个重复行")
    print(f"原始行数: {original_rows}，处理后行数: {df.shape[0]}")
    print(f"结果已保存到: {output_file}")


# 使用示例
if __name__ == "__main__":
    input_filename = "house_data.xlsx"  # 请修改为你的输入文件名
    output_filename = "house_cleared.xlsx"  # 请修改为你想要的输出文件名

    remove_duplicate_rows(input_filename, output_filename)

