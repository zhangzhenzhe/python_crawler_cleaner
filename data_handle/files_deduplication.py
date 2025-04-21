# _*_ coding : utf-8 _*_
# @Time : 2025/4/1 16:55
# @Author : 张振哲
# @File : files_deduplication
# @Project : data_handle

import pandas as pd
import glob


def merge_and_deduplicate_excel():
    # 获取所有xlsx文件（默认扫描当前目录）
    files = glob.glob('*.xlsx')  # 可修改为特定路径如：'your_folder/*.xlsx'

    # 读取并合并数据
    dfs = []
    for file in files:
        df = pd.read_excel(file, engine='openpyxl')
        dfs.append(df)

    if not dfs:
        print("未找到.xlsx文件，请检查路径设置")
        return

    merged_df = pd.concat(dfs, ignore_index=True)

    # 跨文件去重（原始文件内的重复也会被删除）
    final_df = merged_df.drop_duplicates()

    # 设置输出文件名
    final_df.to_excel('merged_result.xlsx', index=False, engine='openpyxl')
    print(f'处理完成！总处理文件数：{len(files)}\n'
          f'合并后总行数：{len(merged_df)}\n'
          f'去重后总行数：{len(final_df)}\n'
          f'结果文件：merged_result.xlsx')


# 执行主函数
if __name__ == '__main__':
    merge_and_deduplicate_excel()

