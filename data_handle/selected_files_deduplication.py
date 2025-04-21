import pandas as pd
import os
import shlex


def merge_and_deduplicate_excel():
    # 获取用户输入的文件列表
    file_input = input("请输入要处理的Excel文件（支持多个文件，用空格分隔）：\n").strip()

    if not file_input:
        print("未输入任何文件名！")
        return

    try:
        raw_files = shlex.split(file_input)
    except ValueError as e:
        print(f"输入解析失败：{e}")
        return

    # 验证文件有效性
    valid_files = []
    for f in raw_files:
        if not f.endswith('.xlsx'):
            print(f"跳过非xlsx文件：{f}")
            continue
        if os.path.isfile(f):
            valid_files.append(f)
        else:
            print(f"文件不存在：{f}")

    if not valid_files:
        print("没有有效的xlsx文件可处理！")
        return

    # 读取所有数据（无表头模式）
    dfs = []
    for file in valid_files:
        try:
            # 关键修改：添加 header=None 参数
            df = pd.read_excel(file, engine='openpyxl', header=None)
            dfs.append(df)
            print(f"成功读取：{file}（{len(df)}行）")
        except Exception as e:
            print(f"读取失败【{file}】：{str(e)}")

    if not dfs:
        print("所有文件读取失败")
        return

    merged_df = pd.concat(dfs, ignore_index=True)

    # 关键修改：去重时保持所有列相同
    original_count = len(merged_df)
    final_df = merged_df.drop_duplicates(keep='first')

    # 保存结果（不保留列名）
    output_file = "house_merged_result.xlsx"
    final_df.to_excel(output_file, index=False, header=False, engine='openpyxl')

    print("\n处理结果：")
    print(f"输入行总计：{original_count}")
    print(f"去重后行数：{len(final_df)}")
    print(f"生成文件：{output_file}")


if __name__ == '__main__':
    merge_and_deduplicate_excel()

# house_data_0.xlsx house_data_1.xlsx house_data_2.xlsx house_data_3.xlsx house_data_4.xlsx house_data_5.xlsx house_data_6.xlsx house_data_7.xlsx
