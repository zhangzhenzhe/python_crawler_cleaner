import pandas as pd
import re


# 定义一个函数来处理“房子信息”列
def process_house_info(row):
    # 定义关键字和对应的列名
    keys = ["布局", "面积", "朝向", "装潢", "楼层", "年份", "材质"]
    # 初始化一个字典来存储结果
    result = {key: "" for key in keys}

    # 如果“房子信息”列为空，直接返回空字典
    if pd.isna(row["房子信息"]):
        return result

    # 按“|”分割“房子信息”列
    parts = row["房子信息"].split("|")
    for part in parts:
        part = part.strip()
        # 检查每个部分属于哪个类别
        if re.search(r"\d室\d厅", part):
            result["布局"] = part
        elif re.search(r"\d+\.\d+平米", part):
            result["面积"] = part
        elif re.search(r"[东西南北]", part):
            result["朝向"] = part
        elif part in ["毛坯", "简装", "精装"]:
            result["装潢"] = part
        elif re.search(r"层", part):
            result["楼层"] = part
        elif re.search(r"\d+年", part):
            result["年份"] = part
        elif part in ["板楼", "塔楼", "板塔结合"]:
            result["材质"] = part
    return result


# 定义一个函数来处理“关注人数/发布时间”列
def process_attention_time(row):
    if pd.isna(row["关注人数和发布时间"]):
        return {"关注人数": "", "发布时间": ""}
    parts = row["关注人数和发布时间"].split(" / ")
    if len(parts) == 2:
        return {"关注人数": parts[0], "发布时间": parts[1]}
    else:
        return {"关注人数": "", "发布时间": ""}


# 定义一个函数来处理“标签”列
def process_tags(row):
    # 初始化结果字典
    result = {
        "是否近地铁": "",
        "VR房源": "",
        "VR看装修": "",
        "房本年限": "",
        "是否随时看房": ""
    }

    if pd.isna(row["标签"]):
        return result

    # 检查每个标签并填入对应列
    if "近地铁" in row["标签"]:
        result["是否近地铁"] = "近地铁"

    if "VR房源" in row["标签"]:
        result["VR房源"] = "VR房源"

    if "VR看装修" in row["标签"]:
        result["VR看装修"] = "VR看装修"

    # 检查是否包含“房本满x年”（其中x是汉字或数字）
    pattern = r"房本满[\d\u4e00-\u9fa5]+年"
    match = re.search(pattern, row["标签"])
    if match:
        result["房本年限"] = match.group()

    if "随时看房" in row["标签"]:
        result["是否随时看房"] = "随时看房"

    return result


# 读取Excel文件
def process_excel(file_path, output_path):
    df = pd.read_excel(file_path)

    # 处理“房子信息”列
    house_info_df = df.apply(process_house_info, axis=1, result_type="expand")
    house_info_df.columns = ["布局", "面积", "朝向", "装潢", "楼层", "年份", "材质"]

    # 处理“关注人数/发布时间”列
    attention_time_df = df.apply(process_attention_time, axis=1, result_type="expand")
    attention_time_df.columns = ["关注人数", "发布时间"]

    # 处理“标签”列
    tags_df = df.apply(process_tags, axis=1, result_type="expand")
    tags_df.columns = ["是否近地铁", "VR房源", "VR看装修", "房本年限", "是否随时看房"]

    # 合并处理后的数据
    result_df = pd.concat([df, house_info_df, attention_time_df, tags_df], axis=1)
    result_df.drop(columns=["房子信息", "关注人数和发布时间", "标签"], inplace=True)

    # 保存到新的Excel文件
    result_df.to_excel(output_path, index=False)


# 调用函数处理文件
process_excel("house_merged_result.xlsx", "data_divided_result.xlsx")