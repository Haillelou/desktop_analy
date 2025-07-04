import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from adjustText import adjust_text
import locale
import re

def generate_chart_for_feature(data, page_name, feature_name, start_date, output_dir):
    """为指定页面的单个功能生成点击率图表。"""
    # 筛选特定功能的数据
    feature_data = data[(data['页面名称'] == page_name) & (data['功能名称'] == feature_name)].copy()

    if feature_data.empty:
        print(f"功能 '{feature_name}' 在页面 '{page_name}' 没有数据，跳过生成图表。")
        return

    # --- 数据计算 ---
    feature_data['点击uv'] = pd.to_numeric(feature_data['点击uv'], errors='coerce')
    feature_data['曝光uv'] = pd.to_numeric(feature_data['曝光uv'], errors='coerce')
    feature_data['点击率'] = (feature_data['点击uv'] / feature_data['曝光uv'] * 100).fillna(0)
    feature_data = feature_data.sort_values('日期')

    # --- 绘制图表 ---
    fig, ax = plt.subplots(figsize=(20, 10))
    ax.plot(feature_data['日期'], feature_data['点击率'], marker='o', linestyle='-', color='tab:blue')

    texts = []
    for _, row in feature_data.iterrows():
        label = f"{row['点击率']:.2f}%\n({int(row['点击uv'])}/{int(row['曝光uv'])})"
        texts.append(ax.text(row['日期'], row['点击率'], label, ha='center', va='bottom', fontsize=9))
    
    adjust_text(texts, arrowprops=dict(arrowstyle='->', color='gray', lw=0.5))

    # --- 图表美化 ---
    ax.set_title(f'“{page_name}” - “{feature_name}”功能每日点击率 (自{start_date}起)', fontsize=16)
    ax.set_xlabel('日期', fontsize=12)
    ax.set_ylabel('点击率 (%)', fontsize=12)
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %a'))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()

    # --- 保存图表 ---
    # 清理文件名，移除无效字符
    safe_feature_name = re.sub(r'[\/:]', '-', feature_name)
    output_filename = f"{safe_feature_name}_ctr_chart.png"
    output_path = os.path.join(output_dir, output_filename)
    plt.savefig(output_path, dpi=300)
    plt.close(fig) # 关闭图表，释放内存
    print(f"图表已保存至：{output_path}")

# --- 主逻辑 ---
if __name__ == "__main__":
    # --- 配置 ---
    try:
        locale.setlocale(locale.LC_TIME, 'zh_CN.UTF-8')
    except locale.Error:
        print("Locale 'zh_CN.UTF-8' not supported, using default.")
    
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False

    # --- 数据加载和预处理 ---
    file_path = '/Users/haillelou/Documents/桌面端改版/数据复盘/featheruse/全功能点击率明细.csv'
    data = pd.read_csv(file_path)
    data['日期'] = pd.to_datetime(data['日期'], format='%Y/%m/%d')
    
    start_date = '2025-05-26'
    data = data[data['日期'] >= start_date]

    # --- 批量生成图表配置 ---
    pages_to_process = [
        {
            "name": "我的课程页",
            "output_dir": "/Users/haillelou/Documents/桌面端改版/数据复盘/featheruse/图表/我的课程页图表"
        },
        {
            "name": "班课主页",
            "output_dir": "/Users/haillelou/Documents/桌面端改版/数据复盘/featheruse/图表/班课主页图表"
        }
    ]

    # --- 循环处理每个页面 ---
    for page_config in pages_to_process:
        target_page_name = page_config["name"]
        output_directory = page_config["output_dir"]
        os.makedirs(output_directory, exist_ok=True)

        # 获取该页面的所有独立功能名称
        page_data = data[data['页面名称'] == target_page_name]
        features_to_plot = page_data['功能名称'].unique()

        if len(features_to_plot) == 0:
            print(f"页面 '{target_page_name}' 没有找到任何功能数据，跳过。")
            continue

        print(f"\n--- 开始为“{target_page_name}”的 {len(features_to_plot)} 个功能生成图表 ---")
        for feature in features_to_plot:
            generate_chart_for_feature(data, target_page_name, feature, start_date, output_directory)
        print(f"--- “{target_page_name}”的图表已全部生成 ---")

    print("\n所有页面的图表均已生成完毕。")