import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import re
import locale
from adjustText import adjust_text

def get_chinese_font_name():
    """Find an available Chinese font on the system and return its name."""
    font_names = ['PingFang HK', 'Heiti TC', 'SimHei', 'Arial Unicode MS']
    for name in font_names:
        try:
            plt.figure(figsize=(1,1))
            plt.text(0.5, 0.5, '测试', fontproperties=name)
            plt.close()
            print(f"找到并设置中文字体: {name}")
            return name
        except Exception:
            continue
    print("警告: 未找到任何指定的中文字体. 将使用默认西文字体。")
    return 'sans-serif'

# --- Global Settings ---
try:
    locale.setlocale(locale.LC_TIME, 'zh_CN.UTF-8')
except locale.Error:
    print("Locale 'zh_CN.UTF-8' not supported, using default.")

plt.rcParams['font.sans-serif'] = [get_chinese_font_name()]
plt.rcParams['axes.unicode_minus'] = False

# --- File Paths ---
DATA_FILE = '/Users/haillelou/Documents/桌面端改版/数据复盘/featheruse/desktop_analy/功能详细使用情况/功能详细使用情况.csv'
MY_COURSE_PAGE_OUTPUT_DIR = '/Users/haillelou/Documents/桌面端改版/数据复盘/featheruse/desktop_analy/图表/我的课程页图表'
SEASON_PAGE_OUTPUT_DIR = '/Users/haillelou/Documents/桌面端改版/数据复盘/featheruse/desktop_analy/图表/班课主页图表'

# Ensure output directories exist
for path in [MY_COURSE_PAGE_OUTPUT_DIR, SEASON_PAGE_OUTPUT_DIR]:
    if not os.path.exists(path):
        os.makedirs(path)

def generate_multiline_ctr_chart(data, page_name, feature_name, output_dir):
    """Generates a daily CTR line chart with multiple lines for each sub-feature."""
    feature_df = data[(data['页面名称'] == page_name) & (data['功能名称'] == feature_name)].copy()

    if feature_df.empty:
        print(f"功能 '{feature_name}' 在页面 '{page_name}' 没有数据，跳过。")
        return

    fig, ax = plt.subplots(figsize=(18, 10))
    
    # Use a color cycle for different lines
    colors = plt.cm.viridis(np.linspace(0, 1, feature_df['细分'].nunique()))
    
    texts = []
    for i, (sub_feature, group) in enumerate(feature_df.groupby('细分')):
        group = group.sort_values('日期')
        ax.plot(group['日期'], group['点击率(%)'], marker='o', linestyle='-', label=sub_feature, color=colors[i])
        for _, row in group.iterrows():
            label = f"{row['点击率(%)']:.2f}%"
            texts.append(ax.text(row['日期'], row['点击率(%)'], label, ha='center', va='bottom', fontsize=9))

    adjust_text(
        texts, 
        force_points=(0.2, 0.5), 
        force_text=(0.4, 0.8), 
        expand_points=(1.5, 1.5),
        arrowprops=dict(arrowstyle='->', color='grey', lw=0.5)
    )

    ax.set_title(f'“{page_name}” - “{feature_name}” 功能每日点击率', fontsize=18, pad=20)
    ax.set_xlabel('日期', fontsize=14)
    ax.set_ylabel('点击率 (%)', fontsize=14)
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    # Add a legend
    ax.legend(title='细分功能', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    plt.tight_layout(rect=[0, 0, 0.85, 1]) # Adjust layout to make room for legend

    safe_feature_name = re.sub(r'[\/:]', '-', feature_name)
    output_filename = f"{safe_feature_name}_daily_ctr_chart.png"
    output_path = os.path.join(output_dir, output_filename)
    plt.savefig(output_path, dpi=300)
    plt.close(fig)
    print(f"图表已保存: {output_path}")

def main():
    """Main function to load data and generate all charts.""" 
    try:
        df = pd.read_csv(DATA_FILE)
    except FileNotFoundError:
        print(f"错误: 数据文件未找到 at {DATA_FILE}")
        return

    df['日期'] = pd.to_datetime(df['日期'])
    if df['点击率(%)'].dtype == 'object':
        df['点击率(%)'] = df['点击率(%)'].str.replace('%', '', regex=False).astype(float)

    pages_to_process = [
        {"name": "我的课程页", "output_dir": MY_COURSE_PAGE_OUTPUT_DIR},
        {"name": "班课主页", "output_dir": SEASON_PAGE_OUTPUT_DIR}
    ]

    for page in pages_to_process:
        page_name = page['name']
        output_dir = page['output_dir']
        
        page_df = df[df['页面名称'] == page_name]
        features = page_df['功能名称'].unique()

        if len(features) == 0:
            print(f"页面 '{page_name}' 未找到任何功能数据.")
            continue

        print(f"\n--- 开始为“{page_name}”的 {len(features)} 个功能生成图表 ---")
        for feature in features:
            generate_multiline_ctr_chart(df, page_name, feature, output_dir)
        print(f"--- “{page_name}”的图表已全部生成 ---")

    print("\n所有图表生成完毕。")

if __name__ == '__main__':
    main()