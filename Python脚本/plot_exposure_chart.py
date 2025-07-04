import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os

def plot_exposure_chart(data, page_name, output_path):
    """
    为指定页面绘制每日曝光量图表并保存。
    """
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
    plt.rcParams['axes.unicode_minus'] = False

    # 筛选数据
    page_data = data[data['页面名称'] == page_name].sort_values('日期')

    # 创建图表
    fig, ax = plt.subplots(figsize=(20, 8))

    # 绘制折线
    ax.plot(page_data['日期'], page_data['曝光uv'], marker='o', linestyle='-')

    # 在每个数据点上标注数值
    for i, row in page_data.iterrows():
        ax.text(row['日期'], row['曝光uv'], f'{int(row["曝光uv"])}', ha='center', va='bottom')

    # 设置标题和标签
    ax.set_title(f'“{page_name}”每日曝光UV', fontsize=20)
    ax.set_xlabel('日期', fontsize=14)
    ax.set_ylabel('曝光UV', fontsize=14)

    # 设置X轴，确保每天都有刻度
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax.get_xticklabels(), rotation=90, ha="right")

    # 显示网格
    ax.grid(True, linestyle='--', alpha=0.6)

    # 自动调整布局
    plt.tight_layout()

    # 保存图表
    plt.savefig(output_path, dpi=300)
    plt.close(fig) # 关闭图表，释放内存
    print(f'图表已保存至：{output_path}')

# --- 主逻辑 ---
# 读取CSV数据
file_path = '/Users/haillelou/Documents/桌面端改版/数据复盘/featheruse/全功能点击率明细.csv'
data = pd.read_csv(file_path)

# 数据预处理
data['日期'] = pd.to_datetime(data['日期'], format='%Y/%m/%d')
daily_exposures = data[['日期', '页面名称', '曝光uv']].drop_duplicates()

# 创建输出目录
output_dir = '/Users/haillelou/Documents/桌面端改版/数据复盘/featheruse/图表'
os.makedirs(output_dir, exist_ok=True)

# 为“我的课程页”生成图表
plot_exposure_chart(
    daily_exposures,
    '我的课程页',
    os.path.join(output_dir, 'my_course_page_exposure.png')
)

# 为“班课主页”生成图表
plot_exposure_chart(
    daily_exposures,
    '班课主页',
    os.path.join(output_dir, 'class_home_page_exposure.png')
)