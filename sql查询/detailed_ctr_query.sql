set hive.mapred.mode=nonstrict;

-- This query calculates detailed Click-Through Rates (CTR) based on the analysis requirements
-- in '功能埋点说明.md' and references the logic from 'daily_ctr_query.sql'.
--
-- Key Fixes:
-- 1. Correct Exposure Calculation: Exposures are now calculated at the page level and then joined with click data.
-- 2. Correct URL Paths: URL paths for '我的课程页' have been corrected (e.g., using '/click/tutorNewPc/...' instead of '/click/myCoursePage/...').

WITH page_exposures AS (
    -- First, calculate the daily unique exposures for each main page.
    SELECT
        dt,
        CASE
            WHEN url = '/expose/tutorNewPc/homepageExpose' THEN '我的课程页'
            WHEN url = '/expose/seasonPage/seasonPageExpose' THEN '班课主页'
        END AS page_name,
        COUNT(DISTINCT user_id) AS expose_uv
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02'
      AND url IN ('/expose/tutorNewPc/homepageExpose', '/expose/seasonPage/seasonPageExpose')
    GROUP BY dt, CASE
            WHEN url = '/expose/tutorNewPc/homepageExpose' THEN '我的课程页'
            WHEN url = '/expose/seasonPage/seasonPageExpose' THEN '班课主页'
        END
),

feature_clicks AS (
    -- Next, gather all detailed click events and count unique users for each.

    -- 1. 我的课程页 - 筛选项点击
    SELECT
        dt, '我的课程页' AS page_name, '筛选项点击' AS feature_name,
        CASE lower(other['filtername'])
            WHEN '1' THEN '科目'
            WHEN '2' THEN '年级'
            WHEN '3' THEN '类型'
            WHEN '4' THEN '课程状态'
            ELSE '未知'
        END AS detail,
        user_id
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02' AND url = '/click/tutorNewPc/filterClick'

    UNION ALL

    -- 2. 我的课程页 - 班课卡片点击
    SELECT
        dt, '我的课程页' AS page_name, '班课卡片点击' AS feature_name,
        concat('直播:', CASE lower(other['islive']) WHEN '1' THEN '是' ELSE '否' END,
               ', 待办:', CASE WHEN lower(other['islive']) = '0' THEN CASE lower(other['istasktodo']) WHEN '1' THEN '有' ELSE '无' END ELSE 'N/A' END) AS detail,
        user_id
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02' AND url = '/click/tutorNewPc/seasonCardClick'

    UNION ALL

    -- 3. 我的课程页 - 个人中心功能点击
    SELECT
        dt, '我的课程页' AS page_name, '个人中心功能点击' AS feature_name,
        CASE lower(other['functiontype'])
            WHEN '1' THEN '金币商城'
            WHEN '2' THEN '一对一课程'
            WHEN '3' THEN '设置'
            WHEN '4' THEN '意见反馈'
            WHEN '5' THEN '清除缓存'
            WHEN '6' THEN '检查更新'
            WHEN '7' THEN '退出登录'
            ELSE '未知'
        END AS detail,
        user_id
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02' AND url = '/click/userInfo/function'

    UNION ALL

    -- 4. 我的课程页 - 设置弹窗切tab点击
    SELECT
        dt, '我的课程页' AS page_name, '设置弹窗切tab点击' AS feature_name,
        CASE lower(other['tabtype'])
            WHEN '0' THEN '设备调试'
            WHEN '1' THEN '家长监督'
            WHEN '2' THEN '帮助中心'
            ELSE '未知'
        END AS detail,
        user_id
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02' AND url LIKE '%/settingsPopup/tab%' -- Assuming this URL is correct as it's not in daily_ctr_query

    UNION ALL

    -- 5. 班课主页 - 金刚位点击
    SELECT
        dt, '班课主页' AS page_name, '金刚位功能点击' AS feature_name,
        CASE lower(other['functiontype'])
            WHEN '2' THEN '课程资料'
            WHEN '3' THEN '课堂检测'
            WHEN '4' THEN '标记记录'
            WHEN '5' THEN '排行榜'
            ELSE '未知'
        END AS detail,
        user_id
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02' AND url = '/click/seasonPage/functionType'

    UNION ALL

    -- 6. 班课主页 - 工具栏点击
    SELECT
        dt, '班课主页' AS page_name, '工具栏功能点击' AS feature_name,
        CASE lower(other['toolname'])
            WHEN '1' THEN '课程评价'
            WHEN '2' THEN '隐藏班课'
            WHEN '3' THEN '知识卡片'
            WHEN '4' THEN '老师发卡'
            ELSE '未知'
        END AS detail,
        user_id
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02' AND url = '/click/seasonPage/toolClick'

    UNION ALL

    -- 7. 班课主页 - 课程大纲点击
    SELECT
        dt, '班课主页' AS page_name, '课程大纲点击' AS feature_name,
        concat('直播:', CASE lower(other['islive']) WHEN '1' THEN '是' ELSE '否' END,
               ', 待办:', CASE WHEN lower(other['islive']) = '0' THEN CASE lower(other['istasktodo']) WHEN '1' THEN '有' ELSE '无' END ELSE 'N/A' END) AS detail,
        user_id
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02' AND url = '/click/seasonPage/episodeSyllabusClick'

    UNION ALL

    -- 8. 班课主页 - 课程环节点击
    SELECT
        dt, '班课主页' AS page_name, '课程环节点击' AS feature_name,
        other['episodelinkname'] AS detail,
        user_id
    FROM dw_dwd.dwd_tutor_frog_di
    WHERE dt BETWEEN '2025-05-20' AND '2025-07-02' AND url = '/click/seasonPage/episodeLinkClick'
),

daily_feature_clicks AS (
    SELECT
        dt,
        page_name,
        feature_name,
        detail,
        COUNT(DISTINCT user_id) AS click_uv
    FROM feature_clicks
    GROUP BY dt, page_name, feature_name, detail
)

SELECT
    fc.dt AS `日期`,
    fc.page_name AS `页面名称`,
    fc.feature_name AS `功能名称`,
    fc.detail AS `细分`,
    fc.click_uv AS `点击uv`,
    COALESCE(pe.expose_uv, 0) AS `曝光uv`,
    CASE
        WHEN COALESCE(pe.expose_uv, 0) > 0 THEN fc.click_uv * 100.0 / pe.expose_uv
        ELSE 0
    END AS `点击率(%)`
FROM daily_feature_clicks fc
LEFT JOIN page_exposures pe ON fc.dt = pe.dt AND fc.page_name = pe.page_name
ORDER BY `页面名称`, `功能名称`, `细分`;