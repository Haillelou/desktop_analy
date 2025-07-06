SELECT distinct base_t.userid as `用户id`
    , case base_t.semesterid 
        when 132 then '23寒'
        when 133 then '23春'
        when 134 then '23暑'
        when 135 then '23秋'
        else base_t.semesterid
        end as `学季`
    , case base_t.grade_type
        when 'xiaoxue' then '小学'
        when 'chuzhong' then '初中'
        when 'gaozhong' then '高中'
        else base_t.grade_type
        end as `学部`
    , case base_t.gradeid 
        when -3 then '新概念'
        when -2 then '剑桥班' 
        when 98 then '小班'
        when 99 then '中班'
        when 100 then '大班'
        when 1 then '一年级' 
        when 2 then '二年级' 
        when 3 then '三年级' 
        when 4 then '四年级' 
        when 5 then '五年级' 
        when 6 then '六年级' 
        when 7 then '初一' 
        when 8 then '初二' 
        when 9 then '初三' 
        when 10 then '高一' 
        when 11 then '高二' 
        when 12 then '高三' 
        else base_t.gradeid 
        end as `年级` 
    , case base_t.courseid
        when 1 then '语文' 
        when 2 then '数学' 
        when 3 then '英语' 
        when 4 then '物理' 
        when 5 then '化学' 
        when 6 then '生物' 
        when 7 then '历史' 
        when 8 then '地理' 
        when 9 then '政治' 
        when 14 then '思想品德' 
        when 15 then '科学'
        when 101 then '文综' 
        when 201 then '编程'
        when 204 then '人文博雅'
        when 206 then '阅读表达'
        when 207 then '新思维'
        else base_t.courseid 
        end as `科目`
    , base_t.period as `期数`           
    , base_t.less_mark_verify_name as `班课标签分类名称(sku)`
    , base_t.lesson_mark_name as `班课标签`
    , base_t.seasonid as `班课id`
    , base_t.seasonname as `班课名`
    , time2dt(base_t.order_paidtime) as `支付日期`
    , season_t.season_week as `周几上课`        
    , season_t.season_time as `上课时间`        
    , season_t.teacher_nickname as `主讲姓名`
    , season_t.teacher_ldap as `主讲ldap`
    , base_t.text_book_edition_name AS `教材版本`
    , base_t.mixed_level_name as `班型`
    , case when xinlao_t.userid is not null then '老用户' else '新用户' end as `是否新老用户`
    , base_t.is_first_renewal as `是否首续用户`
    , base_t.is_course_first_renewal as `是否学科首续用户`
                                
                           
    , nvl(address_t1.province,address_t2.province) as `省份`
    , nvl(address_t1.city,address_t2.city) as `市`
    , nvl(address_t1.county,address_t2.county) as `区县`
    , case
        when nvl(address_t1.city,address_t2.city) rlike '北京|上海|广州|深圳'  then '一线城市'
        when nvl(address_t1.city,address_t2.city) rlike '成都|杭州|武汉|重庆|南京|天津|苏州|西安|长沙|沈阳|青岛|郑州|大连|东莞|宁波' then '新一线'
        when nvl(address_t1.city,address_t2.city) rlike '厦门|福州|无锡|合肥|昆明|哈尔滨|济南|佛山|长春|温州|石家庄|南宁|常州|泉州|南昌|贵阳|太原|烟台|嘉兴|南通|金华|珠海|惠州|徐州|海口|乌鲁木齐|绍兴|中山|台州|兰州' then '二线城市'
        when nvl(address_t1.city,address_t2.city) rlike '潍坊|保定|镇江|扬州|桂林|唐山|三亚|湖州|呼和浩特|廊坊|洛阳|威海|盐城|临沂|江门|汕头|泰州|漳州|邯郸|济宁|芜湖|淄博|银川|柳州|绵阳|湛江|鞍山|赣州|大庆|宜昌|包头|咸阳|秦皇岛|株洲|莆田|吉林|淮安|肇庆|宁德|衡阳|南平|连云港|丹东|丽江|揭阳|延边朝鲜族自治州|舟山|九江|龙岩|沧州|抚顺|襄阳|上饶|营口|三明|蚌埠|丽水|岳阳|清远|荆州|泰安|衢州|盘锦|东营|南阳|马鞍山|南充|西宁|孝感|齐齐哈尔' then '三线城市'
        when nvl(address_t1.city,address_t2.city) rlike '乐山|湘潭|遵义|宿迁|新乡|信阳|滁州|锦州|潮州|黄冈|开封|德阳|德州|梅州|鄂尔多斯|邢台|茂名|大理白族|韶关|商丘|安庆|黄石|六安|玉林|宜春|北海|牡丹江|张家口|梧州|日照|咸宁|常德|佳木斯|红河哈尼族|黔东南|阳江|晋中|渭南|呼伦贝尔|恩施土|河源|郴州|阜阳|聊城|大同|宝鸡|许昌|赤峰|运城|安阳|临汾|宣城|曲靖|西双版纳|邵阳|葫芦岛|平顶山|辽阳|菏泽|本溪|驻马店|汕尾|焦作|黄山|怀化|四平|榆林|十堰|宜宾|滨州|抚州|淮南|周口|黔南布依族|泸州|玉溪|眉山|通化|宿州|枣庄|内江|遂宁|吉安|通辽|景德镇|阜新|雅安|铁岭|承德|娄底' then '四线城市'
        when nvl(address_t1.city,address_t2.city) rlike '克拉玛依|长治|永州|绥化|巴音郭楞|拉萨|云浮|益阳|百色|资阳|荆门|松原|凉山|达州|伊犁|广安|自贡|汉中|朝阳|漯河|钦州|贵港|安顺|鄂州|广元|河池|鹰潭|乌兰察布|铜陵|昌吉|衡水|黔西南|濮阳|锡林郭勒|巴彦淖尔|鸡西|贺州|防城港|兴安|白山|三门峡|忻州|双鸭山|楚雄|新余|来宾|淮北|亳州|湘西||吕梁|攀枝花|晋城|延安|毕节|张家界|酒泉|崇左|萍乡|乌海|伊春|六盘水|随州|德宏傣族|池州|黑河|哈密|文山壮族|阿坝藏族|天水|辽源|张掖|铜仁|鹤壁|儋州|保山|安康|白城|巴中|普洱|鹤岗|莱芜|阳泉|甘孜藏族|嘉峪关|白银|临沧|商洛|阿克苏|海西蒙|大兴安岭|七台河|朔州|铜川|定西|迪庆藏族|日喀则|庆阳|昭通|喀什地区|怒江傈僳族|海东|阿勒泰|平凉|石嘴山|武威|阿拉善|塔城地|林芝|金昌|吴忠|中卫|陇南|山南|吐鲁番|博尔塔拉|临夏回族|固原|甘南藏族|昌都|阿里|海南藏族|和田地区|克孜勒苏柯|海北藏族|那曲|玉树|黄南藏族|果洛藏族|三沙' then '五线城市'
        else '其他城市' 
        end as `城市线级`
    , focus_textbook_t.textbookversion as `focus标注教材版本`
    , nvl(address_t1.edition_name,address_t2.edition_name) as `focus口径地址对应初数版本`
    , attend_finish_t.should_attend_cnt as `应到课次数`
    , attend_finish_t.live_attend_cnt as `直播到课次数`
    , attend_finish_t.live_finish_cnt as `直播完课次数`
    , attend_finish_t.replay_attend_cnt as `回放到课次数`
    , attend_finish_t.replay_finish_cnt as `回放完课次数`
    , attend_finish_t.pseudo_should_attend_cnt as `伪直播应到课次数`
    , attend_finish_t.pseudo_attend_cnt as `伪直播到课次数`
    , attend_finish_t.pseudo_finish_cnt as `伪直播完课次数`
    , attend_finish_t.total_finish_cnt as `总完课次数`
    , attend_finish_t.should_attend_gonggu_cnt as `巩固课应到课次数`
    , attend_finish_t.attend_gonggu_cnt as `巩固课直播到课次数`
    , attend_finish_t.is_attend_parent as `是否家长课到课`
    , attend_finish_t.parent_attend_ratio as `家长课到课时长占比`
    , nvl(exercise_t.should_answer_cnt,0) as `互动题应作答题目数`
    , nvl(exercise_t.answer_cnt,0) as `互动题作答题目数`
    , nvl(exercise_t.answer_correct_cnt,0) as `互动题正确作答题目数`
    , nvl(exercise_t2.should_answer_cnt,0) as `伪直播互动题应作答题目数`
    , nvl(exercise_t2.answer_cnt,0) as `伪直播互动题作答题目数`
    , nvl(exercise_t2.answer_correct_cnt,0) as `伪直播互动题正确作答题目数`
    , nvl(submit_t.homework_cnt,0) as `作业提交次数`
    , nvl(submit_t.note_cnt,0) as `笔记提交次数`
    , nvl(fenceng_t.kehou_should_answer_cnt,0) as `分层随堂探究应提交次数`
    , nvl(fenceng_t.kehou_answer_cnt,0) as `分层随堂探究提交次数`
    , nvl(fenceng_t.kehou_answer_right_cnt,0) as `分层随堂探究作答正确题目数`
    , nvl(qa_t.small_qa_cnt,0) as `小班答疑次数`
    , nvl(qa_t.1v60_qa_cnt,0) as `1v60答疑次数`
    , nvl(qa_t.big_qa_cnt,0) as `大班答疑次数`
    , nvl(qa_t.gonggu_qa_cnt,0) as `巩固练习次数`
    , nvl(call_t.call_out_cnt,0) as `外呼次数`    
    , nvl(call_t.call_cnt,0) as `外呼接通次数`
    , nvl(call_t.1min_plus_call_cnt,0) as `外呼1min以上次数`
    , nvl(call_t.5min_plus_call_cnt,0) as `外呼5min以上次数`
    , nvl(call_t.10min_plus_call_cnt,0) as `外呼10min以上次数`
    , nvl(call_t.15min_plus_call_cnt,0) as `外呼15min以上次数`
    , nvl(round(call_t.call_duration_min,2),0) as `外呼沟通累计时长_分`
    , base_t.is_semi_renewal as `是否半季补缴`
    , base_t.is_geji AS `是否隔季续报`
    , course_t.course_cnt as `系统班科目数`
    , course_t.coursename as `系统班科目明细`
                                       
                                       
                                      
                                         
                                          
                                              
    , base_t.teamid as `小班id`
    , mentor_t.mentor_id as `辅导id`
    , mentor_t.mentor_ldap as `辅导ldap`
    , mentor_t.mentor_nickname as `辅导姓名`
    , mentor_t.mentor_region as `辅导城市`
    , mentor_t.manager_ldap as `组长ldap`
    , mentor_t.manager_fullname as `组长姓名`
    , mentor_t.manager_id as `组长id`
    , mentor_t.director_ldap as `主管ldap`
    , mentor_t.director_fullname as `主管姓名`
    , mentor_t.director_id as `主管id`
    , prod_line_t.user_type as `用户来源`
    , prod_line_t.channel as `一级渠道`
    , course_seme_t.course_semester_cnt as `学科学习季度数`
    , seme_t.semester_cnt as `系统学习季度数`
    , seme_t.long_semester_cnt as `系统长学季学习季度数`
    , test_t.avg_score as `平均评估成绩`
    , base_t.geji_seasonid as `隔季续报班课id`
    , base_t.geji_paiddt as `隔季续报日期`
                                                           
                                                   
    , nvl(case when base_t.courseid >200 then coin_t.suyang_moneybalance
        else coin_t.subject_moneybalance end,0) as `金币余额`
                                                  
                                                    
                                                    
    , coin_t2.rewardscore as `班课内获取金币数`

from 
(
    select distinct userid
        , semesterid
        , teamid
        , seasonid
        , seasonname 
        , less_mark_verify_name
        , lesson_mark_name
        , courseid
        , grade_type 
        , gradeid
        , period                  
        , text_book_edition_name
        , mixed_level_name
        , is_semi_renewal
        , is_geji
        , split(geji_properties[0], '@')[11] AS geji_seasonid 
        , split(geji_properties[0], '@')[6] AS geji_paiddt
        , order_paidtime
        , keyfrom
        , mentorid 
        , is_first_renewal
        , is_course_first_renewal
    from tutor.dm_tutor_season_team_renewal_detail_da
    where dt = '2025-06-10'
        and semesterid = '141' 
        and courseid in ('1','2','3')
        and grade_type in ('xiaoxue','chuzhong')
        and  1=1  
        and seasonname not rlike '测试'
        and  1=1 
) base_t
/*
left join 
(
    select distinct userid
        , seasonid
        , is_semi_renewal
        , is_geji
    from tutor.dm_tutor_season_team_renewal_detail_da
    where dt ='2025-06-10'
        and seasonname not rlike '测试'
) renewal_t 
on base_t.userid = renewal_t.userid 
    and base_t.geji_seasonid = renewal_t.seasonid 
*/
left join 
(   
    select seasonid
        , concat_ws(',',collect_set(teacher_nickname)) as teacher_nickname 
        , concat_ws(',',collect_set(teacher_ldap)) as teacher_ldap
        , concat_ws(',', sort_array(collect_set(season_week))) as season_week
        , concat_ws(',', sort_array(collect_set(season_time))) as season_time
    from tutor.dw_season_order_information_new
    where semesterid = '141' 
        and courseid in ('1','2','3')
        and grade_type in ('xiaoxue','chuzhong')
        and  1=1 
    group by seasonid 
) season_t 
on base_t.seasonid = season_t.seasonid 

left join 
(               
    select distinct studentid
        , subjectid
        , textbookversion
    from dw_ori.ori_tutor_focus_cms_student_student_subject_info_da 
) focus_textbook_t 
on base_t.userid = focus_textbook_t.studentid
    and base_t.courseid = focus_textbook_t.subjectid   

left join
(                 
    select distinct userid
            , province
            , city 
            , county
            , address_1.address as address
            /*
            , CASE when province = '天津' or province = '天津市' then '人教版'
                when province = '内蒙古' or province = '内蒙古自治区' then  '人教版'
                when province = '上海' or province = '上海市' then '沪教版'
                when province = '江西' or province = '江西省' then  '人教版'
                when province = '云南' or province = '云南省' then  '北师大'
                when province = '西藏' or province = '西藏自治区' then  '人教版'
                when province = '甘肃' or province = '甘肃省' then  '北师大'
                when province = '宁夏' or province = '宁夏回族自治区' then  '北师大'
                when province = '新疆' or province = '新疆维吾尔自治区' then  '人教版'
              else edition end as edition_name
              */
            , case when province rlike '北京' then '人教/课改'
                when province rlike '天津' then '人教'
                when province rlike '上海' then '沪教'
                when province rlike '重庆' then '人教/北师/华师'
                else edition
                end as edition_name 
    from
    (
        select distinct userid 
            , province
            , city 
            , county
            /*
            , CASE WHEN province IN ('北京', '北京市', '上海', '上海市', '天津', '天津市', '重庆', '重庆市') THEN concat(substring(province,1,2), substring(county,1,2))
                ELSE concat(substring(province,1,2), substring(city,1,2))
              END AS address  
            */
            , case when province=city or city rlike '省直辖县级行政区划' then concat(substring(province,1,2), substring(county,1,2))
                else concat(substring(province,1,2), substring(city,1,2)) 
                end as address 
        from 
        (
            select distinct a.userid 
                , a.addressid 
                , b.province
                , b.city 
                , b.county
                , dense_rank() over(partition by a.userid order by a.dbutime desc) as update_rk
                , a.dbutime
            from dw_ori.ori_tutor_address_user_address_recent_usage_da a 
            left join dw_ods.ods_tutor_address_user_address_da_view b on a.userid = b.userid and a.addressid = b.id
            where b.province not rlike '猿|不'
                and b.city not rlike '猿|不'
                 and b.county not rlike '猿|不'
        ) t 
        where update_rk = '1'
     ) address_1

    left join
    (
        select concat(SUBSTRING(province,1,2),SUBSTRING(city,1,2)) as address,text_book_edition_name as edition
        from temp.focus_address_text_book_edition_name_new 
    ) focus_edition
    on address_1.address = focus_edition.address
) address_t1     
on base_t.userid = address_t1.userid 

left join
(   
    select distinct userid
            , province
            , city 
            , county
            , address_2.address as address
            /*
            , CASE when province = '天津' or province = '天津市' then '人教版'
                when province = '内蒙古' or province = '内蒙古自治区' then  '人教版'
                when province = '上海' or province = '上海市' then '沪教版'
                when province = '江西' or province = '江西省' then  '人教版'
                when province = '云南' or province = '云南省' then  '北师大'
                when province = '西藏' or province = '西藏自治区' then  '人教版'
                when province = '甘肃' or province = '甘肃省' then  '北师大'
                when province = '宁夏' or province = '宁夏回族自治区' then  '北师大'
                when province = '新疆' or province = '新疆维吾尔自治区' then  '人教版'
              else edition end as edition_name
              */
            , case when province rlike '北京' then '人教/课改'
                when province rlike '天津' then '人教'
                when province rlike '上海' then '沪教'
                when province rlike '重庆' then '人教/北师/华师'
                else edition
                end as edition_name 
    from
    (
        select distinct userid 
            , province
            , city 
            , county
            /*
            , CASE WHEN province IN ('北京', '北京市', '上海', '上海市', '天津', '天津市', '重庆', '重庆市') THEN concat(substring(province,1,2), substring(county,1,2))
                ELSE concat(substring(province,1,2), substring(city,1,2))
              END AS address  
            */
            , case when province=city or city rlike '省直辖县级行政区划' then concat(substring(province,1,2), substring(county,1,2))
                else concat(substring(province,1,2), substring(city,1,2)) 
                end as address 
        from 
        (   
            select distinct a.userid 
                , a.addressid 
                , b.province
                , b.city  
                , b.county
            from tutor.ori_mysql_tutor_address_user_default_address a 
            left join dw_ods.ods_tutor_address_user_address_da_view b on a.userid = b.userid and a.addressid = b.id
            where b.province not rlike '猿|不' 
                and b.city not rlike '猿|不'
                 and b.county not rlike '猿|不'
        ) s
     ) address_2
    
    left join
    (
        select concat(SUBSTRING(province,1,2),SUBSTRING(city,1,2)) as address,text_book_edition_name as edition
        from temp.focus_address_text_book_edition_name_new
    ) focus_edition
    on address_2.address = focus_edition.address
) address_t2    
on base_t.userid = address_t2.userid 

left join 
(
    select userid
        , concat_ws(',',sort_array(collect_set(case courseid 
            when 1 then '语文' 
            when 2 then '数学' 
            when 3 then '英语' 
            when 4 then '物理' 
            when 5 then '化学' 
            when 6 then '生物' 
            when 7 then '历史' 
            when 8 then '地理' 
            when 9 then '政治' 
            when 204 then '人文博雅'
            when 206 then '阅读表达'
            when 207 then '新思维'
            else courseid 
            end))) as coursename
                                                                          
                                                                          
                                                                        
                                                                             
                                                                            
                                                                              
        , count(distinct courseid) as course_cnt
    from tutor.dm_tutor_season_team_renewal_detail_da
    where dt = '2025-06-10'
        and semesterid = '141' 
        and grade_type in ('xiaoxue','chuzhong')
                     
        and seasonname NOT RLIKE '测试'
                    
    group by userid 
) course_t 
on base_t.userid = course_t.userid

left join
(
    select distinct less_id
        , team_id
        , mentor_id 
        , mentor_ldap 
        , mentor_nickname 
        , mentor_region 
        , manager_ldap 
        , manager_fullname 
        , manager_id 
        , director_ldap 
        , director_fullname 
        , director_id 
    from tutor.dm_tutor_lesson_team_mentor_da
    where dt = '2025-06-10'
) mentor_t
on base_t.seasonid = mentor_t.less_id 
    and base_t.teamid = mentor_t.team_id

left join 
     
	(select semesterid,courseid,userid,grade_type,gradeid,lesson_mark_name,
		max(user_type) as user_type,
		max(channel) as channel,
		max(plat) as plat
	from 
	    (select distinct semesterid,courseid,userid,gradetype as grade_type,gradeid,
	    	less_mark_name as lesson_mark_name,user_type,
	    	case when channel='猿辅导app' and plat in ('OPPO','VIVO','小米','华为') then '猿辅导app-应用市场'
	            when channel='猿辅导app' then '猿辅导app-自然流量' else channel end as channel,plat
		    from tutor.dw_tutor_xueke_user_source_da
		    where 1=1 
         and dt=date_sub(current_date,1)
		    and semesterid = '141'
		    and gradetype in ('xiaoxue','chuzhong')
		    and courseid in ('1','2','3') ) a 
	    group by semesterid,courseid,userid,grade_type,gradeid,lesson_mark_name
	union all
	select semester_id as semesterid,course_id as courseid,user_id as userid,grade_type,grade as gradeid,lesson_mark_name,
		max(user_type) as user_type,
		max(channel) as channel,
		max(plat) as plat
	    from tutor.dw_tutor_suyang_user_source_da
	    where dt=date_sub(current_date,1)
	    and semester_id = '141'
	    and grade_type in ('xiaoxue','chuzhong')
	    and course_id in ('1','2','3')
	    group by semester_id,course_id,user_id,grade_type,grade,lesson_mark_name) prod_line_t
	on base_t.userid = prod_line_t.userid 
    and base_t.semesterid = prod_line_t.semesterid 
    and base_t.courseid = prod_line_t.courseid
    and base_t.gradeid = prod_line_t.gradeid
    and base_t.lesson_mark_name = prod_line_t.lesson_mark_name

left join
(       
  select userid
    , courseid 
    , count(distinct semesterid) as course_semester_cnt
  from tutor.dw_season_order_information_new
  where order_paidtime > '0' 
    and refundedtime = '0'
    and is_normal_order = '1'
    and is_inner_user = '0'
    and is_test = '0' 
    and courseid != '201'
    and seasonname not rlike '线上回测|YFD|团队内部培训|走查|系统班作品活动说明会|md|通知+1|秋下'
    and to_date(from_unixtime(int(order_paidtime/1000))) <= '2025-06-10'
    and source not in ('12','22','19')
    and lessonmarkid not in ('63', '71')
    and lesson_episode_deletedtime = '0'
    and semesterid <= '141'
  group by userid 
    , courseid 
) course_seme_t
on base_t.userid = course_seme_t.userid 
  and base_t.courseid = course_seme_t.courseid 

left join
(       
  select userid
    , count(distinct semesterid) as semester_cnt 
    , count(distinct if(semesterid%2=1,semesterid,null)) as long_semester_cnt
  from tutor.dw_season_order_information_new
  where order_paidtime > '0' 
    and refundedtime = '0'
    and is_normal_order = '1'
    and is_inner_user = '0'
    and is_test = '0' 
    and courseid != '201'
    and seasonname not rlike '线上回测|YFD|团队内部培训|走查|系统班作品活动说明会|md|通知+1|秋下'
    and to_date(from_unixtime(int(order_paidtime/1000))) <='2025-06-10'
    and source not in ('12','22','19')
    and lessonmarkid not in ('63', '71')
    and lesson_episode_deletedtime = '0'
    and semesterid <= '141'
  group by userid 
) seme_t
on base_t.userid = seme_t.userid 

left join
(        
    select userid
        , lessonid
                          
                                                                         
                                                                           
        , avg(score) as avg_score
                     
                     
    FROM tutor.dw_tutor_student_lesson_jam_baseline_da 
    WHERE exam_category = 'jam' 
        and examtype = '三讲一测'
  and submittedtime>'0'
  and to_date(from_unixtime(int(submittedtime/1000)))<='2025-06-10'
    group by userid 
        , lessonid
) test_t
on base_t.userid = test_t.userid
    and base_t.seasonid = test_t.lessonid

left join
(         
    select finish_t.userid as userid
        , finish_t.lesson_id as lesson_id
        , count(distinct if(finish_t.lesson_episode_label_id='1000010',finish_t.episodeid,null)) as should_attend_cnt 
             
        , count(distinct if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_attend_live='1',finish_t.episodeid,null)) as live_attend_cnt 
        , count(distinct if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_attend_live='1' and (finish_t.live_duration/finish_t.episode_duration>=0.3 or finish_t.live_duration/60000>=30),finish_t.episodeid,null)) live_finish_cnt 
             
        , count(distinct if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_replay='1',finish_t.episodeid,null)) as replay_attend_cnt 
        , count(distinct if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_replay='1' and (finish_t.replay_duration/finish_t.episode_duration>=0.3 or finish_t.replay_duration/60000>=30),finish_t.episodeid,null)) replay_finish_cnt
              
        , count(distinct if(finish_t.lesson_episode_label_id='1000010',pseudofinish_t.episodeid,null)) as pseudo_should_attend_cnt 
        , count(distinct if(finish_t.lesson_episode_label_id='1000010' and pseudofinish_t.is_attend_live='1',pseudofinish_t.episodeid,null)) as pseudo_attend_cnt
        , count(distinct if(finish_t.lesson_episode_label_id='1000010' and pseudofinish_t.is_attend_live='1' and (pseudofinish_t.live_duration/pseudofinish_t.episode_duration>=0.3 or pseudofinish_t.live_duration/60000>=30),pseudofinish_t.episodeid,null)) as pseudo_finish_cnt
                 
        , count(distinct if(
            (finish_t.lesson_episode_label_id='1000010' and finish_t.is_attend_live='1' and (finish_t.live_duration/finish_t.episode_duration>=0.3 or finish_t.live_duration/60000>=30))
            or (finish_t.lesson_episode_label_id='1000010' and finish_t.is_replay='1' and (finish_t.replay_duration/finish_t.episode_duration>=0.3 or finish_t.replay_duration/60000>=30))
            or (finish_t.lesson_episode_label_id='1000010' and pseudofinish_t.is_attend_live='1' and (pseudofinish_t.live_duration/pseudofinish_t.episode_duration>=0.3 or pseudofinish_t.live_duration/60000>=30))
            ,finish_t.episodeid,null)) as total_finish_cnt
        
                 
        , count(distinct if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_gonggu='1',finish_t.episodeid,null)) as should_attend_gonggu_cnt
        , count(distinct if(finish_t.lesson_episode_label_id='1000010' and finish_t.is_gonggu='1' and finish_t.is_attend_live='1',finish_t.episodeid,null)) as attend_gonggu_cnt
              
        , count(distinct if(finish_t.lesson_episode_label_id='1000028' and finish_t.is_attend_live='1',finish_t.episodeid,null)) as is_attend_parent 
        , round(sum(if(finish_t.lesson_episode_label_id='1000028',finish_t.live_duration,0))
            / sum(if(finish_t.lesson_episode_label_id='1000028',finish_t.episode_duration,0)),4) as parent_attend_ratio
    from 
    (
        SELECT distinct semester_id as semesterid
            , grade_type
            , grade as gradeid
            , lesson_subject_id as courseid
            , case when episode_week_day = '1' then '周一'
                when episode_week_day = '2' then '周二'
                when episode_week_day = '3' then '周三'
                when episode_week_day = '4' then '周四'
                when episode_week_day = '5' then '周五'
                when episode_week_day = '6' then '周六'
                when episode_week_day = '7' then '周日'
                else episode_week_day 
                end as episode_week_day
            , lesson_id 
            , episode_id as episodeid
            , lesson_episode_label_id
            , cast(dense_rank() over(partition by lesson_id order by episode_start_dt) as int ) as episode_rank
            , user_id as userid
            , is_attend_live
            , live_duration
            , episode_duration
            , is_replay
            , replay_duration
            , case when episode_name rlike '总结巩固|综合实践|拓展延伸|巩固与延伸|收心课|测试|赠课' 
                or (episode_end_tp-episode_start_tp)/1000/60<=31 then 1 else 0 end as is_gonggu
        FROM tutor.dw_tutor_lesson_episode_user_finish_class_da
        where dt = date_sub(current_date,1)
            and semester_id = '141'
            and lesson_subject_id != '201'
            and lesson_flat_category = '3'       
            and lesson_mark_id not in ('63', '71')        
            and lesson_episode_label_id in ('1000010','1000028')            
            and is_have_ticket = '1'             
            and is_deleted = '0'         
            and episode_start_dt<='2025-06-10'
    ) finish_t 
    
    left join 
    (             
        select distinct a.lessonid
            , a.episodeid
            , a.pseudolivelessonid
            , a.pseudoliveepisodeid
            , b.semester_id as semesterid 
            , b.grade_type 
            , b.grade as gradeid
            , b.lesson_subject_id as courseid
            , b.user_id as userid
            , b.is_attend_live
            , b.live_duration
            , b.episode_duration
            , hour(from_unixtime(int(b.episode_start_tp/1000))) as epi_hour 
            , case b.episode_week_day 
                when 1 then '周一'
                when 2 then '周二'
                when 3 then '周三'
                when 4 then '周四'
                when 5 then '周五'
                when 6 then '周六'
                when 7 then '周日'
                else b.episode_week_day 
                end as episode_week_day
        from dw_ods.ods_tutor_pseudo_live_pseudo_live_lesson_da a 
        join tutor.dw_tutor_lesson_episode_user_finish_class_da b on a.pseudolivelessonid=b.lesson_id and a.pseudoliveepisodeid=b.episode_id
        where a.dt = date_sub(current_date,1)
            and b.dt = date_sub(current_date,1)
            and b.semester_id = '141'
            and b.lesson_subject_id != '201'
            and b.lesson_flat_category = '3'       
            and b.lesson_mark_id not in ('63', '71')        
            and b.is_have_ticket = '1'             
            and b.is_deleted = '0'         
    ) pseudofinish_t 
    on finish_t.lesson_id = pseudofinish_t.lessonid 
        and finish_t.episodeid = pseudofinish_t.episodeid
        and finish_t.userid = pseudofinish_t.userid 
    
    group by finish_t.userid
        , finish_t.lesson_id
) attend_finish_t
on base_t.userid = attend_finish_t.userid
    and base_t.seasonid = attend_finish_t.lesson_id 

left join 
(      
    select a.mentorid
        , a.studentid as userid 
        , a.lessonid as seasonid 
        , count(distinct if(a.roomtype='1',a.episodeid,null)) as small_qa_cnt
        , count(distinct if(a.roomtype='4',a.episodeid,null)) as 1v60_qa_cnt
        , count(distinct if(a.roomtype='2',a.episodeid,null)) as big_qa_cnt
        , count(distinct if(a.roomtype='3',a.episodeid,null)) as gonggu_qa_cnt
    from tutor.ori_mysql_tutor_focus_cms_student_mentor_qa_episode_student_record_da a
    join tutor.ori_mysql_tutor_focus_stat_student_mentor_qa_episode_attendance_record_da b on a.studentid=b.studentid and a.episodeid=b.episodeid
    where a.episodestatus = '2'       
        and to_date(from_unixtime(int(a.episodestarttime/1000))) between '2021-01-01' and  '2025-06-10'
    group by a.mentorid
        , a.studentid
        , a.lessonid
) qa_t 
on base_t.userid = qa_t.userid
    and base_t.seasonid = qa_t.seasonid
    and base_t.mentorid = qa_t.mentorid 

left join 
(      
    select mentorid
        , dialeduserid as userid 
        , count(distinct id) as call_out_cnt 
        , count(distinct if(duration/60>0,id,null)) as call_cnt 
        , count(distinct if(duration/60>=1,id,null)) as 1min_plus_call_cnt 
        , count(distinct if(duration/60>=5,id,null)) as 5min_plus_call_cnt 
        , count(distinct if(duration/60>=10,id,null)) as 10min_plus_call_cnt 
        , count(distinct if(duration/60>=15,id,null)) as 15min_plus_call_cnt 
        , sum(duration/60) as call_duration_min
    from tutor.ori_mysql_tutor_primary_sop_call_record_da
    where dialeduserid != '-1'       
        and callmode = '0'      
        and to_date(from_unixtime(int(starttime/1000))) between '2025-06-04' and '2025-06-10' 
    group by mentorid
        , dialeduserid
) call_t 
on base_t.userid = call_t.userid
    and base_t.mentorid = call_t.mentorid 

left join 
(
    select user_id as userid 
        , season_id as seasonid 
        , count(distinct if(is_finish_homework=1,episode_id,null)) as homework_cnt
        , count(distinct if(is_finish_note=1,episode_id,null)) as note_cnt
    from dw_dws.dws_tutor_user_after_episode_activity_info_da 
    where dt = date_sub(current_date,1)
  and to_date(from_unixtime(int(episode_start_time/1000)))<='2025-06-10'
    group by user_id
        , season_id
) submit_t
on base_t.userid = submit_t.userid 
    and base_t.seasonid = submit_t.seasonid


left join 
(       
    select a.userid 
        , a.seasonid 
        , count(distinct concat_ws('-',exercise_t.episodeid,exercise_t.quiz_pageid)) as should_answer_cnt 
        , count(distinct if(user_exercise_t.submittedtime>'0',concat_ws('-',user_exercise_t.episodeid,user_exercise_t.quiz_pageid),null)) as answer_cnt 
        , count(distinct if(user_exercise_t.submittedtime>'0' and user_exercise_t.result>'0',concat_ws('-',user_exercise_t.episodeid,user_exercise_t.quiz_pageid),null)) as answer_correct_cnt 
    from 
    (
        select distinct a.userid
            , a.seasonid 
            , a.episodeid  
        from tutor.dw_season_order_information_new a 
        join tutor.dw_tutor_lesson_episode_user_finish_class_da b on b.dt=date_sub(current_date,1) and a.userid=b.user_id and a.seasonid=b.lesson_id and a.episodeid=b.episode_id
        where a.order_paidtime > '0'       
            and a.refundedtime = '0'       
            and a.is_normal_order = '1'                
            and a.is_inner_user = '0'         
            and a.is_test = '0'          
            and a.semesterid = '141'
            and a.courseid != '201'        
            and a.courseid in ('1','2','3')
            and a.grade_type in ('xiaoxue','chuzhong') 
            and  1=1 
            and a.seasonname not rlike '线上回测|YFD|团队内部培训|走查|系统班作品活动说明会|md|通知+1|测试'           
            and to_date(from_unixtime(int(a.order_paidtime/1000))) <=  '2025-06-10'
            and to_date(from_unixtime(int(a.episode_starttime/1000))) <=  '2025-06-10'
            and a.source not in ('12','22','19')         
            and a.lessonmarkid not in('63', '71')               
            and a.lesson_episode_label_id = '1000010'
            and b.is_have_ticket = '1'
            and b.is_attend_live = '1'
    ) a 

    left join
    (        
        select distinct roomid as episodeid 
            , id 
            , quizid
            , pageid
            , concat_ws('-', quizid, pageid) as quiz_pageid
        from tutor.ori_mysql_tutor_live_exercise_quiz_da 
        lateral view explode(split(questionpageids,',')) t as pageid
    ) exercise_t 
    on a.episodeid = exercise_t.episodeid

    left join
    (        
        select distinct roomid as episodeid 
            , pageid
            , userid
            , result
            , submittedtime
            , quizid 
            , concat_ws('-', quizid, pageid) as quiz_pageid
        from tutor.ods_tutor_live_exercise_user_answer_result_da
    ) user_exercise_t 
    on a.userid = user_exercise_t.userid 
        and a.episodeid = user_exercise_t.episodeid
        and exercise_t.quizid = user_exercise_t.quizid
        and exercise_t.pageid = user_exercise_t.pageid  

    group by a.userid 
        , a.seasonid 
) exercise_t
on base_t.userid = exercise_t.userid 
    and base_t.seasonid = exercise_t.seasonid

left join 
(          
    select a.userid 
        , a.seasonid 
        , count(distinct concat_ws('-',exercise_t.episodeid,exercise_t.quiz_pageid)) as should_answer_cnt 
        , count(distinct if(user_exercise_t.submittedtime>'0',concat_ws('-',user_exercise_t.episodeid,user_exercise_t.quiz_pageid),null)) as answer_cnt 
        , count(distinct if(user_exercise_t.submittedtime>'0' and user_exercise_t.result>'0',concat_ws('-',user_exercise_t.episodeid,user_exercise_t.quiz_pageid),null)) as answer_correct_cnt 
    from 
    (
        select distinct userid
            , seasonid 
            , episodeid  
        from tutor.dw_season_order_information_new
        where order_paidtime > '0'       
            and refundedtime = '0'       
            and is_normal_order = '1'                
            and is_inner_user = '0'         
            and is_test = '0'          
            and semesterid = '141'
            and courseid != '201'        
            and courseid in ('1','2','3')
            and grade_type in ('xiaoxue','chuzhong') 
            and  1=1 
            and seasonname not rlike '线上回测|YFD|团队内部培训|走查|系统班作品活动说明会|md|通知+1|测试'           
            and to_date(from_unixtime(int(order_paidtime/1000))) <= '2025-06-10'
            and to_date(from_unixtime(int(episode_starttime/1000))) <=  '2025-06-10'
            and source not in ('12','22','19')         
            and lessonmarkid not in('63', '71')               
            and lesson_episode_label_id = '1000010'
    ) a 

    join 
    (
        select distinct lessonid
            , episodeid
            , pseudolivelessonid
            , pseudoliveepisodeid
        from dw_ods.ods_tutor_pseudo_live_pseudo_live_lesson_da
        where dt = '2025-06-10'
    ) b
    on a.seasonid = b.lessonid
        and a.episodeid = b.episodeid

    join
    (
        select distinct lesson_id 
            , episode_id as episodeid
            , user_id as userid
        FROM tutor.dw_tutor_lesson_episode_user_finish_class_da
        where dt = date_sub(current_date,1)
            and semester_id = '141'
            and lesson_subject_id != '201'
            and lesson_flat_category = '3'       
            and lesson_mark_id not in ('63', '71')        
            and lesson_episode_label_id in ('1000010','1000028')            
            and is_have_ticket = '1'             
            and is_deleted = '0'         
            and episode_start_dt<=  '2025-06-10'
            and is_attend_live = '1'
    ) c 
    on a.userid = c.userid 
        and b.pseudolivelessonid = c.lesson_id
        and b.pseudoliveepisodeid = c.episodeid 

    left join
    (        
        select distinct roomid as episodeid 
            , id 
            , quizid
            , pageid
            , concat_ws('-', quizid, pageid) as quiz_pageid
        from tutor.ori_mysql_tutor_live_exercise_quiz_da 
        lateral view explode(split(questionpageids,',')) t as pageid
    ) exercise_t 
    on c.episodeid = exercise_t.episodeid

    left join
    (        
        select distinct roomid as episodeid 
            , pageid
            , userid
            , result
            , submittedtime
            , quizid 
            , concat_ws('-', quizid, pageid) as quiz_pageid
        from tutor.ods_tutor_live_exercise_user_answer_result_da
    ) user_exercise_t 
    on a.userid = user_exercise_t.userid 
        and c.episodeid = user_exercise_t.episodeid
        and exercise_t.quizid = user_exercise_t.quizid
        and exercise_t.pageid = user_exercise_t.pageid  

    group by a.userid 
        , a.seasonid 
) exercise_t2
on base_t.userid = exercise_t2.userid 
    and base_t.seasonid = exercise_t2.seasonid

left join 
(        
    select userid
        , max(if(accounttypeid='101',moneybalance,0)) as subject_moneybalance
        , max(if(accounttypeid='105',moneybalance,0)) as suyang_moneybalance
    from tutor.ods_tutor_user_money_account_snapshot_da
    where dt = date_sub(current_date,1)
        and accounttypeid in ('101','105')                         
        and currencyid = '10'
    group by userid 
) coin_t 
on base_t.userid = coin_t.userid
/*
left join 
( -- 推题
    select send.userid
        , send.seasonid 
        , count(distinct send.recommendexerciseinfoid) as send_cnt 
        , count(distinct if(expose.recommendexerciseinfoid is not null,expose.recommendexerciseinfoid,null)) as expose_cnt 
        , count(distinct if(submit.recommendexerciseinfoid is not null,submit.recommendexerciseinfoid,null)) as submit_cnt 
    from 
    ( -- 推题发送(包括高级群发+打开app入口)
        select distinct taskid
            , studentid as userid
            , round
            , lessonid as seasonid
            , recommendexerciseinfoid
            , if(appsendtime>0,appsendtime,sendtime) as sendtime
        from dw_ori.ori_tutor_focus_cms_student_student_personalized_question_exercise_da
        lateral view explode(split(sendh5urlbatchchattaskids,',')) B as taskid
    ) send 

    left join 
    ( -- 推题打开
        select user_id as userid
            , less_id as seasonid
            , split(other['pushtextid'],'-')[1] as recommendexerciseinfoid
            , other['pushquestionsource'] as pushquestionsource
            , min(action_tp) as min_expose_time -- 毫秒级别
        from dw_dwd.dwd_tutor_frog_di
        where dt as recommend_date
            and url in ('/expose/pushquestion/pageExpose')
        group by user_id,less_id,split(other['pushtextid'],'-')[1],other['pushquestionsource']
    ) expose 
    on send.userid = expose.userid
        and send.seasonid = expose.seasonid
        and send.recommendexerciseinfoid = expose.recommendexerciseinfoid

    left join 
    ( -- 推题提交
        select user_id as userid
            , less_id as seasonid
            , split(other['pushtextid'],'-')[1] as recommendexerciseinfoid
            , max(case when other['pushquestionsource']='miniapp' then 1 else 0 end) as is_miniapp_submit
            , max(case when other['pushquestionsource']='app' then 1 else 0 end) as is_app_submit
            , max(case when other['pushquestionsource']='push' then 1 else 0 end) as is_push_submit
            , max(case when other['pushquestionsource'] not in ('miniapp','app','push') or other['pushquestionsource'] is null or other['pushquestionsource']=''
                then 1 else 0 end) as is_other_submit
        from dw_dwd.dwd_tutor_frog_di
        where dt as recommend_date
            and url in ('/click/pushquestion/submitAnswer')
            and other['partname'] in ('startExercise','reinforcementExercise') --小学提交&初中变式题模块提交
            group by less_id,user_id,split(other['pushtextid'],'-')[1]
    ) submit
    on expose.userid = submit.userid 
        and expose.seasonid = submit.seasonid 
        and expose.recommendexerciseinfoid = submit.recommendexerciseinfoid
    group by send.userid
        , send.seasonid
) recommend_t 
on base_t.userid = recommend_t.userid
    and base_t.seasonid = recommend_t.seasonid
   
left join 
( -- 判断新老用户
  select aa.userid
       , aa.semesterid
       , case 
          when bb.userid is not null then '老用户'
          else '新用户'
         end as is_xinlao
  from 
  ( select distinct userid
          , semesterid
          , case 
              when pmod(semesterid,2)>0 then semesterid-2
              when pmod(semesterid,2)=0 then semesterid-1
            end as last_changji_id
     from tutor.dm_tutor_season_team_renewal_detail_da 
     where dt='2025-06-10'
     and semesterid>='127') aa
  left join 
    (select distinct userid,semesterid
      from tutor.dm_tutor_season_team_renewal_detail_da
      where dt='2025-06-10'
     and semesterid>='127') bb
  on aa.last_changji_id=bb.semesterid 
   and aa.userid=bb.userid) xinlao_t
  */ 
left join 
(        
    select distinct userid
    from tutor.dw_tutor_season_order_snapshot_da_view  
    where dt ='2025-06-10'
        and order_paidtime > '0'
        and refundedtime = '0'
        and is_normal_order = '1'
        and is_inner_user = '0'
        and is_test = '0'
        and (grade_type in ('xiaoxue','chuzhong') 
            or (grade_type='gaozhong' and lessonmarkid in ('242','244','246','266','554')))
        and (cast(cast(semesterid as int)+2 as string) = '141' or cast(cast(semesterid as int)+1 as string) = '141')
        and semesterid%2=1
        and courseid != '201' 
        and seasonname not rlike '线上回测|YFD|团队内部培训|走查|系统班作品活动说明会|md|通知+1|竞赛|强基|衔接|复习|录播|赠课'
        and lesson_mark_name not rlike '竞赛|强基|中考冲刺营|素养误操作补收专用|作文通UT'
        and source not in ('12','22','19') 
        and lesson_episode_label_id = '1000010'
        and lesson_episode_deletedtime = '0'
) xinlao_t 
on base_t.userid = xinlao_t.userid

left join 
(           
    select distinct lessonid as seasonid
        , userid
        , rewardscore
    from dw_ori.ori_tutor_wb_lesson_stat_pipe_tutor_lesson_student_stat_da
) coin_t2
on base_t.seasonid = coin_t2.seasonid 
    and base_t.userid = coin_t2.userid

left join 
(             
  select a.userid,
       a.seasonid,
       count(distinct episodeid) as kehou_should_answer_cnt, 
       count(case when is_finish='1' then 1 else null end) as kehou_answer_cnt,
       sum(case when kehou_answer_right_cnt is not null then kehou_answer_right_cnt else 0 end) as kehou_answer_right_cnt
from 
(
select distinct 
       userid,
       seasonid,
       episodeid
from tutor.dw_tutor_season_order_snapshot_da_view 
where  dt = '2025-06-10'
      and order_paidtime > '0'       
      and refundedtime = '0'       
      and is_normal_order = '1'                
      and is_inner_user = '0'         
      and is_test = '0'          
      and semesterid = '141'
      and courseid != '201'        
      and courseid in ('1','2','3')
      and grade_type in ('xiaoxue','chuzhong') 
      and  1=1 
      and seasonname not rlike '线上回测|YFD|团队内部培训|走查|系统班作品活动说明会|md|通知+1|测试'           
      and to_date(from_unixtime(int(order_paidtime/1000))) <= '2025-06-10'
      and to_date(from_unixtime(int(episode_starttime/1000))) <=  '2025-06-10'
      and to_date(from_unixtime(int(episode_starttime/1000))) >= to_date(from_unixtime(int(order_paidtime/1000)))
      and source not in ('12','22','19')         
      and lessonmarkid not in('63', '71')               
      and lesson_episode_label_id = '1000010'
            ) a 
left join 
(
  select userid,
         sourceid,
         is_finish,
         count(distinct case when answerStatus='1' then questionid else null end) as kehou_answer_right_cnt
from 
(
select userid,
       sourceid,
       is_finish,
       get_json_object(get_json_object(question_list, '$.questionEntity'),'$.questionId') AS questionid,
       get_json_object(get_json_object(question_list, '$.marking'),'$.answerStatus') AS answerStatus
  from 
  (
  select  a.userid,
          a.sourceid,
          a.exerciseid,
          a.is_finish,
          b.answersheetreportid,
          explode(split(
            regexp_replace(
              substr(compressedquestionmarkings, 2, length(compressedquestionmarkings) - 2),
              '\\},\\{', '}#{'
                ),'#')) as question_list
  from
  ( 
  select   userid,
           sourceid,
           exerciseid,
           case when status in ('2','3') then '1' else '0' end as is_finish
  from dw_ods.ods_mysql_tutor_modular_exercise_user_sub_modular_exercise_da
  where dt=date_sub(current_date,1)
  and exercisetype='32'
  ) a
  left join tutor.ods_tutor_lesson_exercise_exercise_da b on a.exerciseid=b.id
  left join tutor.ods_tutor_answer_sheet_report_tutor_answer_sheet_report_da c on b.answersheetreportid=c.id
  where  b.dt=date_sub(current_date,1)
    ) base
  ) base 
group by userid,
         sourceid,
         is_finish
  ) afterclass_t
 on a.episodeid=afterclass_t.sourceid and a.userid=afterclass_t.userid
 group by a.userid,
          a.seasonid
  ) fenceng_t
  on base_t.userid=fenceng_t.userid and base_t.seasonid=fenceng_t.seasonid