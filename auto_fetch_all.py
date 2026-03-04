#!/usr/bin/env python3
"""
iTalent 考勤数据全自动爬取脚本
流程：获取月份列表 → 循环获取每月汇总+每日明细 → 生成Excel
"""

import requests
import json
import csv
import urllib.parse
from datetime import datetime

# 配置
BASE_URL = "https://cloud.italent.cn"
COOKIE = "key-154722667=false; Tita_PC=OD_LS5vIwo8IRMnR2_Rh1kD3B6sy9p6s7hdoGdQTawQKcJjqPKyKoWJMDmfiHG56; ssn_Tita_PC=OD_LS5vIwo8IRMnR2_Rh1kD3B6sy9p6s7hdoGdQTawQKcJjqPKyKoWJMDmfiHG56; 00000000-0000-0000-0000-000000000000.widgetState=false; user_polling_timespace_107977=0"
USER_ID = "154722667"


def get_headers():
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Cookie": COOKIE,
        "Origin": "https://www.italent.cn",
        "Referer": "https://www.italent.cn/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }


def fetch_month_list():
    """获取月份列表"""
    url = f"{BASE_URL}/api/v2/UI/TableList"
    params = {
        "viewName": "Attendance.SingleObjectListView.MyMonthlyTableList",
        "metaObjName": "Attendance.Monthly",
        "app": "Attendance",
        "PaaS-SourceApp": "Attendance",
        "PaaS-CurrentView": "Attendance.MyMonthly",
        "frontendVersion": "2025121900",
        "shadow_context": '{"appModel":"italent","uppid":"1"}',
        "_qsrcapp": "attendance",
        "quark_s": "5e48745f7a4c5448a104850c2e99ebbb6e44ce4fb6e4255e4470d648fbf97527"
    }
    payload = {
        "table_data": {
            "advance": {"cmp_render": {"viewPath": "MyMonthlyTableList", "status": "enable"}},
            "hasCheckColumn": False,
            "ext_data": {"ListViewLabel": "我的月报列表"},
            "isEnableGlobleCheck": False,
            "hasRowHandler": True,
            "paging": {"total": 0, "capacity": 100, "page": 0, "capacityList": [15, 30, 60, 100]},
            "isAvatars": True,
            "viewName": "Attendance.SingleObjectListView.MyMonthlyTableList",
            "operateColumWidth": 140,
            "extendsParam": "",
            "isSyncRowHandler": True,
            "isFrozenOperationColumnHandler": True,
            "isCustomListViewExisted": False,
            "getTreeNodeUrl": None,
            "sort_fields": [{"sort_column": "Month", "sort_dir": "desc"}, {"sort_column": "OId", "sort_dir": "asc"}],
            "description": "我的月报列表",
            "metaObjName": "Attendance.Monthly",
            "isCustomListView": False,
            "navViewIsCustom": False,
            "navViewName": "Attendance.MyMonthly",
            "navViewVersion": "0"
        },
        "search_data": {
            "metaObjName": "Attendance.Monthly",
            "searchView": "Attendance.MyMonthlySearchForm",
            "items": [
                {"name": "Attendance.Monthly.UserId",
                    "text": "王李逍(wanglx@geovis.com.cn)", "value": USER_ID, "num": "2"},
                {"name": "Attendance.Monthly.StdIsDeleted",
                    "text": "否", "value": "0", "num": "4"},
                {"name": "Attendance.Monthly.MonthlyDataType",
                    "text": "明细", "value": "2", "num": "5"}
            ],
            "searchFormFilterJson": None
        }
    }

    try:
        resp = requests.post(url, headers=get_headers(),
                             params=params, json=payload, timeout=30)
        print(f"   状态码: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            # 尝试多种数据位置
            records = data.get("cmp_data", {}).get("data", [])
            if not records:
                records = data.get("biz_data", [])
            if not records:
                records = data.get("rows", [])

            print(f"   data 数量: {len(records)}")

            months = []
            month_ids = {}
            monthly_records = []  # 月报数据

            for r in records:
                month_val = r.get("Month", {}).get("value", "")
                if not month_val:
                    month_val = r.get("Month", "")
                if month_val:
                    months.append(month_val)
                    # 保存月报数据
                    monthly_records.append(r)
                    # 获取 ID
                    month_id = r.get("_id", {}).get("value", "")
                    if not month_id:
                        month_id = r.get("_id", "")
                    if month_id:
                        month_ids[month_val] = month_id

            # 保存月份ID映射供后续使用
            if month_ids:
                with open("month_ids.json", "w") as f:
                    json.dump(month_ids, f)
                print(f"   月份ID已保存到 month_ids.json")

            # 保存月报数据
            if monthly_records:
                with open("monthly_data.json", "w", encoding="utf-8") as f:
                    json.dump(monthly_records, f, ensure_ascii=False, indent=2)
                print(f"   月报数据已保存到 monthly_data.json")

            return months, monthly_records
        else:
            print(f"   响应: {resp.text[:200]}")
    except Exception as e:
        print(f"获取月份列表失败: {e}")
    return [], []


def fetch_monthly_detail(month, month_id):
    """获取月报汇总数据（备用，实际上月份列表已包含月报数据）"""
    url = f"{BASE_URL}/api/v2/UI/TableList"
    params = {
        "metaObjName": "Attendance.Monthly",
        "id": month_id,  # 使用月份ID
        "listViewName": "Attendance.SingleObjectListView.MyMonthlyTableList",
        "bc_sign": "b209724b0a51aac7c1a0e00611838b9edf1fa875",
        "bc_ts": "1772591431307",
        "bc_nonce": "v7BxvA",
        "viewName": "Attendance.SingleObjectListView.MyMonthlyTableListForDetailPage",
        "PaaS-SourceApp": "Attendance",
        "app": "Attendance",
        "frontendVersion": "2025121900",
        "shadow_context": '{"appModel":"italent","uppid":"1"}',
        "_qsrcapp": "attendance",
        "quark_s": "3256b27ef01de74126bec624511d79e1991d56504316a1115db13f25275bfd43"
    }
    payload = {
        "table_data": {
            "advance": {"cmp_render": {"viewPath": "MyMonthlyNewTableList", "status": "enable"}},
            "hasCheckColumn": False,
            "ext_data": {"ListViewLabel": "我的月报列表详情页用"},
            "isEnableGlobleCheck": False,
            "hasRowHandler": False,
            "paging": {"total": 0, "capacity": 15, "page": 0, "capacityList": [15, 30, 60, 100]},
            "isAvatars": True,
            "viewName": "Attendance.SingleObjectListView.MyMonthlyTableListForDetailPage",
            "operateColumWidth": 140,
            "extendsParam": "",
            "isSyncRowHandler": False,
            "isFrozenOperationColumnHandler": False,
            "isCustomListViewExisted": False,
            "getTreeNodeUrl": None,
            "sort_fields": [{"sort_column": "Month", "sort_dir": "desc"}, {"sort_column": "OId", "sort_dir": "asc"}],
            "description": "我的月报列表 放在月报详情页中",
            "metaObjName": "Attendance.Monthly",
            "isCustomListView": False
        },
        "page_context": {
            "metaObjName": "Attendance.Monthly",
            "viewName": "AttendanceEmployeeIdentityAttendance.MonthlyDetail",
            "DataId": month_id,  # 使用月份ID
            "currentDetailPageViewName": "Attendance.MyMonthlyDetail"
        },
        "moduleID": "Attendance.SingleObjectListView.MyMonthlyTableListForDetailPage"
    }

    try:
        resp = requests.post(url, headers=get_headers(),
                             params=params, json=payload, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"获取月报数据失败: {e}")
    return None


def fetch_daily_detail(month, month_id):
    """获取每日明细数据"""
    url = f"{BASE_URL}/api/v2/UI/TableList"
    params = {
        "metaObjName": "Attendance.EveryDayAttendanceDetails",
        "id": month_id,  # 使用月份ID
        "listViewName": "Attendance.SingleObjectListView.MyMonthlyTableList",
        "bc_sign": "1da23ce3b10186a5c8fdaafcf496799606eaa03d",
        "bc_ts": "1772598490215",
        "bc_nonce": "dk6ayb",
        "viewName": "Attendance.SingleObjectListView.AttendanceDetailListView",
        "PaaS-SourceApp": "Attendance",
        "app": "Attendance",
        "frontendVersion": "2025121900",
        "shadow_context": '{"appModel":"italent","uppid":"1"}',
        "_qsrcapp": "attendance",
        "quark_s": "73dca058eeb2a532d880b41d99fadfc06c6f52f4bc0a060f75b50453d344ec6d"
    }
    payload = {
        "table_data": {
            "advance": {"cmp_render": {"viewPath": "MyMonthlyDailyNewTableList", "status": "enable"}},
            "hasCheckColumn": False,
            "ext_data": {"ListViewLabel": "日出勤情况列表"},
            "isEnableGlobleCheck": False,
            "hasRowHandler": False,
            "paging": {"total": 0, "capacity": 30, "page": 0, "capacityList": [15, 30, 60, 100]},
            "isAvatars": True,
            "viewName": "Attendance.SingleObjectListView.AttendanceDetailListView",
            "operateColumWidth": 140,
            "extendsParam": "",
            "isSyncRowHandler": True,
            "isFrozenOperationColumnHandler": False,
            "isCustomListViewExisted": False,
            "getTreeNodeUrl": None,
            "sort_fields": [{"sort_column": "Date", "sort_dir": "desc"}, {"sort_column": "OId", "sort_dir": "asc"}],
            "description": "日出勤情况列表",
            "metaObjName": "Attendance.EveryDayAttendanceDetails",
            "isCustomListView": True
        },
        "page_context": {
            "metaObjName": "Attendance.Monthly",
            "viewName": "AttendanceEmployeeIdentityAttendance.MonthlyDetail",
            "DataId": month_id,  # 使用月份ID
            "currentDetailPageViewName": "Attendance.MyMonthlyDetail"
        },
        "moduleID": "Attendance.SingleObjectListView.AttendanceDetailListView"
    }

    try:
        resp = requests.post(url, headers=get_headers(),
                             params=params, json=payload, timeout=30)
        print(f"      状态码: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            # 尝试多种数据位置
            records = data.get("cmp_data", {}).get("data", [])
            if not records:
                records = data.get("biz_data", [])
            if not records:
                records = data.get("rows", [])
            print(f"      记录数: {len(records)}")
            return records  # 直接返回记录数组
    except Exception as e:
        print(f"获取每日明细失败: {e}")
    return []


def extract_records(data):
    """提取数据记录"""
    if not data:
        return []
    return data.get("cmp_data", {}).get("data", [])


def get_text(record, field, default=""):
    """获取字段文本值"""
    if field in record:
        return record[field].get("text", default)
    return default


def save_month_list_csv(months, filename):
    """保存月份列表CSV"""
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["序号", "月份"])
        for i, month in enumerate(months, 1):
            writer.writerow([i, month])
    print(f"✅ 月份列表已保存: {filename} ({len(months)}个月)")


def save_monthly_csv(all_records, filename):
    """保存月报汇总CSV"""
    fields = [
        ("Month", "月份"),
        ("UserId", "员工"),
        ("StartDate", "开始日期"),
        ("StopDate", "结束日期"),
        ("TargetDays", "应出勤天数"),
        ("HolidayDays", "节假日天数"),
        ("ActualDays", "实际出勤天数"),
        ("WorkPeriodExcludeRestTime", "实出勤工时(小时)"),
        ("LateDuration", "迟到时长(分钟)"),
        ("LateTimes", "迟到次数"),
        ("LeaveEarlyDuration", "早退时长(分钟)"),
        ("LeaveEarlyTimes", "早退次数"),
        ("AbsenteeismDuration", "旷工时长"),
        ("AnnualLeave", "年假"),
        ("SickLeave", "病假"),
        ("MarriageLeave", "婚假"),
        ("MaternityLeave", "产假"),
        ("BereavementLeave", "丧假"),
        ("BirthSeizeLeave", "生育假"),
        ("PaternityLeave", "陪产假"),
        ("Business", "出差"),
        ("AttendancePlanOId", "考勤方案"),
        ("AffirmStatus", "确认状态"),
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([f[1] for f in fields])

        for record in all_records:
            row = []
            for field, _ in fields:
                value = get_text(record, field)
                if field == "UserId" and value:
                    value = value.split("(")[0] if "(" in value else value
                row.append(value)
            writer.writerow(row)

    print(f"✅ 月报汇总已保存: {filename} ({len(all_records)}条记录)")


def save_daily_csv(all_records, filename):
    """保存每日明细CSV"""
    fields = [
        ("Date", "日期"),
        ("UserId", "员工"),
        ("AttendanceOrg", "考勤组织"),
        ("DateType", "班次类型"),
        ("TargetDays", "应出勤天数"),
        ("ActualDays", "实际出勤天数"),
        ("WorkDuration", "工作时长(小时)"),
        ("WorkPeriod", "工作时段(小时)"),
        ("AStatusAbnormal", "异常次数"),
        ("AStatusNormal", "正常次数"),
        ("LateTimes", "迟到次数"),
        ("LateDuration", "迟到时长(分钟)"),
        ("LeaveEarlyTimes", "早退次数"),
        ("LeaveEarlyDuration", "早退时长(分钟)"),
        ("AbsenteeismDuration", "旷工时长(小时)"),
        ("OutTimes", "外出次数"),
        ("OutDuration", "外出时长(小时)"),
        ("AnnualLeave", "年假(天)"),
        ("SickLeave", "病假(天)"),
        ("MarriageLeave", "婚假(天)"),
        ("MaternityLeave", "产假(天)"),
        ("BereavementLeave", "丧假(天)"),
        ("PaternityLeave", "陪产假(天)"),
        ("Business", "出差(天)"),
        ("SealingStatus", "封存状态"),
        ("CalculatedState", "计算状态"),
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([f[1] for f in fields])

        for record in all_records:
            row = []
            for field, _ in fields:
                value = get_text(record, field)
                if field == "UserId" and value:
                    value = value.split("(")[0] if "(" in value else value
                row.append(value)
            writer.writerow(row)

    print(f"✅ 每日明细已保存: {filename} ({len(all_records)}条记录)")


def fetch_swiping_card_records(month, dates, user_id, quark_s_statistics):
    """获取打卡记录数据"""
    url = f"{BASE_URL}/api/v2/UI/TableList"
    params = {
        "viewName": "Attendance.SingleObjectListView.MyStatisticsEmpAttendanceDataList",
        "metaObjName": "Attendance.AttendanceStatistics",
        "app": "Attendance",
        "shadow_context": '{"appModel":"italent","uppid":"1"}',
        "frontendVersion": "2025061300",
        "_qsrcapp": "attendance",
        "quark_s": quark_s_statistics
    }

    # 将日期列表转换为逗号分隔的字符串
    dates_str = ",".join(dates) if dates else ""

    payload = {
        "table_data": {
            "advance": {"cmp_render": {"viewPath": "", "status": "enable"}, "layout": {}},
            "hasCheckColumn": True,
            "isEnableGlobleCheck": False,
            "hasRowHandler": True,
            "paging": {"capacity": "15", "page": "0", "total": 0, "capacityList": [15, 30, 60, 100]},
            "isAvatars": True,
            "viewName": "Attendance.SingleObjectListView.MyStatisticsEmpAttendanceDataList",
            "metaObjName": "Attendance.AttendanceStatistics",
            "isCustomListView": True
        },
        "search_data": {
            "metaObjName": "Attendance.AttendanceStatistics",
            "searchView": "Attendance.AttendanceStatisticsSearchView",
            "items": [
                {"name": "Attendance.AttendanceStatistics.StaffId", "value": user_id},
                {"name": "Attendance.AttendanceStatistics.StdIsDeleted", "value": "0"},
                {"name": "Attendance.AttendanceStatistics.Status", "value": "1"},
                {"name": "Attendance.AttendanceStatistics.SwipingCardDate",
                    "value": dates_str}
            ],
            "searchFormFilterJson": None
        }
    }

    try:
        resp = requests.post(url, headers=get_headers(),
                             params=params, json=payload, timeout=30)
        print(f"      状态码: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("cmp_data", {}).get("data", [])
            if not records:
                records = data.get("biz_data", [])
            if not records:
                records = data.get("rows", [])

            # 保存原始数据供调试
            if month == "2026-03":
                with open("swiping_card_sample.json", "w", encoding="utf-8") as f:
                    json.dump(records[:2], f, ensure_ascii=False, indent=2)

            print(f"      打卡记录数: {len(records)}")
            return records
    except Exception as e:
        print(f"获取打卡记录失败: {e}")
    return []


def save_swiping_card_csv(all_records, filename):
    """保存打卡记录CSV"""
    fields = [
        ("SwipingCardDate", "打卡日期"),
        ("StaffId", "员工"),
        ("DateType", "日期类型"),
        ("OIdWorkShift", "班次"),
        ("ActualForFirstCard", "首次打卡时间"),
        ("ActualForLastCard", "最后打卡时间"),
        ("WorkPeriod", "工作时段(小时)"),
        ("AttendanceStatus", "考勤状态"),
        ("AttendanceStatusDetail", "考勤状态详情"),
        ("AbsenceDuration", "缺勤时长(分钟)"),
        ("ModifiedTime", "最后修改时间"),
    ]

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([f[1] for f in fields])

        for record in all_records:
            row = []
            for field, _ in fields:
                value = get_text(record, field)
                if field == "StaffId" and value:
                    value = value.split("(")[0] if "(" in value else value
                row.append(value)
            writer.writerow(row)

    print(f"✅ 打卡记录已保存: {filename} ({len(all_records)}条记录)")


def extract_dates_from_daily(daily_records):
    """从每日明细中提取出勤日期"""
    dates = []
    for record in daily_records:
        date_val = record.get("Date", {})
        if isinstance(date_val, dict):
            date_str = date_val.get("value", "") or date_val.get("text", "")
        else:
            date_str = date_val
        if date_str:
            # 转换日期格式 2026/03/01 -> 2026-03-01
            date_str = date_str.replace("/", "-")
            dates.append(date_str)
    return dates


if __name__ == "__main__":
    print("=" * 70)
    print("iTalent 考勤数据全自动爬取")
    print("=" * 70)

    # 打卡记录接口的 quark_s（需要用户提供）
    QUARK_S_STATISTICS = "09165bd736fc409cc09d6d3f7c216e620bb70f47464880b30fd7ee536e445587"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. 获取月份列表（同时包含月报汇总数据）
    print("\n📅 步骤1: 获取月份列表和月报汇总...")
    months, monthly_records = fetch_month_list()
    print(f"   找到 {len(months)} 个月份")

    if not months:
        print("❌ 未获取到月份列表，退出")
        exit()

    # 月报数据已在月份列表中获取
    all_monthly = monthly_records
    print(f"   月报汇总: {len(all_monthly)} 条")

    # 2. 循环获取每日明细
    all_daily = []

    # 读取月份ID映射
    try:
        with open("month_ids.json", "r") as f:
            month_ids = json.load(f)
    except:
        month_ids = {}

    print("\n📅 步骤2: 获取每日明细...")
    for month in months:
        month_id = month_ids.get(month, "")
        print(f"\n📊 正在处理 {month} 的每日明细... (ID: {month_id[:8]}...)")

        if not month_id:
            print("   ❌ 无ID，跳过")
            continue

        # 获取每日明细
        daily_records = fetch_daily_detail(month, month_id)
        all_daily.extend(daily_records)
        print(f"   明细: {len(daily_records)} 条")

    # 3. 获取打卡记录
    all_swiping_cards = []

    print("\n📅 步骤3: 获取打卡记录...")
    # 按月份分组每日明细，然后获取打卡记录
    daily_by_month = {}
    for record in all_daily:
        date_val = record.get("Date", {})
        if isinstance(date_val, dict):
            date_str = date_val.get("value", "") or date_val.get("text", "")
        else:
            date_str = date_val
        if date_str:
            month_key = date_str[:7].replace("/", "-")  # 2026/03 -> 2026-03
            if month_key not in daily_by_month:
                daily_by_month[month_key] = []
            daily_by_month[month_key].append(record)

    for month, daily_records in daily_by_month.items():
        print(f"\n📊 正在处理 {month} 的打卡记录...")

        # 提取日期
        dates = extract_dates_from_daily(daily_records)
        if not dates:
            print("   ❌ 无日期数据，跳过")
            continue

        print(f"   出勤日期: {len(dates)} 天")

        # 获取打卡记录
        swiping_records = fetch_swiping_card_records(
            month, dates, USER_ID, QUARK_S_STATISTICS)
        all_swiping_cards.extend(swiping_records)

    # 4. 生成Excel/CSV
    print("\n💾 步骤4: 生成Excel文件...")

    month_list_file = f"考勤月份列表_{timestamp}.csv"
    monthly_file = f"考勤月报汇总_全部_{timestamp}.csv"
    daily_file = f"考勤每日明细_全部_{timestamp}.csv"
    swiping_file = f"考勤打卡记录_全部_{timestamp}.csv"

    save_month_list_csv(months, month_list_file)
    save_monthly_csv(all_monthly, monthly_file)
    save_daily_csv(all_daily, daily_file)
    save_swiping_card_csv(all_swiping_cards, swiping_file)

    print("\n" + "=" * 70)
    print("✅ 全部完成！")
    print(f"   月份列表: {len(months)} 个月")
    print(f"   月报汇总: {len(all_monthly)} 条记录")
    print(f"   每日明细: {len(all_daily)} 条记录")
    print(f"   打卡记录: {len(all_swiping_cards)} 条记录")
    print("=" * 70)
