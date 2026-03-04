#!/usr/bin/env python3
"""
iTalent 考勤数据爬取工具
支持配置文件或交互式输入
"""

import requests
import json
import csv
import os
import sys
from datetime import datetime

# 配置文件路径
CONFIG_FILE = "config.json"


def load_config():
    """加载配置文件"""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_config(config):
    """保存配置文件"""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    print(f"✅ 配置已保存到 {CONFIG_FILE}")


def get_headers(cookie):
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Cookie": cookie,
        "Origin": "https://www.italent.cn",
        "Referer": "https://www.italent.cn/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }


def get_text(record, field, default=""):
    """获取字段文本值"""
    if field in record:
        return record[field].get("text", default)
    return default


def fetch_month_list(cookie, quark_s_monthly):
    """获取月份列表和月报汇总"""
    url = "https://cloud.italent.cn/api/v2/UI/TableList"
    params = {
        "viewName": "Attendance.SingleObjectListView.MyMonthlyTableList",
        "metaObjName": "Attendance.Monthly",
        "app": "Attendance",
        "PaaS-SourceApp": "Attendance",
        "PaaS-CurrentView": "Attendance.MyMonthly",
        "frontendVersion": "2025121900",
        "shadow_context": '{"appModel":"italent","uppid":"1"}',
        "_qsrcapp": "attendance",
        "quark_s": quark_s_monthly
    }
    payload = {
        "table_data": {
            "advance": {"cmp_render": {"viewPath": "MyMonthlyTableList", "status": "enable"}},
            "hasCheckColumn": False,
            "ext_data": {"ListViewLabel": "我的月报列表"},
            "isEnableGlobleCheck": False,
            "hasRowHandler": True,
            "paging": {"total": 0, "capacity": 15, "page": 0, "capacityList": [15, 30, 60, 100]},
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
                    "text": "", "value": "", "num": "2"},
                {"name": "Attendance.Monthly.StdIsDeleted",
                    "text": "否", "value": "0", "num": "4"},
                {"name": "Attendance.Monthly.MonthlyDataType",
                    "text": "明细", "value": "2", "num": "5"}
            ],
            "searchFormFilterJson": None
        }
    }

    try:
        resp = requests.post(url, headers=get_headers(
            cookie), params=params, json=payload, timeout=30)
        print(f"   状态码: {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("cmp_data", {}).get("data", [])
            if not records:
                records = data.get("biz_data", [])
            if not records:
                records = data.get("rows", [])

            print(f"   data 数量: {len(records)}")

            months = []
            month_ids = {}
            monthly_records = []

            for r in records:
                month_val = r.get("Month", {}).get("value", "")
                if not month_val:
                    month_val = r.get("Month", "")
                if month_val:
                    months.append(month_val)
                    monthly_records.append(r)
                    month_id = r.get("_id", {}).get("value", "")
                    if not month_id:
                        month_id = r.get("_id", "")
                    if month_id:
                        month_ids[month_val] = month_id

            if month_ids:
                with open("month_ids.json", "w") as f:
                    json.dump(month_ids, f)

            return months, monthly_records
        else:
            print(f"   响应: {resp.text[:200]}")
    except Exception as e:
        print(f"获取月份列表失败: {e}")
    return [], []


def fetch_daily_detail(cookie, quark_s_daily, month_id):
    """获取每日明细数据"""
    url = "https://cloud.italent.cn/api/v2/UI/TableList"
    params = {
        "metaObjName": "Attendance.EveryDayAttendanceDetails",
        "id": month_id,
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
        "quark_s": quark_s_daily
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
            "DataId": month_id,
            "currentDetailPageViewName": "Attendance.MyMonthlyDetail"
        },
        "moduleID": "Attendance.SingleObjectListView.AttendanceDetailListView"
    }

    try:
        resp = requests.post(url, headers=get_headers(
            cookie), params=params, json=payload, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("cmp_data", {}).get("data", [])
            if not records:
                records = data.get("biz_data", [])
            if not records:
                records = data.get("rows", [])
            return records
    except Exception as e:
        print(f"获取每日明细失败: {e}")
    return []


def save_month_list_csv(months, filename):
    """保存月份列表"""
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["序号", "月份"])
        for i, month in enumerate(months, 1):
            writer.writerow([i, month])
    print(f"✅ 月份列表已保存: {filename} ({len(months)}个月)")


def save_monthly_csv(all_records, filename):
    """保存月报汇总"""
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
    print(f"✅ 月报汇总已保存: {filename} ({len(all_records)}条)")


def save_daily_csv(all_records, filename):
    """保存每日明细"""
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
    print(f"✅ 每日明细已保存: {filename} ({len(all_records)}条)")


def interactive_input():
    """交互式输入配置"""
    print("\n" + "=" * 60)
    print("请输入 iTalent 认证信息")
    print("=" * 60)

    cookie = input("\n请输入 Cookie: ").strip()
    quark_s_monthly = input("请输入月报列表的 quark_s: ").strip()
    quark_s_daily = input("请输入每日明细的 quark_s: ").strip()

    config = {
        "cookie": cookie,
        "quark_s_monthly": quark_s_monthly,
        "quark_s_daily": quark_s_daily
    }

    save = input("\n是否保存配置供下次使用? (y/n): ").strip().lower()
    if save == 'y':
        save_config(config)

    return config


def main():
    print("=" * 70)
    print("          iTalent 考勤数据爬取工具")
    print("=" * 70)

    # 加载或输入配置
    config = load_config()

    if config.get("cookie"):
        print(f"\n发现已保存的配置文件: {CONFIG_FILE}")
        use_saved = input("是否使用已保存的配置? (y/n): ").strip().lower()
        if use_saved != 'y':
            config = interactive_input()
    else:
        config = interactive_input()

    cookie = config.get("cookie", "")
    quark_s_monthly = config.get("quark_s_monthly", "")
    quark_s_daily = config.get("quark_s_daily", "")

    if not cookie or not quark_s_monthly or not quark_s_daily:
        print("❌ 配置不完整，请重新输入")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. 获取月份列表和月报汇总
    print("\n📅 步骤1: 获取月份列表和月报汇总...")
    months, monthly_records = fetch_month_list(cookie, quark_s_monthly)
    print(f"   找到 {len(months)} 个月份")
    print(f"   月报汇总: {len(monthly_records)} 条")

    if not months:
        print("❌ 未获取到月份列表，请检查 Cookie 和 quark_s 是否正确")
        input("\n按回车键退出...")
        return

    # 2. 获取每日明细
    all_daily = []

    try:
        with open("month_ids.json", "r") as f:
            month_ids = json.load(f)
    except:
        month_ids = {}

    print("\n📅 步骤2: 获取每日明细...")
    for month in months:
        month_id = month_ids.get(month, "")
        print(
            f"\n📊 正在处理 {month}... (ID: {month_id[:8] if month_id else 'N/A'}...)")

        if not month_id:
            print("   ❌ 无ID，跳过")
            continue

        daily_records = fetch_daily_detail(cookie, quark_s_daily, month_id)
        all_daily.extend(daily_records)
        print(f"   明细: {len(daily_records)} 条")

    # 3. 生成文件
    print("\n💾 步骤3: 生成Excel文件...")

    month_list_file = f"考勤月份列表_{timestamp}.csv"
    monthly_file = f"考勤月报汇总_{timestamp}.csv"
    daily_file = f"考勤每日明细_{timestamp}.csv"

    save_month_list_csv(months, month_list_file)
    save_monthly_csv(monthly_records, monthly_file)
    save_daily_csv(all_daily, daily_file)

    print("\n" + "=" * 70)
    print("✅ 全部完成！")
    print(f"   月份列表: {len(months)} 个月")
    print(f"   月报汇总: {len(monthly_records)} 条记录")
    print(f"   每日明细: {len(all_daily)} 条记录")
    print("=" * 70)

    input("\n按回车键退出...")


if __name__ == "__main__":
    main()
