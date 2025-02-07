#!/bin/bash

# 获取当前系统的时区信息
local_timezone=$(date +%z)

# 计算上个月的第一天和最后一天（本地时间）
last_month_start_local=$(date -d "$(date +%Y-%m-01) -1 month" '+%Y-%m-%d 00:00:00')
last_month_end_local=$(date -d "$(date +%Y-%m-01) -1 day" '+%Y-%m-%d 23:59:59')

# 将本地时间转换为 UTC 时间
start_utc=$(TZ="UTC" date -d "${last_month_start_local} ${local_timezone}" '+%Y-%m-%dT%H:%M:%SZ')
end_utc=$(TZ="UTC" date -d "${last_month_end_local} ${local_timezone}" '+%Y-%m-%dT%H:%M:%SZ')

# 设置并输出用于传递给下游节点的参数
echo '${setValue(START="'$start_utc'")}'
echo '${setValue(END="'$end_utc'")}'