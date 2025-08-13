#!/bin/bash

# LTP 测试汇总脚本
# 用法: ./ltp_summary.sh <ltp_output_file>
# 或者: /opt/ltp/runltp -d /curvine-fuse -f fs | tee ltp_output.txt && ./ltp_summary.sh ltp_output.txt

if [ $# -eq 0 ]; then
    echo "用法: $0 <ltp_output_file>"
    echo "示例: $0 ltp_output.txt"
    exit 1
fi

OUTPUT_FILE=$1

if [ ! -f "$OUTPUT_FILE" ]; then
    echo "错误: 文件 $OUTPUT_FILE 不存在"
    exit 1
fi

echo "正在分析 LTP 测试结果..."
echo "-----------------------------------------------"

# 统计各种结果
TOTAL_TESTS=$(grep -c "<<<test_start>>>" "$OUTPUT_FILE")
PASSED=$(grep -c "TPASS" "$OUTPUT_FILE")
FAILED=$(grep -c "TFAIL\|FAIL" "$OUTPUT_FILE")
SKIPPED=$(grep -c "TCONF\|CONF" "$OUTPUT_FILE")
BROKEN=$(grep -c "TBROK\|BROK" "$OUTPUT_FILE")
WARNINGS=$(grep -c "TWARN\|WARN" "$OUTPUT_FILE")

# 获取失败的测试用例名称
echo "测试结果汇总:"
echo "-----------------------------------------------"
if [ $FAILED -gt 0 ]; then
    echo "失败的测试用例:"
    grep -B1 "TFAIL\|FAIL" "$OUTPUT_FILE" | grep "tag=" | sed 's/.*tag=\([^ ]*\).*/  - \1/' | sort -u
    echo ""
fi

if [ $BROKEN -gt 0 ]; then
    echo "中断的测试用例:"
    grep -B1 "TBROK\|BROK" "$OUTPUT_FILE" | grep "tag=" | sed 's/.*tag=\([^ ]*\).*/  - \1/' | sort -u
    echo ""
fi

# 显示汇总统计
echo "-----------------------------------------------"
echo "Total Tests:         $TOTAL_TESTS"
echo "Total Passed:        $PASSED"
echo "Total Failed:        $FAILED"
echo "Total Broken:        $BROKEN"
echo "Total Skipped:       $SKIPPED"
echo "Total Warnings:      $WARNINGS"

# 获取系统信息
KERNEL_VERSION=$(uname -r)
MACHINE_ARCH=$(uname -m)

echo "Kernel Version:      $KERNEL_VERSION"
echo "Machine Architecture: $MACHINE_ARCH"
echo "-----------------------------------------------"

# 计算成功率
if [ $TOTAL_TESTS -gt 0 ]; then
    SUCCESS_RATE=$(echo "scale=2; ($PASSED * 100) / $TOTAL_TESTS" | bc)
    echo "Success Rate:        ${SUCCESS_RATE}%"
fi

# 返回值：如果有失败的测试，返回非零值
if [ $FAILED -gt 0 ] || [ $BROKEN -gt 0 ]; then
    exit 1
fi

exit 0