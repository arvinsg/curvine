#!/bin/bash

# LTP 测试运行和汇总脚本
# 用法: ./run_ltp_with_summary.sh [-d tmpdir] [-f test_suite]

# 默认参数
TMPDIR="/curvine-fuse"
TEST_SUITE="fs"
LTP_PATH="/opt/ltp"
OUTPUT_FILE="ltp_output_$(date +%Y%m%d_%H%M%S).txt"

# 解析命令行参数
while getopts "d:f:p:h" opt; do
    case $opt in
        d)
            TMPDIR="$OPTARG"
            ;;
        f)
            TEST_SUITE="$OPTARG"
            ;;
        p)
            LTP_PATH="$OPTARG"
            ;;
        h)
            echo "用法: $0 [-d tmpdir] [-f test_suite] [-p ltp_path]"
            echo "  -d tmpdir     : 临时目录路径 (默认: /curvine-fuse)"
            echo "  -f test_suite : 测试套件名称 (默认: fs)"
            echo "  -p ltp_path   : LTP 安装路径 (默认: /opt/ltp)"
            echo "  -h            : 显示帮助信息"
            exit 0
            ;;
        \?)
            echo "无效选项: -$OPTARG" >&2
            exit 1
            ;;
    esac
done

# 检查 LTP 是否安装
if [ ! -d "$LTP_PATH" ]; then
    echo "错误: LTP 未安装在 $LTP_PATH"
    echo "请先安装 LTP 或使用 -p 选项指定正确的路径"
    exit 1
fi

# 实时处理函数
process_output() {
    local passed=0
    local failed=0
    local broken=0
    local skipped=0
    local warnings=0
    local current_test=""
    local failed_tests=""
    local broken_tests=""
    
    while IFS= read -r line; do
        echo "$line" | tee -a "$OUTPUT_FILE"
        
        # 检测测试开始
        if [[ $line == *"<<<test_start>>>"* ]]; then
            current_test=""
        fi
        
        # 获取测试名称
        if [[ $line == *"tag="* ]]; then
            current_test=$(echo "$line" | sed 's/.*tag=\([^ ]*\).*/\1/')
        fi
        
        # 统计结果
        if [[ $line == *"TPASS"* ]]; then
            ((passed++))
        elif [[ $line == *"TFAIL"* ]] || [[ $line == *"FAIL"* ]]; then
            ((failed++))
            if [ -n "$current_test" ]; then
                failed_tests="$failed_tests $current_test"
            fi
        elif [[ $line == *"TBROK"* ]] || [[ $line == *"BROK"* ]]; then
            ((broken++))
            if [ -n "$current_test" ]; then
                broken_tests="$broken_tests $current_test"
            fi
        elif [[ $line == *"TCONF"* ]] || [[ $line == *"CONF"* ]]; then
            ((skipped++))
        elif [[ $line == *"TWARN"* ]] || [[ $line == *"WARN"* ]]; then
            ((warnings++))
        fi
        
        # 每10个测试显示一次进度
        total=$((passed + failed + broken + skipped))
        if [ $((total % 10)) -eq 0 ] && [ $total -gt 0 ]; then
            echo ""
            echo "===== 进度汇总 ====="
            echo "已完成测试: $total"
            echo "通过: $passed | 失败: $failed | 中断: $broken | 跳过: $skipped | 警告: $warnings"
            echo "===================="
            echo ""
        fi
    done
    
    # 最终汇总
    echo ""
    echo "======================================="
    echo "           LTP 测试完成汇总"
    echo "======================================="
    
    if [ -n "$failed_tests" ]; then
        echo "失败的测试用例:"
        for test in $failed_tests; do
            echo "  - $test"
        done
        echo ""
    fi
    
    if [ -n "$broken_tests" ]; then
        echo "中断的测试用例:"
        for test in $broken_tests; do
            echo "  - $test"
        done
        echo ""
    fi
    
    total=$((passed + failed + broken + skipped))
    
    echo "---------------------------------------"
    echo "Total Tests:         $total"
    echo "Total Passed:        $passed"
    echo "Total Failed:        $failed"
    echo "Total Broken:        $broken"
    echo "Total Skipped:       $skipped"
    echo "Total Warnings:      $warnings"
    echo "Kernel Version:      $(uname -r)"
    echo "Machine Architecture: $(uname -m)"
    echo "---------------------------------------"
    
    if [ $total -gt 0 ]; then
        success_rate=$(echo "scale=2; ($passed * 100) / $total" | bc)
        echo "Success Rate:        ${success_rate}%"
    fi
    
    echo "详细日志已保存到: $OUTPUT_FILE"
    echo "======================================="
    
    # 返回值
    if [ $failed -gt 0 ] || [ $broken -gt 0 ]; then
        return 1
    fi
    return 0
}

# 运行 LTP 测试
echo "开始运行 LTP 测试..."
echo "测试套件: $TEST_SUITE"
echo "临时目录: $TMPDIR"
echo "---------------------------------------"

# 运行测试并实时处理输出
if [ -x "$LTP_PATH/runltp" ]; then
    "$LTP_PATH/runltp" -d "$TMPDIR" -f "$TEST_SUITE" 2>&1 | process_output
    exit_code=$?
elif [ -x "$LTP_PATH/kirk" ]; then
    # 如果有 kirk，使用 kirk
    "$LTP_PATH/kirk" -U ltp -f "$TEST_SUITE" -d "$TMPDIR" 2>&1 | process_output
    exit_code=$?
else
    echo "错误: 找不到 runltp 或 kirk 执行文件"
    exit 1
fi

exit $exit_code