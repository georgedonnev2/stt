import re
from typing import Dict, Union


def parse_command_response(response: str) -> Dict[str, Union[int, str]]:
    """
    解析机器人命令响应字符串，返回结构化字典

    参数:
        response: 命令响应字符串，格式应为 "ErrorID,{ResultID},CommandString"
        例如:
            "0,{1},MovL(P,user,tool,a,v|speed,cp|r)"
            "-1,{},InverseKin(0,0,0,0,0,0);"

    返回:
        包含解析结果的字典，结构:
        {
            "error_id": int,     # 错误码 (如 0 或 -1)
            "value": str,        # 结果值 (可能为空字符串)
            "command": str       # 执行的原始命令
        }

    异常:
        ValueError: 当响应格式不符合预期时抛出
    """
    if not isinstance(response, str):
        raise ValueError("响应必须是字符串类型")

    # 修改后的正则表达式，允许 ErrorID 为负数，允许 {} 为空
    pattern = r"^\s*(-?\d+)\s*,\s*{(.*?)}\s*,\s*(.*)\s*$"
    match = re.fullmatch(pattern, response)

    if not match:
        raise ValueError(
            f"无效的响应格式: '{response}'。预期格式: 'ErrorID,{{ResultID}},Command'"
        )

    try:
        error_id = int(match.group(1))  # 现在支持负数
    except ValueError:
        raise ValueError(f"ErrorID 必须是整数，得到: '{match.group(1)}'")

    return {
        "error_id": error_id,
        "value": match.group(2).strip(),  # 即使为空也会返回空字符串
        "command": match.group(3).strip(),
    }


# 更新后的测试用例
responses = [
    "0, {1}, MovL(P,user,tool,a,v|speed,cp|r)",  # 正常带值
    "-1, {}, InverseKin(0.000000,0.000000,0.000000,0.000000,0.000000,0.000000);",  # 负数ErrorID + 空值
    "2, {  }, SetPayload(5.0)",  # 只有空格
    "-3, {error}, EmergencyStop()",  # 负数ErrorID + 值
    "invalid_format",  # 无效格式
    "0, {unclosed, MovL()",  # 不闭合括号
]

for resp in responses:
    try:
        parsed = parse_command_response(resp)
        print(f"解析成功: {parsed}")
    except ValueError as e:
        print(f"解析失败: '{resp}' -> {e}")
