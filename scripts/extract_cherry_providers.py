#!/usr/bin/env python3
"""
从 CherryStudio 导出的 data.json 中提取模型/提供商配置。

用法:
  python scripts/extract_cherry_providers.py              # 输出到 cherry_providers.json
  python scripts/extract_cherry_providers.py --yaml       # 同时生成 YAML 片段到 cherry_providers.yaml
  python scripts/extract_cherry_providers.py --data /path/to/data.json
"""

import argparse
import json
import os


def load_cherry_data(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data


def extract_providers(data: dict) -> list:
    """从 CherryStudio 导出的 data 中解析出 providers 列表。"""
    raw = data["localStorage"]["persist:cherry-studio"]
    inner = json.loads(raw)
    llm = json.loads(inner["llm"]) if isinstance(inner["llm"], str) else inner["llm"]
    providers = llm["providers"]
    if isinstance(providers, str):
        providers = json.loads(providers)
    return providers


def main():
    parser = argparse.ArgumentParser(description="从 CherryStudio data.json 提取 provider 配置")
    parser.add_argument(
        "--data",
        default=os.path.join(os.path.dirname(__file__), "..", "data.json"),
        help="CherryStudio 导出的 data.json 路径",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="输出 JSON 路径，默认与 data 同目录下的 cherry_providers.json",
    )
    parser.add_argument(
        "--yaml",
        action="store_true",
        help="额外生成适配 LLMBalancer providers.yaml 的 YAML 片段",
    )
    parser.add_argument(
        "--enabled-only",
        action="store_true",
        help="只输出 enabled=true 的 provider",
    )
    args = parser.parse_args()

    data_path = os.path.abspath(args.data)
    if not os.path.isfile(data_path):
        print(f"错误: 文件不存在 {data_path}")
        return 1

    data = load_cherry_data(data_path)
    providers = extract_providers(data)

    if args.enabled_only:
        providers = [p for p in providers if p.get("enabled", True)]

    out_path = args.out
    if out_path is None:
        out_path = os.path.join(os.path.dirname(data_path), "cherry_providers.json")
    out_path = os.path.abspath(out_path)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(providers, f, ensure_ascii=False, indent=2)

    print(f"已提取 {len(providers)} 个 provider -> {out_path}")

    if args.yaml:
        yaml_path = out_path.replace(".json", ".yaml")
        if yaml_path == out_path:
            yaml_path = os.path.join(os.path.dirname(out_path), "cherry_providers.yaml")
        lines = [
            "# 从 CherryStudio 提取的 provider，可复制到 providers.yaml",
            "# 每个 (provider, model) 一条记录，需根据实际填写 level / rpm_limit 等",
            "providers:",
        ]
        for p in providers:
            p_name = (p.get("name") or p.get("id", "")).replace('"', '\\"')
            base_url = (p.get("apiHost") or "").rstrip("/")
            if not base_url.endswith("/v1"):
                base_url = f"{base_url}/v1" if base_url else ""
            api_key = p.get("apiKey") or "not-needed"
            models = p.get("models") or []
            if not models:
                # 无模型时保留一条，用 provider 作为一条
                pid = p.get("id", "")
                lines.append(f'  - provider_id: "{pid}"  # {p_name}')
                lines.append(f'    base_url: "{base_url}"')
                lines.append(f'    api_key: "{api_key}"')
                lines.append('    model_name: "gpt-4o"')
                lines.append("    level: 5")
                lines.append("    rpm_limit: 0")
                lines.append("    max_concurrent: 2")
                lines.append("    timeout_seconds: 120")
                lines.append("")
                continue
            for m in models:
                model_id = m.get("id") or m.get("name") or "unknown"
                # 每条记录唯一：provider_id__model_id，方便区分同一 API 下多模型
                provider_id = f"{p.get('id', '')}__{model_id}"
                comment = f"{p_name} / {model_id}"
                lines.append(f'  - provider_id: "{provider_id}"  # {comment}')
                lines.append(f'    base_url: "{base_url}"')
                lines.append(f'    api_key: "{api_key}"')
                lines.append(f'    model_name: "{model_id}"')
                lines.append("    level: 5")
                lines.append("    rpm_limit: 0")
                lines.append("    max_concurrent: 2")
                lines.append("    timeout_seconds: 120")
                lines.append("")
        with open(yaml_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"已生成 YAML 片段 -> {yaml_path}")

    return 0


if __name__ == "__main__":
    exit(main())
