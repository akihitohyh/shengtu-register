"""生图注册机 — ChatGPT 账号注册，只输出 access_token。

用法:
    python main.py -n 10              # 注册 10 个账号
    python main.py -n 5 -c cfg.yaml   # 指定配置文件
    python main.py -n 3 -o at.txt     # 指定输出文件

输出: 每行一个 access_token 的 txt 文件（默认 access_tokens.txt）
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import yaml

from register.registrar import register_worker

logger = logging.getLogger("register")


def load_config(path: str = "config.yaml") -> dict:
    """加载配置文件，合并默认值。"""
    defaults = {
        "proxy": {"url": "", "flaresolverr_url": ""},
        "registration": {"threads": 2, "total": 10},
        "mail": {
            "providers": [{
                "type": "outlook_token",
                "enable": True,
                "mode": "graph",
                "mailboxes": "",
            }],
            "request_timeout": 30,
            "wait_timeout": 45,
            "wait_interval": 3,
        },
        "output_file": "access_tokens.txt",
    }

    cfg = dict(defaults)
    config_path = Path(path)
    if config_path.exists():
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            _deep_merge(cfg, raw)
    return cfg


def _deep_merge(base: dict, overlay: dict) -> None:
    for key, value in overlay.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def run(config: dict, count: int, output_file: str) -> int:
    """运行注册流水线，返回成功数量。"""
    proxy = str(config.get("proxy", {}).get("url", "")).strip()
    flaresolverr = str(config.get("proxy", {}).get("flaresolverr_url", "")).strip()
    mail_config = config.get("mail", {})
    threads = int(config.get("registration", {}).get("threads", 2))

    logger.info("=" * 60)
    logger.info(f"注册机启动: {count} 个账号, {threads} 线程")
    logger.info(f"代理: {proxy or '(无)'}")
    logger.info(f"输出: {output_file}")
    logger.info("=" * 60)

    access_tokens: list[str] = []
    succeeded = 0
    failed = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max(1, threads)) as executor:
        futures = {
            executor.submit(
                register_worker,
                index=i,
                proxy=proxy,
                flaresolverr_url=flaresolverr,
                mail_config=mail_config,
            ): i
            for i in range(1, count + 1)
        }

        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
            except Exception as e:
                failed += 1
                logger.warning(f"[{idx}/{count}] 异常: {e}")
                continue

            if result.get("ok"):
                succeeded += 1
                at = result.get("result", {}).get("access_token", "")
                if at:
                    access_tokens.append(at)
                    # 实时写入，防止中途丢失
                    with open(output_file, "a", encoding="utf-8") as f:
                        f.write(at + "\n")
                email = result.get("result", {}).get("email", "?")
                cost = result.get("cost_seconds", 0)
                logger.info(f"[{idx}/{count}] ✓ {email} ({cost:.1f}s)")
            else:
                failed += 1
                error = result.get("error", "?")
                logger.warning(f"[{idx}/{count}] ✗ {error}")

    elapsed = time.time() - start_time
    logger.info("=" * 60)
    logger.info(f"完成: 成功 {succeeded}, 失败 {failed}, 耗时 {elapsed:.0f}s")
    logger.info(f"Access Token 已写入: {output_file}")
    logger.info("=" * 60)

    return succeeded


def main():
    parser = argparse.ArgumentParser(description="ChatGPT 生图注册机")
    parser.add_argument("-c", "--config", default="config.yaml", help="配置文件路径 (默认 config.yaml)")
    parser.add_argument("-n", "--count", type=int, default=None, help="注册数量 (覆盖配置文件)")
    parser.add_argument("-o", "--output", default=None, help="输出文件 (默认 access_tokens.txt)")
    parser.add_argument("-v", "--verbose", action="store_true", help="详细日志")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)-5s] %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_config(args.config)
    count = args.count or int(config.get("registration", {}).get("total", 10))
    output_file = args.output or str(config.get("output_file", "access_tokens.txt"))

    succeeded = run(config, count, output_file)
    sys.exit(0 if succeeded > 0 else 1)


if __name__ == "__main__":
    main()
