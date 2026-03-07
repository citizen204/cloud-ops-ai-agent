#!/usr/bin/env python3
"""
verify_aws.py — AWS Academy Learner Lab credential health-check.

Loads credentials from the project .env file, calls AWS STS
GetCallerIdentity, and reports whether the temporary session token
is still valid or has expired.

Usage:
    python scripts/verify_aws.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# 1. Load .env file (lightweight, no third-party dependency needed)
# ---------------------------------------------------------------------------

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"


def _load_dotenv(path: Path) -> None:
    """Parse a simple KEY=VALUE .env file into os.environ."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if key and value:
            os.environ.setdefault(key.strip(), value.strip())


# ---------------------------------------------------------------------------
# Colour helpers (works on macOS / Linux terminals)
# ---------------------------------------------------------------------------

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _ok(msg: str) -> None:
    print(f"{GREEN}{BOLD}[OK]{RESET}  {msg}")


def _fail(msg: str) -> None:
    print(f"{RED}{BOLD}[FAIL]{RESET}  {msg}")


def _warn(msg: str) -> None:
    print(f"{YELLOW}{BOLD}[WARN]{RESET}  {msg}")


def _info(msg: str) -> None:
    print(f"{CYAN}[INFO]{RESET}  {msg}")


# ---------------------------------------------------------------------------
# 2. Pre-flight: check that required env vars are present
# ---------------------------------------------------------------------------

REQUIRED_VARS = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
]


def _preflight() -> bool:
    """Return True if all required env vars are set and non-empty."""
    missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
    if missing:
        _fail("以下环境变量缺失:")
        for v in missing:
            print(f"       - {v}")
        print()
        _warn(
            "请先从 AWS Academy Learner Lab 获取凭证, "
            "填入 .env 文件后重试。"
        )
        print(f"       .env 路径: {ENV_PATH}")
        return False
    return True


# ---------------------------------------------------------------------------
# 3. Call STS to verify the credentials
# ---------------------------------------------------------------------------

def _verify() -> bool:
    """Call sts:GetCallerIdentity and return True on success."""
    try:
        import boto3  # noqa: delay import so preflight runs even without boto3
        from botocore.exceptions import (
            BotoCoreError,
            ClientError,
            NoCredentialsError,
        )
    except ImportError:
        _fail("boto3 未安装。请先运行: pip install boto3")
        return False

    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    sts = boto3.client(
        "sts",
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    )

    try:
        identity = sts.get_caller_identity()
        _ok("AWS 凭证有效!")
        print()
        _info(f"Account : {identity['Account']}")
        _info(f"Arn     : {identity['Arn']}")
        _info(f"UserId  : {identity['UserId']}")
        _info(f"Region  : {region}")

        bucket = os.getenv("S3_LOG_BUCKET")
        if bucket:
            print()
            _info(f"S3 Bucket : {bucket}")
            _info(f"S3 Prefix : {os.getenv('S3_LOG_PREFIX', 'logs/')}")

        return True

    except (ClientError, NoCredentialsError, BotoCoreError) as exc:
        error_code = ""
        if hasattr(exc, "response"):
            error_code = exc.response.get("Error", {}).get("Code", "")

        print()
        if error_code in ("ExpiredToken", "ExpiredTokenException"):
            _fail("AWS Session Token 已过期!")
        elif error_code == "InvalidClientTokenId":
            _fail("AWS Access Key ID 无效!")
        elif error_code == "SignatureDoesNotMatch":
            _fail("AWS Secret Access Key 不匹配!")
        else:
            _fail(f"AWS 认证失败: {exc}")

        print()
        print(f"  {BOLD}请按以下步骤重新获取凭证:{RESET}")
        print()
        print("  1. 打开 AWS Academy → Learner Lab")
        print("  2. 点击 Start Lab (等待绿灯)")
        print("  3. 点击 AWS Details → Show (AWS CLI 区域)")
        print("  4. 复制三个值, 更新到 .env 文件:")
        print(f"     {CYAN}{ENV_PATH}{RESET}")
        print()
        print("  需要更新的字段:")
        print(f"     {YELLOW}AWS_ACCESS_KEY_ID{RESET}")
        print(f"     {YELLOW}AWS_SECRET_ACCESS_KEY{RESET}")
        print(f"     {YELLOW}AWS_SESSION_TOKEN{RESET}")
        print()
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print()
    print(f"  {BOLD}cloud-ops-ai-agent — AWS Credential Verifier{RESET}")
    print("  " + "─" * 46)
    print()

    _load_dotenv(ENV_PATH)

    if not _preflight():
        sys.exit(1)

    _info(f".env 已加载: {ENV_PATH}")
    _info("正在验证 AWS STS GetCallerIdentity ...")
    print()

    if not _verify():
        sys.exit(1)

    print()
    _ok("一切就绪, 可以正常使用 S3 日志上传功能。")
    print()


if __name__ == "__main__":
    main()
