#!/usr/bin/env bash
set -euo pipefail

REPO="ALHDLIOX/wx-cli"
BIN_NAME="wx"
INSTALL_DIR="${WX_INSTALL_DIR:-$HOME/.local/bin}"

is_source_repo() {
  [ -f Cargo.toml ] && awk '
    /^[[:space:]]*\[/ { in_package = ($0 ~ /^[[:space:]]*\[package\][[:space:]]*(#.*)?$/) }
    in_package && /^[[:space:]]*name[[:space:]]*=[[:space:]]*"wx-cli"[[:space:]]*(#.*)?$/ { found = 1 }
    END { exit !found }
  ' Cargo.toml
}

CARGO=""
if is_source_repo; then
  if command -v cargo >/dev/null 2>&1; then
    CARGO="$(command -v cargo)"
  elif [ -x "$HOME/.cargo/bin/cargo" ]; then
    CARGO="$HOME/.cargo/bin/cargo"
  fi
fi

if [ -n "$CARGO" ]; then
  # ── 源码构建 ──────────────────────────────────────────────
  echo "检测到 wx-cli 源码，正在构建..."
  "$CARGO" build --release
  mkdir -p "$INSTALL_DIR"
  cp target/release/wx "${INSTALL_DIR}/${BIN_NAME}"
else
  # ── 检测平台 ────────────────────────────────────────────────
  OS=$(uname -s)
  ARCH=$(uname -m)

  case "${OS}-${ARCH}" in
    Darwin-arm64)   ASSET="wx-macos-arm64" ;;
    Darwin-x86_64)  ASSET="wx-macos-x86_64" ;;
    Linux-x86_64)   ASSET="wx-linux-x86_64" ;;
    Linux-aarch64)  ASSET="wx-linux-arm64" ;;
    *)
      echo "不支持的平台: ${OS}-${ARCH}"
      echo "请从 https://github.com/${REPO}/releases 手动下载"
      exit 1
      ;;
  esac

  # ── 获取最新版本号 ──────────────────────────────────────────
  echo "正在获取最新版本..."
  if ! TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/') || [ -z "$TAG" ]; then
    echo "获取版本失败，请检查网络或访问 https://github.com/${REPO}/releases"
    echo "也可以改为源码构建（需先安装 Rust / cargo）："
    echo "  git clone https://github.com/${REPO}.git && cd wx-cli && ./install.sh"
    exit 1
  fi

  echo "版本: ${TAG}  平台: ${ASSET}"

  # ── 下载 ────────────────────────────────────────────────────
  URL="https://github.com/${REPO}/releases/download/${TAG}/${ASSET}"
  TMP=$(mktemp)
  trap 'rm -f "$TMP"' EXIT

  echo "下载中: ${URL}"
  curl -fsSL --progress-bar -o "$TMP" "$URL"
  chmod +x "$TMP"

  # ── 安装 ────────────────────────────────────────────────────
  mkdir -p "$INSTALL_DIR"
  mv "$TMP" "${INSTALL_DIR}/${BIN_NAME}"
fi

echo ""
echo "✓ wx 已安装到 ${INSTALL_DIR}/${BIN_NAME}"
case ":${PATH:-}:" in
  *":${INSTALL_DIR}:"*) ;;
  *) echo "提示：安装目录不在 PATH 中，请运行 export PATH=\"${INSTALL_DIR}:\$PATH\"（可加入 shell 配置文件）" ;;
esac
echo ""
echo "快速开始："
echo "  sudo wx init                              # 首次初始化（微信须登录运行）"
echo "  sudo wx key extract --hook-seconds 90     # 缺分片密钥时补齐"
echo "  wx doctor                                 # 环境 / 密钥健康检查"
echo "  wx sessions                               # 查看最近会话"
echo "  wx --help"
