#!/usr/bin/env bash
#
# QuotaDB Installation Script
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/iluxav/quota-db/main/scripts/install.sh | bash
#   curl -sSL https://raw.githubusercontent.com/iluxav/quota-db/main/scripts/install.sh | bash -s -- --version v0.1.0 --prefix /opt/quota-db
#
# Options:
#   --version VERSION    Install specific version (default: latest)
#   --prefix PATH        Installation prefix (default: /usr/local)
#   --help               Show this help message
#

set -euo pipefail

# Configuration
GITHUB_REPO="${GITHUB_REPO:-iluxav/quota-db}"
BINARY_NAME="quota-db"
INSTALL_PREFIX="/usr/local"
VERSION=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

success() {
    echo -e "${GREEN}[OK]${NC} $*"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

die() {
    error "$*"
    exit 1
}

# Show help
show_help() {
    cat << EOF
QuotaDB Installation Script

Usage:
    install.sh [OPTIONS]

Options:
    --version VERSION    Install specific version (default: latest)
    --prefix PATH        Installation prefix (default: /usr/local)
    --help               Show this help message

Examples:
    # Install latest version
    curl -sSL https://raw.githubusercontent.com/iluxav/quota-db/main/scripts/install.sh | bash

    # Install specific version
    curl -sSL https://raw.githubusercontent.com/iluxav/quota-db/main/scripts/install.sh | bash -s -- --version v0.1.0

    # Install to custom location
    curl -sSL https://raw.githubusercontent.com/iluxav/quota-db/main/scripts/install.sh | bash -s -- --prefix ~/.local
EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --version)
                VERSION="$2"
                shift 2
                ;;
            --prefix)
                INSTALL_PREFIX="$2"
                shift 2
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                die "Unknown option: $1"
                ;;
        esac
    done
}

# Detect OS
detect_os() {
    local os
    os="$(uname -s)"
    case "$os" in
        Linux*)
            echo "linux"
            ;;
        Darwin*)
            echo "darwin"
            ;;
        MINGW*|MSYS*|CYGWIN*)
            die "Windows is not supported. Please use WSL2 or Docker."
            ;;
        *)
            die "Unsupported operating system: $os"
            ;;
    esac
}

# Detect architecture
detect_arch() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        x86_64|amd64)
            echo "x86_64"
            ;;
        aarch64|arm64)
            echo "aarch64"
            ;;
        armv7l)
            die "32-bit ARM is not supported"
            ;;
        *)
            die "Unsupported architecture: $arch"
            ;;
    esac
}

# Check for required commands
check_dependencies() {
    local deps=(curl tar)
    for dep in "${deps[@]}"; do
        if ! command -v "$dep" &> /dev/null; then
            die "Required command not found: $dep"
        fi
    done

    # Check for sha256sum or shasum
    if ! command -v sha256sum &> /dev/null && ! command -v shasum &> /dev/null; then
        warn "Neither sha256sum nor shasum found. Skipping checksum verification."
        return 1
    fi
    return 0
}

# Get the latest release version from GitHub
get_latest_version() {
    local url="https://api.github.com/repos/${GITHUB_REPO}/releases/latest"
    local version

    version=$(curl -sSL "$url" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    if [[ -z "$version" ]]; then
        die "Failed to fetch latest version from GitHub"
    fi

    echo "$version"
}

# Download and verify a file
download_file() {
    local url="$1"
    local output="$2"

    info "Downloading: $url"
    if ! curl -sSL -o "$output" "$url"; then
        die "Failed to download: $url"
    fi
}

# Verify SHA256 checksum
verify_checksum() {
    local file="$1"
    local checksum_file="$2"

    info "Verifying checksum..."

    if command -v sha256sum &> /dev/null; then
        if sha256sum -c "$checksum_file" &> /dev/null; then
            success "Checksum verified"
            return 0
        fi
    elif command -v shasum &> /dev/null; then
        if shasum -a 256 -c "$checksum_file" &> /dev/null; then
            success "Checksum verified"
            return 0
        fi
    else
        warn "Skipping checksum verification (no sha256sum/shasum)"
        return 0
    fi

    die "Checksum verification failed"
}

# Install the binary
install_binary() {
    local src="$1"
    local dest="$2"
    local bin_dir="${dest}/bin"

    # Create bin directory if needed
    if [[ ! -d "$bin_dir" ]]; then
        info "Creating directory: $bin_dir"
        if ! mkdir -p "$bin_dir" 2>/dev/null; then
            # Try with sudo
            sudo mkdir -p "$bin_dir"
        fi
    fi

    # Install the binary
    info "Installing ${BINARY_NAME} to ${bin_dir}/"
    if ! mv "$src" "${bin_dir}/${BINARY_NAME}" 2>/dev/null; then
        # Try with sudo
        sudo mv "$src" "${bin_dir}/${BINARY_NAME}"
    fi

    # Make executable
    if ! chmod +x "${bin_dir}/${BINARY_NAME}" 2>/dev/null; then
        sudo chmod +x "${bin_dir}/${BINARY_NAME}"
    fi

    success "Installed ${BINARY_NAME} to ${bin_dir}/${BINARY_NAME}"
}

# Check if the binary is in PATH
check_path() {
    local bin_dir="${INSTALL_PREFIX}/bin"

    if [[ ":$PATH:" != *":$bin_dir:"* ]]; then
        warn "${bin_dir} is not in your PATH"
        echo ""
        echo "Add it to your PATH by running:"
        echo ""
        echo "  export PATH=\"${bin_dir}:\$PATH\""
        echo ""
        echo "Or add this line to your ~/.bashrc, ~/.zshrc, or equivalent:"
        echo ""
        echo "  export PATH=\"${bin_dir}:\$PATH\""
        echo ""
    fi
}

# Main installation function
main() {
    parse_args "$@"

    echo ""
    echo "QuotaDB Installer"
    echo "================="
    echo ""

    # Detect platform
    local os arch
    os=$(detect_os)
    arch=$(detect_arch)
    info "Detected platform: ${os}-${arch}"

    # Check dependencies
    local verify_checksum=true
    if ! check_dependencies; then
        verify_checksum=false
    fi

    # Get version to install
    if [[ -z "$VERSION" ]]; then
        info "Fetching latest version..."
        VERSION=$(get_latest_version)
    fi
    info "Installing version: $VERSION"

    # Build asset name
    local asset_name="${BINARY_NAME}-${os}-${arch}"
    local download_url="https://github.com/${GITHUB_REPO}/releases/download/${VERSION}/${asset_name}.tar.gz"
    local checksum_url="${download_url}.sha256"

    # Create temp directory
    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap "rm -rf $tmp_dir" EXIT

    # Download files
    download_file "$download_url" "${tmp_dir}/${asset_name}.tar.gz"

    if [[ "$verify_checksum" == "true" ]]; then
        download_file "$checksum_url" "${tmp_dir}/${asset_name}.tar.gz.sha256"

        # Change to temp dir for checksum verification (paths in checksum file are relative)
        cd "$tmp_dir"
        verify_checksum "${asset_name}.tar.gz" "${asset_name}.tar.gz.sha256"
    fi

    # Extract
    info "Extracting archive..."
    tar -xzf "${tmp_dir}/${asset_name}.tar.gz" -C "$tmp_dir"

    # Install
    install_binary "${tmp_dir}/${BINARY_NAME}" "$INSTALL_PREFIX"

    # Verify installation
    if command -v "${BINARY_NAME}" &> /dev/null; then
        echo ""
        success "Installation complete!"
        echo ""
        "${BINARY_NAME}" --version 2>/dev/null || true
    else
        check_path
    fi

    echo ""
    info "Quick start:"
    echo "  ${BINARY_NAME} --help"
    echo "  ${BINARY_NAME} --bind 127.0.0.1:6380"
    echo ""
}

main "$@"
