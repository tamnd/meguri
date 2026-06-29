---
title: "Installation"
description: "Install meguri from Go, Homebrew, Scoop, a release archive, a Linux package, or the container image."
weight: 20
---

meguri is a single static binary with no cgo and no runtime dependencies. Pick whichever channel suits you.

## Go

```bash
go install github.com/tamnd/meguri/cmd/meguri@latest
```

## Homebrew (macOS and Linux)

```bash
brew install tamnd/tap/meguri
```

## Scoop (Windows)

```bash
scoop bucket add tamnd https://github.com/tamnd/scoop-bucket
scoop install meguri
```

## Linux (apt and dnf)

A signed apt and dnf repository tracks every release.

```bash
# Debian, Ubuntu
curl -fsSL https://tamnd.github.io/linux-repo/gpg.key \
  | sudo gpg --dearmor -o /usr/share/keyrings/tamnd.gpg
echo "deb [signed-by=/usr/share/keyrings/tamnd.gpg] https://tamnd.github.io/linux-repo/apt stable main" \
  | sudo tee /etc/apt/sources.list.d/tamnd.list
sudo apt update && sudo apt install meguri

# Fedora, RHEL
sudo tee /etc/yum.repos.d/tamnd.repo <<'EOF'
[tamnd]
name=tamnd
baseurl=https://tamnd.github.io/linux-repo/rpm
enabled=1
gpgcheck=1
gpgkey=https://tamnd.github.io/linux-repo/gpg.key
EOF
sudo dnf install meguri
```

## Release archive

Every release attaches archives for Linux, macOS, Windows, and FreeBSD on amd64 and arm64, with a `checksums.txt` and a cosign signature. Download, verify, and drop the binary on your `PATH`.

## Container image

```bash
docker run --rm -v "$PWD:/data" ghcr.io/tamnd/meguri inspect /data/partition.meguri
```

## Build from source

```bash
git clone https://github.com/tamnd/meguri
cd meguri
make build      # bin/meguri, CGO disabled
```
