# AWS CloudShell Deployment Guide

## One-Command Deployment

Copy and paste this entire command into AWS CloudShell:

```bash
# Clone and setup in one command
git clone https://github.com/YOUR_USERNAME/telegram-diamond-bot.git && \
cd telegram-diamond-bot && \
chmod +x setup.sh && \
./setup.sh
