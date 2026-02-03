# 💎 Diamond Trading Bot

A Telegram bot for diamond trading with admin, supplier, and client roles.

## Features
- Role-based access (Admin/Supplier/Client)
- Diamond stock management
- Deal negotiation system
- Excel file upload/download
- Real-time notifications
- Activity logging
- AWS S3 integration

## Quick Deploy on AWS EC2

### 1. Launch EC2 Instance
- OS: Ubuntu 22.04 LTS
- Type: t2.micro or larger
- Storage: 20GB
- Security Group: Open port 10000

### 2. Connect & Setup
```bash
ssh -i your-key.pem ubuntu@ec2-ip
