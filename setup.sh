cat > setup.sh << 'EOF'
#!/bin/bash
echo "🚀 Setting up Diamond Bot..."

# Update system
apt update
apt upgrade -y

# Install dependencies
apt install python3 python3-pip python3-venv git sqlite3 -y

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python packages
pip install -r requirements.txt

# Create directories
mkdir -p data backups logs temp

echo "✅ Setup complete!"
echo "👉 1. Edit .env file: nano .env"
echo "👉 2. Add your BOT_TOKEN"
echo "👉 3. Run: python3 diamond_bot.py"
EOF
