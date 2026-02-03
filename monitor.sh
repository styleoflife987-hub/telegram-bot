#!/bin/bash
# monitor.sh - Monitor and auto-restart bot

echo "🔍 Starting bot monitor..."
echo "📝 Monitor logs: logs/monitor.log"

while true; do
    if ! pgrep -f "python.*diamond_bot.py" > /dev/null; then
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        echo "[$TIMESTAMP] ❌ Bot not running. Restarting..." >> logs/monitor.log
        echo "[$TIMESTAMP] ❌ Bot not running. Restarting..."
        
        # Restart bot
        ./start.sh >> logs/monitor.log 2>&1
        
        # Wait for restart
        sleep 10
    else
        TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
        echo "[$TIMESTAMP] ✅ Bot is running (PID: $(pgrep -f 'python.*diamond_bot.py'))" >> logs/monitor.log
    fi
    
    # Check every 30 seconds
    sleep 30
done
