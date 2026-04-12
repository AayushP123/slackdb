FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY slackdb_bot.py .

# Expose port
EXPOSE 3000

# Run the bot
CMD ["python", "slackdb_bot.py"]
