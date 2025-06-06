FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    chromium \
    chromium-driver \
    libxi6 \
    libgconf-2-4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libdbus-1-3 \
    libgtk-3-0 \
    libgbm-dev \
    libx11-xcb-dev \
    libxcb-dri3-0 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN python -m playwright install chromium


# Copy application code
COPY src/ .


# Create necessary directories
RUN mkdir -p /app/data

# Set permissions
RUN chmod +x *.py

# Command to run the application
CMD ["python", "Main.py"]