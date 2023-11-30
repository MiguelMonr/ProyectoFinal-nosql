FROM python:3.11

WORKDIR /app

# Install any dependencies your Python scripts might need
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python scripts into the container
COPY . .

# Command to run your Python scripts
CMD ["sh", "-c", "sleep 15 && python3 conexionmongo.py"]