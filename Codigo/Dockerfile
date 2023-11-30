FROM python:3.11.4

WORKDIR /app

# Install any dependencies your Python scripts might need
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python scripts into the container
COPY . .

# Command to run your Python scripts
CMD sleep 45 && python cocktails_data.py && \
    python importIngredientsNeo4J.py && \
    python importDrinksNeo4J.py && \
    python importRealationsNeo4J.py && \
    python importCassandra.py