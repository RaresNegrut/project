# Use a lean Ubuntu base image
FROM ubuntu:22.04

# Suppress interactive prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3 and pip 3
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the requirements file and install dependencies *using python3 -m pip*
# This guarantees that the packages install into the Python 3 environment.
COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

# Copy the main application file
COPY simulator/ ./simulator/
COPY config.json .
COPY energy_data.csv .
COPY energy_simulator.py .

# Set the command to run the application
ENTRYPOINT ["python3", "energy_simulator.py"]