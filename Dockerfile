# Use the official Python image as a base
FROM python:3.9-slim

# Set environment variables for Pip
ENV PIP_NO_CACHE_DIR off
ENV PIP_DISABLE_PIP_VERSION_CHECK on
ENV PYTHONPATH /usr/src/app

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy requirements file first to leverage Docker cache
COPY requirements.txt ./

# Install dependencies using pip
RUN pip install -r requirements.txt

# Copy the rest of the project files into the container
COPY ./ ./

# Default command for your project
CMD ["python", "python_utils/run.py"]