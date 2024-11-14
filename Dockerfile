# Use the official Python image
FROM python:3.12-slim

# Set the working directory
WORKDIR /usr/src/app

# Copy the requirements file
COPY requirements.txt ./

# Install any required dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Expose the port the app runs on
EXPOSE 5000

# Define environment variable
ENV FLASK_ENV=development

# Run the command to start the app
CMD ["uvicorn", "--workers", "1", "--host", "0.0.0.0", "--port", "5000", "app:app"]
