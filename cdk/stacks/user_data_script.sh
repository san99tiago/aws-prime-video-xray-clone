#!/bin/bash
# Enable extra logging
set -x

# Install amazon linux extras
sudo yum install -y amazon-linux-extras

# Refresh environment variables
source /etc/profile

# Update OS
echo "----- Updating OS -----"
sudo yum update -y

# Install and Initialize SSM Agent
echo "----- Initializing SSM Agent -----"
sudo yum install -y https://s3.us-east-1.amazonaws.com/amazon-ssm-us-east-1/latest/linux_amd64/amazon-ssm-agent.rpm
sudo systemctl enable amazon-ssm-agent
sudo systemctl start amazon-ssm-agent

# Install Instance Connect
echo "----- Initializing EC2 Instance Connect Agent -----"
sudo yum install -y ec2-instance-connect

# Create the necessary folders and permissions
mkdir /home/app
mkdir /home/app/logs
sudo chmod 775 /home/app

# Install Python related Dependencies
echo "----- Installing Python3 Dependencies -----"
sudo yum -y install python-pip
sudo amazon-linux-extras enable python3.8
sudo yum -y install python38

# Install git
echo "----- Installing Git -----"
yum install -y git

# Get our custom streamlit app for prime-video-xray source code
echo "----- Downloading source code from Git -----"
cd /home/app/
git clone https://github.com/san99tiago/aws-prime-video-xray-clone.git

# Show the downloaded files
echo "----- Source code files are -----"
ls -lrt /home/app/aws-prime-video-xray-clone/

# Install additional Python dependencies
echo "----- Installing Python dependencies -----"
python3.8 -m pip install -r /home/app/aws-prime-video-xray-clone/backend/chat_ui/requirements.txt
sleep 2

# Run the Streamlit Server (automatically becomes background process)
python3.8 -m streamlit run /home/app/aws-prime-video-xray-clone/backend/chat_ui/prime_video_xray_app.py --server.port 80
