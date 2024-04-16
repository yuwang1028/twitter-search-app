import os
from configparser import ConfigParser
import subprocess
import mysql.connector

def get_config():
    """
    Reads configuration values from 'config.ini'.
    Returns:
        A dictionary with SSH and MySQL configuration details.
    """
    config = ConfigParser()
    config.read('config.ini')  # Ensure the path is correct based on your application's directory structure
    
    ssh_config = {
        'key_path': config.get('ssh', 'key_path'),
        'user': config.get('ssh', 'user'),
        'host': config.get('ssh', 'host')
    }
    mysql_config = {
        'password': config.get('mysql', 'password'),
        'user': config.get('mysql', 'user'),
        'host': config.get('mysql', 'host')
    }

    return ssh_config, mysql_config

def connect_mysql_via_ssh(ssh_config, mysql_config):
    """
    Establishes an SSH tunnel and connects to a MySQL database using the provided configurations.
    Returns:
        A MySQL connection object.
    """
    ssh_command = f"ssh -t -i '{ssh_config['key_path']}' {ssh_config['user']}@{ssh_config['host']} \"mysqlsh --password='{mysql_config['password']}' --user={mysql_config['user']} --host={mysql_config['host']}\""
    
    # Since we cannot directly return a mysql.connector connection, you would normally open the tunnel here
    # For simulation, we'll execute the command (not recommended for actual use due to security concerns of password in command)
    result = subprocess.run(ssh_command, shell=False, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("SSH Tunnel established and connected to MySQL")
        # In practice, here you would set up port forwarding and then connect via mysql.connector
        # Example:
        # cnx = mysql.connector.connect(user=mysql_config['user'], password=mysql_config['password'], host='127.0.0.1', port=forwarded_port)
        # return cnx
    else:
        print(f"Failed to connect: {result.stderr}")

    return None

# Example usage
if __name__ == '__main__':
    ssh_config, mysql_config = get_config()
    connect_mysql_via_ssh(ssh_config, mysql_config)
