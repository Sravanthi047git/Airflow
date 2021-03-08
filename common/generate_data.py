#generate_data.py
import os
from code.common.s3_utils import copy_to_s3
from code.common.system_utils import execute_local, del_local_file


def generate_data():
    path = "/Users/BigData_ETL_Project/Fake-Apache-Log-Generator/"
    os.chdir(path)

    #cmd = ["./venv/bin/python", "generate_fake_profiles.py"]
    cmd = ["/usr/local/bin/python3.8", "generate_fake_profiles.py"]
    execute_local(cmd)

    # List files in current directory
    files = os.listdir(os.curdir)
    # particular files from a directories
    list = [path + k for k in files if 'access_log' in k]
    print(list)
    return list


