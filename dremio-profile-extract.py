# -*- coding: utf-8 -*-
import os
import subprocess
import re
import time
import simplejson as json
import zipfile

REPROCESS = True

PROFILES_DIR = "/tmp/dremio/profiles/"
PROFILES_FOLDER_NAME = "/tmp/dremio/profiles/1ac9c312-0e50-4e12-9ff9-2a9ee4d8d04b/"
DREMIO_BIN_DIR = "/opt/dremio/bin/"
USERNAME = "kevin"
PASSWORD = "password123"
OUTPUT_FORMAT = "ZIP"  # ZIP or JSON
START_TIME = "2020-01-01T00:00:00"
END_TIME = "2020-06-01T00:00:00"
OUTPUT_TYPE = 'w'


BASE_DIR = os.environ.get('PROFILE_STORAGE_DIR', PROFILES_DIR)
OUTPUT_DIR = BASE_DIR


def update_output(filename):
    return os.path.join(BASE_DIR, filename) if filename else None


def get_files():
    only_files = [f for f in os.listdir(BASE_DIR) if os.path.isfile(os.path.join(BASE_DIR, f))]
    return only_files

def file_disk(filename):
    while not os.path.exists(os.path.join(BASE_DIR, filename)):
        time.sleep(1)

    with open(os.path.join(BASE_DIR, filename)) as f:
        return json.load(f)

def export_profiles():
    ret_val = subprocess.run(DREMIO_BIN_DIR + "dremio-admin export-profiles --from '" + START_TIME + "' --to '" + END_TIME + "' -u " + USERNAME + " -p " + PASSWORD + $
    ret_string = ret_val.stdout.decode("utf-8")
    ret_str_array = ret_string.splitlines()
#    print(ret_str_array[0])
    num_profiles = re.search('processed: (.*?),', ret_str_array[0]).group(1)
#    print(num_profiles)
    profile_dir = re.search('path: (.*)$', ret_str_array[1]).group(1)
#    print(profile_dir)
    return(profile_dir)


BASE_DIR = export_profiles() if REPROCESS else PROFILES_FOLDER_NAME
output_filename = BASE_DIR.rstrip('/')+".csv"
file_list = get_files()
# print(file_list)
# print(file_list[-1])
zip_file = BASE_DIR+file_list[-1]
if zip_file.endswith('.zip'):
    with zipfile.ZipFile(BASE_DIR + file_list[-1], 'r') as zip_ref:
        zip_ref.extractall(BASE_DIR)
    os.remove(zip_file)
output_file = open(output_filename, OUTPUT_TYPE)
for f_profile in file_list:
    data = file_disk(f_profile)
    job_id = re.search('profile_(.+?).json', f_profile).group(1)
    user_name = data["user"]
    start_time = str(data["start"])
    phase_row = data["planPhases"]
    for x in phase_row:
        if x["phaseName"] == 'Logical Planning':
            xs = x["plan"].splitlines()
            data_source_num = 1
            for xsr in xs:
                if "table=[" in xsr.strip():
                    query_def = xsr.strip()
                    table_name_list = re.search('table=(.+?)],', query_def).group(1) + ']'
                    column_name_list = re.search('columns=(.+?)],', query_def).group(1) + ']'
#                    print(job_id+'|'+start_time+'|'+user_name+'|'+str(data_source_num)+'|'+table_name_list+'|'+column_name_list)
                    output_file.write(job_id+'|'+start_time+'|'+user_name+'|'+str(data_source_num)+'|'+table_name_list+'|'+column_name_list+'\n')
                    data_source_num=data_source_num+1
output_file.close()
