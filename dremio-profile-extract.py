import sys
import getopt
import os
import subprocess
import re
import json
import zipfile
from datetime import datetime
import time
from pprint import pprint


def main():
    global profiles_dir, username, password, output_format, start_time, end_time, delim, dremio_bin_dir, reprocess, output_type, write_mode, accept_all_certs, connection_type, output_log
    profiles_dir = "/tmp/dremio/profiles/"  # -o or --profiles_dir
    connection_type = "" # -l or --local-attach
    username = ""  # -u or --username
    password = ""  # -p or --password
    output_format = "ZIP"  # -f or --output_format (ZIP or JSON)
    start_time = "1990-01-01T00:00:00"  # -s or --start_time (yyyy-mm-ddThh:mm:ss)
    end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")  # -e or --end_time (yyyy-mm-ddThh:mm:ss)
    delim = "\t"  # -d or --delim  ("\t", "|")
    dremio_bin_dir = "/opt/dremio/bin/"  # -b or --dremio_bin_dir
    reprocess = True  # -r or --no_reprocess (True, False)
    output_type = 'w'  # -w or -a --output_type ('w', 'a')  Write or append
    write_mode = 'OVERWRITE'
    accept_all_certs = False
    output_log = profiles_dir + "audit_history.log"
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hlo:u:p:f:s:e:d:b:rawi",
                                   ["help", "connection_type=", "profiles_dir=", "username=", "password=", "output_format=", "start_time=",
                                    "end_time=", "delim=", "dremio_bin_dir=", "reprocess", "output_type=",
                                    "write_mode=", "incremental"])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    if len(opts) == 0:
        opts = [('-l', ''), ('-i', '')]
    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print_usage()
            sys.exit()
        elif opt in ("-i", "--incremental"):
            start_time = set_times_from_log(output_log)
            print (start_time, end_time)
        elif opt in ("-l", "--local-attach"):
            connection_type = '-l'
        elif opt in ("-o", "--profiles_dir"):
            profiles_dir = arg
        elif opt in ("-u", "--username"):
            username = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-f", "--output_format"):
            if arg in ("ZIP", "JSON"):
                output_format = arg
            else:
                print ('--output_format must be <ZIP, JSON>')
                exit(2)
        elif opt in ("-s", "--start_time"):
            start_time = arg
        elif opt in ("-e", "--end_time"):
            end_time = arg
        elif opt in ("-d", "--delim"):
            delim = arg
        elif opt in ("-b", "--dremio_bin_dir"):
            dremio_bin_dir = arg
        elif opt in ("-r", "--no_reprocess"):
            reprocess = False
        elif opt in "-w":
            output_type = "w"
        elif opt in "-a":
            output_type = "a"
        elif opt in "--write_mode":
            if arg in ("FAIL_IF_EXISTS", "OVERWRITE", "EXISTS"):
                write_mode = arg
            else:
                print ('--write_mode must be <FAIL_IF_EXISTS, OVERWRITE, SKIP>')
                exit(2)

    output_dir = export_profiles() if reprocess else profiles_dir
    output_filename = profiles_dir + "audit_log_" + end_time + ".csv"

    file_list = get_files(output_dir, ".zip")
    # print(file_list)
    zi = 0
    for z_files in file_list:
        zip_file = output_dir + file_list[zi]
        # zip_file_crc = output_dir + file_list[zi + 1]
        if zip_file.endswith('.zip'):
            with zipfile.ZipFile(output_dir + file_list[zi], 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            os.remove(zip_file)
            # os.remove(zip_file_crc)
            zi = zi + 1
    file_list = get_files(output_dir, ".JSON")
    num_profiles = len(file_list)
    output_file = open(output_filename, output_type)
    output_file.write("job_id" + delim + "start_time" + delim + "end_time" + delim + "job_state" + delim + "error" + delim + "user_name" + delim + "query_type" + delim + "source_num" + delim + "parent_name_list" + delim + "table_name_list" + delim + "column_name_list" + '\n')
    l = 0
    for f_profile in file_list:
        l = l + 1
        output_str = ""
        data = file_disk(output_dir, f_profile)
        job_id = re.search('profile_(.+?).JSON', f_profile).group(1)
        j_user_name = data["user"]
        # j_start_time = str(data["start"])
        j_start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data["start"] / 1000.0))
        # j_end_time = str(data["end"])
        # j_end_time = datetime.datetime.fromtimestamp(data["end"]/1000).strftime('%Y-%m-%d %H:%M:%S.%f')
        j_end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(data["end"] / 1000.0))
        j_end_time = str(data["end"])
        job_state_num = data["state"]
        if job_state_num == 2:
            job_state = 'Completed'
        elif job_state_num == 3:
            job_state = 'Cancelled'
        elif job_state_num == 4:
            job_state = 'Error'
        else:
            job_state = 'Unknown'
        try:
            verbose_error = "[]"
#            verbose_error = str(data["verboseError"])
        except:
            verbose_error = "[]"
        try:
            rsp = str(data["resourceSchedulingProfile"])
        except:
            rsp = "[]"
        if "queryType" in rsp:
            query_type = re.search(r"'queryType': '(.+?)'}", rsp).group(1)
        else:
            query_type = '[Unkown]'
        phase_row = data["planPhases"]
        for x in phase_row:
            if x["phaseName"] == 'Convert To Rel':
                xs = x["plan"].splitlines()
                parent_fl = 1
                data_source_num = 1
                parent_name_list = "["
                for xsr in xs:
                    if "ExpansionNode(path=" in xsr.strip() and parent_fl == 1: # this is first level parent (VDS or PDS)
                        query_def = xsr.strip()
                        parent_name_list = re.search(r'path=(.+?)]', query_def).group(1)  # return parent vds name
                        parent_fl = 0
                    if "table=[" in xsr.strip():
                        parent_name_list = parent_name_list + "]"
                        query_def = xsr.strip()
                        table_name_list = re.search('table=(.+?)],', query_def).group(1) + ']'
                        column_name_list = re.search('columns=(.+?)],', query_def).group(1) + ']'
                        output_str = output_str + job_id + delim + j_start_time + delim + j_end_time + delim + job_state + delim + verbose_error + delim + j_user_name + delim + str(query_type) + delim + str(data_source_num) + delim + str(parent_name_list) + delim + str(table_name_list) + delim + str(column_name_list) + '\n'
                        data_source_num = data_source_num + 1
                        parent_fl = 1
                        parent_name_list = ""
        output_file.write(output_str)

    output_log_file = open(output_log, "a")
    output_log_file.write(str(num_profiles) + delim + start_time + delim + end_time + '\n')
    output_log_file.close()

    output_file.close()
    os.chmod(output_filename, 0o777)
    return output_filename


def print_usage():
    print ('\n'
           'dremio-profile-extract.py <options below>\n'
           '   -h, --help\n'
           '   -c, --accept_all_certs\n'
           '       accept all ssl certificates\n'
           '       Default: False\n'
           '   -o, --output_dir <profile output dir> (default: /tmp/dremio/profiles/)\n'
           '   -l, --local-attach (Cannot be used with Username and Password)\n'
           '   -i, --incremental (Cannot be used with start_time, end_time)\n'
           '   -b, --dremio_bin_dir <Dremio bin directory> (default: /opt/dremio/bin)\n'
           '   -u, --username <admin username> (required unless using -l)\n'
           '   -p, --password <admin password> (required unless using -l)\n'
           '   -f, --output_format <ZIP or JSON> (default: ZIP)\n'
           '   -s, --start_time <yyyy-mm-ddThh:mm:ss>\n'
           '       Export profiles beginning from this date inclusively (job_end_time >=\n'
           '       toDate).Example: 2011-12-03T10:15:30\n'
           '   -e, --end_time <yyyy-mm-ddThh:mm:ss>\n'
           '       Export profiles ending by this date exclusively (job_end_time <\n'
           '       toDate).Example: 2011-12-03T10:15:30\n'
           '   -d, --delim  <\\t (tab) , | (pipe), or any character> (default: \\t)\n'
           '   -r  suppresses reprocessing of profile extract (same as --no_reprocess True)  (default: False)\n'
           '   --no_reprocess <True, False> (default: False)\n'
           '   -a  Appends to profile extract file\n'
           '   -w  Overwrites profile extract file (default)\n'
           '   --output_type <w, a> (default: w)\n'
           '   --write-mode\n'
           '     Specifies how we should handle a case, when target file already exists.\n'
           '     Default: OVERWRITE Possible Values: [FAIL_IF_EXISTS, OVERWRITE, SKIP]\n')

def set_times_from_log(log_file):
    if os.path.exists(log_file):
        with open(log_file, 'r') as lf:
            lines = lf.read().splitlines()
            last_line = lines[-1]
            last_end_time = last_line[-19:]
        lf.close()
    else:
        output_log_file = open(log_file, "a")
        output_log_file.write('num_profiles' + delim + 'start_time' + delim + 'end_time' + '\n')
        last_end_time = start_time
        output_log_file.close()
    return (last_end_time)

def update_output(output_dir, filename):
    return os.path.join(output_dir, filename) if filename else None

def get_files(output_dir, f_type):
    only_files = [f for f in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, f)) and f.endswith(f_type)]
    return only_files

def get_dir(output_dir):
    only_dir = [d for d in os.listdir(output_dir) if os.path.isdir(os.path.join(output_dir, d))]
    return only_dir


def file_disk(output_dir, filename):
    # print(output_dir, filename)
    with open(os.path.join(output_dir, filename)) as f:
        return json.load(f)


def export_profiles():
    if connection_type == '-l':
        ret_val = subprocess.Popen([dremio_bin_dir + "dremio-admin", "export-profiles", "--from ", start_time, "--to ", end_time, "-l ", " --output", profiles_dir], stdout=subprocess.PIPE)
        ret_val.wait()
    else:
        ret_val = subprocess.Popen([dremio_bin_dir + "dremio-admin", "export-profiles", "--from ", start_time, "--to ", end_time, "-u ", username, "-p ", password, " --output", profiles_dir], stdout=subprocess.PIPE)
        ret_val.wait()
        ret_string = ret_val.communicate()[0]
    return profiles_dir + get_dir(profiles_dir)[0]+ '/'


if __name__ == "__main__":
    main()
