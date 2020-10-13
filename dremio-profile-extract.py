import sys
import getopt
import os
import subprocess
import re
import json
import zipfile


def main():
    global profiles_dir, username, password, output_format, start_time, end_time, delim, dremio_bin_dir, reprocess, output_type, write_mode, accept_all_certs, connection_type
    profiles_dir = "/tmp/dremio/profiles/"  # -o or --profiles_dir
    connection_type = "" # -l or --local-attach
    username = ""  # -u or --username
    password = ""  # -p or --password
    output_format = "ZIP"  # -f or --output_format (ZIP or JSON)
    start_time = "1990-01-01T00:00:00"  # -s or --start_time (yyyy-mm-ddThh:mm:ss)
    end_time = "2099-12-31T23:59:59"  # -e or --end_time (yyyy-mm-ddThh:mm:ss)
    delim = "\t"  # -d or --delim  ("\t", "|")
    dremio_bin_dir = "/opt/dremio/bin/"  # -b or --dremio_bin_dir
    reprocess = True  # -r or --no_reprocess (True, False)
    output_type = 'w'  # -w or -a --output_type ('w', 'a')  Write or append
    write_mode = 'OVERWRITE'
    accept_all_certs = False
    # print 'Number of args:', len(sys.argv)
    # print 'Argument List:', str(sys.argv)
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hlo:u:p:f:s:e:d:b:raw",
                                   ["help", "connection_type=", "profiles_dir=", "username=", "password=", "output_format=", "start_time=",
                                    "end_time=", "delim=", "dremio_bin_dir=", "reprocess", "output_type=",
                                    "write_mode="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print_usage()
            sys.exit()
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
    # output_filename = output_dir.rstrip('/') + ".csv"
    output_filename = profiles_dir + "audit_log" + ".csv"
    file_list = get_files(output_dir)
    # print(file_list)
    zi = 0
    for z_files in file_list[::2]:
        zip_file = output_dir + file_list[zi]
        zip_file_crc = output_dir + file_list[zi + 1]
        if zip_file.endswith('.zip'):
            with zipfile.ZipFile(output_dir + file_list[zi], 'r') as zip_ref:
                zip_ref.extractall(output_dir)
            os.remove(zip_file)
            os.remove(zip_file_crc)
            zi = zi + 2
    file_list = get_files(output_dir)
    output_file = open(output_filename, output_type)
    for f_profile in file_list:
        data = file_disk(output_dir, f_profile)
        job_id = re.search('profile_(.+?).JSON', f_profile).group(1)
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
                        output_file.write(job_id + delim + start_time + delim + user_name + delim + str(
                            data_source_num) + delim + table_name_list + delim + column_name_list + '\n')
                        data_source_num = data_source_num + 1
        os.remove(os.path.join(output_dir, f_profile))
    output_file.close()
    os.chmod(output_filename, 0o777)


def print_usage():
    print ('\n'
           'dremio-profile-extract.py <options below>\n'
           '   -h, --help\n'
           '   -c, --accept_all_certs\n'
           '       accept all ssl certificates\n'
           '       Default: False\n'
           '   -o, --output_dir <profile output dir> (default: /tmp/dremio/profiles/)\n'
           '   -l, --local-attach (Cannot be used with Username and Password)\n'
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


def update_output(output_dir, filename):
    return os.path.join(output_dir, filename) if filename else None


def get_files(output_dir):
    only_files = [f for f in os.listdir(output_dir) if os.path.isfile(os.path.join(output_dir, f))]
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
        print(dremio_bin_dir + "dremio-admin export-profiles --from '" + start_time + "' --to '" + end_time + "' -l  --output '" + profiles_dir + "'")
        ret_val = subprocess.Popen([dremio_bin_dir + "dremio-admin", "export-profiles", "--from ", start_time, "--to ", end_time, "-l ", " --output", profiles_dir], stdout=subprocess.PIPE)
        ret_val.wait()
        profile_dir = profiles_dir + get_dir(profiles_dir)[0]+ '/'

    else:
        print(dremio_bin_dir + "dremio-admin export-profiles --from '" + start_time + "' --to '" + end_time + "' -u '" + username + "' -p '" + password + "' --output '" + profiles_dir + "'")
        ret_val = subprocess.Popen([dremio_bin_dir + "dremio-admin", "export-profiles", "--from ", start_time, "--to ", end_time, "-u ", username, "-p ", password, " --output", profiles_dir], stdout=subprocess.PIPE)
        ret_val.wait()
        ret_string = ret_val.communicate()[0]
        ret_str_array = ret_string.splitlines()
        num_profiles = re.search('processed: (.*?),', ret_str_array[0].decode()).group(1)
        print("Number of profiles processed: " + num_profiles)
        profile_dir = re.search('path: (.*)$', ret_str_array[1].decode()).group(1)
    os.chmod(profile_dir, 0o777)
    return profile_dir


if __name__ == "__main__":
    main()
