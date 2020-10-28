# dremio-profile-extract
Usage:\
python ./dremio-profile-extract.py -u <ADMIN_USER> -p <ADMIN_PASSWORD>\
or\
python ./dremio-profile-extract.py -l  (this will return all available profiles)\
or\
python ./dremio-profile-extract.py     (this will default to the -l -i options)\
\
Note: -u and -p or -l are required, all other parameters are optional or have a default value.\
\
dremio-profile-extract.py \<options below\>\
&nbsp;&nbsp;&nbsp;-h, --help<br \>
&nbsp;&nbsp;&nbsp;-c, --accept_all_certs<br \>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;accept all ssl certificates<br \>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Default: False<br \>
&nbsp;&nbsp;&nbsp;-o, --output_dir <profile output dir> (default: /tmp/dremio/profiles/)\
&nbsp;&nbsp;&nbsp;-b, --dremio_bin_dir <Dremio bin directory> (default: /opt/dremio/bin)\
&nbsp;&nbsp;&nbsp;-u, --username <admin username> (required)\
&nbsp;&nbsp;&nbsp;-p, --password <admin password> (required)\
&nbsp;&nbsp;&nbsp;-f, --output_format <ZIP or JSON> (default: ZIP)\
&nbsp;&nbsp;&nbsp;-s, --start_time <yyyy-mm-ddThh:mm:ss>\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Export profiles beginning from this date inclusively (job_end_time >= toDate).\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Example: 2011-12-03T10:15:30\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Default: 1990-01-01T00:00:00\
&nbsp;&nbsp;&nbsp;-e, --end_time <yyyy-mm-ddThh:mm:ss>\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Export profiles ending by this date exclusively (job_end_time < toDate).\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Example: 2011-12-03T10:15:30\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Default: Date and Time the script is initiated\        
&nbsp;&nbsp;&nbsp;-d, --delim  <\\t (tab) , | (pipe), or any character> (default: \\t)\
&nbsp;&nbsp;&nbsp;-r  suppresses reprocessing of profile extract (same as --no_reprocess True)  (default: False)\
&nbsp;&nbsp;&nbsp;--no_reprocess <True, False> (default: False)\
&nbsp;&nbsp;&nbsp;-a  Appends to profile extract file\n' \
&nbsp;&nbsp;&nbsp;-w  Overwrites profile extract file (default)\
&nbsp;&nbsp;&nbsp;--output_type <w, a> (default: w)\
&nbsp;&nbsp;&nbsp;--write-mode\n' \
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Specifies how we should handle a case, when target file already exists.\
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Default: OVERWRITE Possible Values: [FAIL_IF_EXISTS, OVERWRITE, SKIP]\
