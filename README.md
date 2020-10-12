# dremio-profile-extract

dremio-profile-extract.py <options below>
   -h, --help
   -c, --accept_all_certs
      accept all ssl certificates
      Default: False
   -o, --output_dir <profile output dir> (default: /tmp/dremio/profiles/)
   -b, --dremio_bin_dir <Dremio bin directory> (default: /opt/dremio/bin)
   -u, --username <admin username> (required)
   -p, --password <admin password> (required)
   -f, --output_format <ZIP or JSON> (default: ZIP)
   -s, --start_time <yyyy-mm-ddThh:mm:ss>
       Export profiles beginning from this date inclusively (job_end_time >=
       toDate).Example: 2011-12-03T10:15:30
   -e, --end_time <yyyy-mm-ddThh:mm:ss>
       Export profiles ending by this date exclusively (job_end_time <
       toDate).Example: 2011-12-03T10:15:30
   -d, --delim  <\\t (tab) , | (pipe), or any character> (default: \\t)
   -r  suppresses reprocessing of profile extract (same as --no_reprocess True)  (default: False)
   --no_reprocess <True, False> (default: False)
   -a  Appends to profile extract file\n' \
   -w  Overwrites profile extract file (default)
   --output_type <w, a> (default: w)
   --write-mode\n' \
     Specifies how we should handle a case, when target file already exists.
     Default: OVERWRITE Possible Values: [FAIL_IF_EXISTS, OVERWRITE, SKIP]
