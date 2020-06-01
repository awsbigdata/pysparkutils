

# This function implements copyMerge from Hadoop API
# copyMerge will be deprecated in Hadoop 3.0
# This can be used in a pySpark application (assumes `sc` variable exists)
# Add header first and then rest of files. 
def copyMerge (headerNames,src_dir, dst_file, overwrite=False, debug=False):
    
    hadoop = sc._jvm.org.apache.hadoop
    conf = hadoop.conf.Configuration()
    fs = hadoop.fs.FileSystem.get(conf)
    apache_ioutils=sc._jvm.org.apache.commons.io.IOUtils
    # check files that will be merged
    files = []
    for f in fs.listStatus(hadoop.fs.Path(src_dir)):
        if f.isFile():
            files.append(f.getPath())
    if not files:
        raise ValueError("Source directory {} is empty".format(src_dir))
    files.sort(key=lambda f: str(f))

    out_stream = fs.create(hadoop.fs.Path(dst_file), overwrite)
    if headerNames is not None:
        if debug: 
                print("Appending Headers {} into {}".format(headerNames, dst_file))
        try:
            ins=apache_ioutils.toInputStream(headerNames)
            hadoop.io.IOUtils.copyBytes(ins, out_stream, conf, False)     # False means don't close out_stream
        finally:
            ins.close()
    try:
        # loop over files in alphabetical order and append them one by one to the target file
        for file in files:
            if debug: 
                print("Appending file {} into {}".format(file, dst_file))

            in_stream = fs.open(file)   # InputStream object
            try:
                hadoop.io.IOUtils.copyBytes(in_stream, out_stream, conf, False)     # False means don't close out_stream
            finally:
                in_stream.close()
    finally:
        out_stream.close()


###Extract header from dataframe
header=",".join([col.name for col in df.schema])


copyMerge(header,'/user/glue/output/merge','/user/glue/output/final2.csv',debug=True)



  
