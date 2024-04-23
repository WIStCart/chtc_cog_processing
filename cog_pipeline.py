#!/usr/bin/env python3

from minio import Minio
from minio.commonconfig import CopySource
import sys
import io
import argparse
import htcondor
import htcondor.dags
import csv
import pathlib
import time
import os
import fnmatch
import glob

'''
The S3Config class is for storing/passing information related to the S3 connection.
It can be initialized by passing an access key file, a secret key file, and an endpoint.
It defaults to using the users's CHTC S3 instance, with access/secret keys coming from
~/.chtc_s3/ and an endpoint of s3dev.chtc.wisc.edu.
'''
class S3Config:
    def __init__(self, akf, skf, endpoint):
        # A default init
        if not akf and not skf and not endpoint:
            home = os.getenv("HOME")
            with open(f"{home}/.chtc_s3/access.key", "r") as f:
                self.access_key = f.read().strip()
            with open(f"{home}/.chtc_s3/secret.key", "r") as f:
                self.secret_key = f.read().strip()

            self.access_key_file = f"{home}/.chtc_s3/access.key"
            self.secret_key_file = f"{home}/.chtc_s3/secret.key"
            self.endpoint = "s3dev.chtc.wisc.edu"

        # If all three required inputs are provided, we initialize with them instead
        elif akf and skf and endpoint:
            with open(akf, "r") as f:
                self.access_key = f.read().strip()
            with open(skf, "r") as f:
                self.secret_key = f.read().strip()

            self.access_key_file = akf
            self.secret_key_file = skf
            self.endpoint = endpoint

        # Otherwise, we determine we've received a partial input, and we don't know how to init.
        else:
            raise Exception("Access/secret keys must be provided together with an endpoint.")

'''
submit_DAG takes information passed to the script and generates a DAG of work for Condor
to process. In particular, it needs to know the input/output buckets used across the workflow,
and any of the raw CSV files that contain variables for each job.

Note that we're using DAGMan here because it gives us access to an automatically-executed
post script. This is important in the workflow, because it's how we determine which files/jobs
were completed successfully. In other words, it provides us with state tracking information.

Documentation for DAGMan can be found at https://htcondor.readthedocs.io/en/latest/automated-workflows/index.html
'''
def submit_DAG(in_bucket, out_bucket, csv_file_list, workflow_dir, pattern, max_running, s3conf):
    # Get the abs path of the current dir before we switch dirs so we can send the executable
    # cogs.sh
    cwd = os.getcwd()
    abs_path = str(pathlib.Path(cwd).absolute())
    script_path = str(pathlib.Path(sys.argv[0]).absolute())

    # Then cd into the workflow directory, so that various file operations and the DAG
    # are executed from the correct context
    os.chdir(workflow_dir)

    # Unlike a traditional submit file, the python bindings don't allow us to 
    # `queue from` a CSV. Instead, we read the CSV files and construct our input
    # arguments manually.
    files = []
    for csv_file in csv_file_list:
        print("READING CSV FILE: ", csv_file)
        with open(csv_file, 'r', newline='') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                files.append(tuple(row))

    print("FILES TO BE COMPUTED ON: ", files)

    submit_description = htcondor.Submit({
        # Set up universe stuff
        "universe":                "container",
        "container_image":         "docker://osgeo/gdal:ubuntu-small-3.6.3",

        # Set up logging info
        "log":                     "$(INFILE).log",
        "output":                  "$(INFILE).out",
        "error":                   "$(INFILE).err",

        # Set up the stuff we actually run
        # Note: In theory we're one directory deeper than the execution context of the script
        # so we grab the executable from up a dir.
        "executable":              "../generate_cogs.sh",
        # "COGFILE":                 "cog_$(INFILE)",
        "arguments":               "$(INFILE) $(BANDS)",

        # And requirements for running stuff
        "request_disk":            "1GB",
        "request_memory":          "1GB",
        "request_cpus":            1,
        
        # Set up the S3/file transfer stuff
        "aws_access_key_id_file":  f"{s3conf.access_key_file}",
        "aws_secret_access_key_file": f"{s3conf.secret_key_file}",
        "transfer_input_files":    f"s3://{s3conf.endpoint}/{in_bucket}/$(FULL_INFILE)",
        # After conversion, $INFILE is actually the generated COG, so we still want to transfer it to the output bucket
        "transfer_output_remaps":  f"\"$(INFILE) = s3://{s3conf.endpoint}/{out_bucket}/$(FULL_INFILE); $(OUTFILE) = s3://{s3conf.endpoint}/{out_bucket}/$(FULL_OUTFILE);\"",
        "should_transfer_files":   "YES",
        "when_to_transfer_output": "ON_EXIT",

        # Release the job after an amount of time that increases exponentially with the number of retries (this is a backoff strategy to give
        # the S3 endpoint time to recover if it's overloaded)
        "periodic_release": f"((time() - EnteredCurrentStatus) >= ((((ClusterId % {int(int(max_running)/10)}) * 60) + 60) * pow(2, NumHolds)))",

        # Remove the job if RetryCount exceeds 5
        "periodic_remove": "NumHolds > 5",
    })

    # Generate input args from the CSVs we read earlier
    input_args = [{
        "node_name": "cog-pipeline-node",
        # Condor will flatten anything from S3 that looks like a subfolder, eg subfolder/my-image.tif gets
        # downloaded as my-image.tif. We need to keep track of the full path so we can transfer it back to the
        # output bucket.
        "FULL_INFILE": files[idx][0],
        "INFILE": ((files[idx][0]).split("/"))[-1],
        "FULL_OUTFILE": (files[idx][0]).replace(".tif", ".jpg"),
        "OUTFILE": (((files[idx][0]).split("/"))[-1]).replace(".tif", ".jpg"),
        "BANDS": files[idx][1]} for idx in range(len(files))]
    print("ABOUTY TO SUBMIT EP JOBS WITH INPUT ARGS: ", input_args)

    # Set up our post script -- this is how we manage state tracking to determine which jobs were actually successful
    # The post script will work by checking the output bucket for all of the expected files.
    script_args = [ "post", f"--input-bucket {in_bucket}", f"--output-bucket {out_bucket}", f"-a {s3conf.access_key_file}",
        f"-s {s3conf.secret_key_file}", f"-e {s3conf.endpoint}"]

    script = htcondor.dags.Script(script_path, script_args)

    # Set up the DAG we use as a job runner
    dag = htcondor.dags.DAG()
    dag.layer(
        name = f"COG-Pipeline-DAG-{pattern}",
        submit_description = submit_description,
        vars = input_args,
        # Allow each individual job to retry itself a max of 3 times
        retries=int(3),
    )

    # The post script needs to be run by our final node, after all of the worker jobs claim they've
    # completed. Here, we manually instantiate the final node (otherwise it's implicit, and runs in
    # a default configuration)
    dag.final(
        name="final",
        post=script,
    )

    dag_file = htcondor.dags.write_dag(dag, os.getcwd(), node_name_formatter=htcondor.dags.SimpleFormatter("_"))
    dag_submit = htcondor.Submit.from_dag(str(dag_file), {
        'batch-name': f"COG-Pipeline-DAG-{pattern}",
        # Hardcode that we don't want more than 10k jobs running at once
        'maxjobs': max_running,
    })

    print("Submitting DAG job...")
    schedd = htcondor.Schedd()
    submit_result = schedd.submit(dag_submit)
    print("Job was submitted")


'''
The overall workflow this script produces uses an HTCondor Cron Job (aka Crondor) to check
for certain triggers that kick of the larger workflow. This function allows us to periodically
monitor the input bucket for new CSV files that contain job information for each of the images
we need to process. Because of this, the CSV files shouldn't be uploaded until we know all of the
files indicated in the file have successfully uploaded.
'''
def submit_crondor(in_bucket, out_bucket, pattern, max_running, s3conf):
    script_path = str(pathlib.Path(sys.argv[0]).absolute())
    print("SCRIPT PATH: ", script_path)

    submit_description = htcondor.Submit({
        "executable":              script_path,
        "arguments":               f"crondor --input-bucket {in_bucket} --output-bucket {out_bucket} -p {pattern} -s {s3conf.secret_key_file} -a {s3conf.access_key_file} -e {s3conf.endpoint} -m {max_running}",
        "universe":                "local",
        "request_disk":            "512MB",
        "request_cpus":            1,
        "request_memory":          512,
        "log":                     "crondor_$(CLUSTER).log",
        "should_transfer_files":   "YES",
        "when_to_transfer_output": "ON_EXIT",
        "output":                  "crondor_$(CLUSTER).out",
        "error":                   "crondor_$(CLUSTER).err",

        # Cron directives. Currently set to run every 15 minutes
        "cron_minute":             "*/15",
        "cron_hour":               "*",
        "cron_day_of_month":       "*",
        "cron_month":              "*",
        "cron_day_of_week":        "*",
        "on_exit_remove":          "false",

        # Specify `getenv` so that our script uses the appropriate environment
        # when it runs in local universe. This allows the crondor to access
        # modules we've installed in the base env (notably, minio)
        "getenv":                  "true",

        # Finally, set the batch name so we know this is a crondor job
        "JobBatchName":            f"COG-Pipeline-Crondor-{pattern}",
    })

    schedd = htcondor.Schedd()
    submit_result = schedd.submit(submit_description)
    print("Crondor job was submitted with JobID %d.0" % submit_result.cluster())

# Given a bucket, and a map of object renames, rename the objects in the bucket
def rename_object(bucket, obj_rename_map, s3conf):
    # Initialize the Minio client
    minioClient = Minio(s3conf.endpoint,
                        access_key=s3conf.access_key,
                        secret_key=s3conf.secret_key,
                        secure=True)

    for obj, new_obj in obj_rename_map.items():
        print(f"RENAMING {bucket}/{obj} TO {bucket}/{new_obj}")
        # There doesn't seem to be the concept of 'mv' with MinIO, so we copy/delete instead
        try:
            minioClient.copy_object(bucket, new_obj, CopySource(bucket, obj))
            minioClient.remove_object(bucket, obj)
        except Exception as e:
            raise Exception(f"Command failed: {str(e)}")

# Given a bucket and a map of local-->remote file names, put the objects into the remote bucket
def put_object(bucket, object_map, s3conf):
    # Initialize the Minio client
    minioClient = Minio(s3conf.endpoint,
                        access_key=s3conf.access_key,
                        secret_key=s3conf.secret_key,
                        secure=True)

    for local_name, remote_name in object_map.items():
        print(f"MOVING {local_name} to {bucket}/{remote_name}")
        try:
            minioClient.fput_object(bucket, remote_name, local_name)
        except Exception as e:
            raise Exception(f"Command failed: {str(e)}")

# Given a bucket and a list of objects, delete the objects from the bucket
def remove_object(bucket, object_list, s3conf):
    # Initialize the Minio client
    minioClient = Minio(s3conf.endpoint,
                        access_key=s3conf.access_key,
                        secret_key=s3conf.secret_key,
                        secure=True)

    for obj in object_list:
        print(f"DELETING {obj} from {bucket}/{obj}")
        try:
            minioClient.remove_object(bucket, obj)
        except Exception as e:
            raise Exception(f"Command failed: {str(e)}")

# Given a bucket and a list of objects, fetch the objects from the bucket
def fetch_input_objects(bucket, object_list, s3conf):
    # Initialize the Minio client
    minioClient = Minio(s3conf.endpoint,
                        access_key=s3conf.access_key,
                        secret_key=s3conf.secret_key,
                        secure=True)

    for obj in object_list:
        try:
            data = minioClient.get_object(bucket, obj)

            # The input CSV may be nested in an S3 "subfolder". We flatten that here.
            with open(obj.split("/")[-1], 'wb') as file_data:
                for d in data.stream(32*1024):
                    file_data.write(d)
        except Exception as e:
            raise Exception(f"Failed to fetch__input_objects: {str(e)}")

# Given a bucket and a glob-like pattern, determine any files in the bucket that match
def get_matching_objects(bucket, pattern, s3conf):
    # Initialize the Minio client
    minioClient = Minio(s3conf.endpoint,
                        access_key=s3conf.access_key,
                        secret_key=s3conf.secret_key,
                        secure=True)
    objects = minioClient.list_objects(bucket, recursive=True)
    # Filter the object names using the glob pattern
    files = [obj.object_name for obj in objects if fnmatch.fnmatch(obj.object_name, pattern)]

    return files

'''
crondorMain is the main exectuable for the Crondor script. When the crondor wakes up,
it will execute crank.py with the `crondor` command. This function is responsible for
checking/fetching the files that match the glob pattern, submitting the DAG, and then
renaming the files in the bucket so we don't reprocess them.
'''
def crondorMain():
    parser = argparse.ArgumentParser(description="SCO GeoTiff Workflow Tool")
    parser.add_argument("command", help="Helper command to run", choices=["crondor"])
    parser.add_argument("--input-bucket", help="The bucket to check for matching objects.")
    parser.add_argument("--output-bucket", help="The output bucket and generated files should be placed in.")
    parser.add_argument("-p", "--pattern", help="The glob pattern to match against.")
    parser.add_argument("-s", "--secret-key", help="The secret key file to use for the S3 connection.")
    parser.add_argument("-a", "--access-key", help="The access key file to use for the S3 connection.")
    parser.add_argument("-e", "--s3-endpoint", help="The hostname of the s3 endpoint to connect to. Defaults to s3dev.chtc.wisc.edu.")
    parser.add_argument("-m", "--max-running", help="The maximum number of jobs to run at once.", default=10000)
    args = parser.parse_args()

    s3conf = S3Config(args.access_key, args.secret_key, args.s3_endpoint)

    # Get any of the matching files. These contain info about the actual files we need to compute on.
    matching_files = get_matching_objects(args.input_bucket, args.pattern, s3conf)
    if len(matching_files) > 0:
        print("MATCHING FILES: ", matching_files)
        fetch_input_objects(args.input_bucket, matching_files, s3conf)
        print(f"Downloaded {len(matching_files)} files")
       
        # We downloaded the remote objects, and in the process we flattened
        # any S3 subfolders. Now we update the matching files list to reflect that.
        local_matching_files = [f.split("/")[-1] for f in matching_files]

        rename_map = {}
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        # Create a timestamped directory to store the files in
        workflow_dir = "workflow-run-" + timestamp
        os.makedirs(workflow_dir, exist_ok=True)

        # We have two things to do. First, we ned to rename the file locally to indicate we're processing
        # Second, we need to rename the remote file to prevent any re-processing
        for index, file in enumerate(local_matching_files):
            # FOR NOW WE ASSUME EVERY FILE ENDS IN raw.csv
            # TODO: Make this work with the glob pattern

            # Create a base filename that contains our timestamp
            file_base = f"{file[0:-7]}processing-{timestamp}.csv"

            # Create the rename map for the remote file by stripping off the file and adding the new base
            remote_parts = matching_files[index].split("/")
            new_remote_path = "/".join(remote_parts[0:-1] + [file_base])
            rename_map[matching_files[index]] = new_remote_path

            # Also, save the files locally
            os.rename(file, f"{workflow_dir}/{file_base}")

            # Update the name of the file in the list. We don't need the workflow dir,
            # to be included, because we'll be executing the DAG from the context of the 
            # workflow directory
            local_matching_files[index] = f"{file_base}"

        # We now have the files, use them to submit the actual workflow
        submit_DAG(args.input_bucket, args.output_bucket, local_matching_files, workflow_dir, args.pattern, args.max_running, s3conf)

        # Finally, rename the remote objects to prevent re-processing
        rename_object(args.input_bucket, rename_map, s3conf)

'''
topMain is the main executable for the script. When running the script from the command line
on the AP, this is the function that gets called first. It's responsible for setting up/running
the crondor that acts as our trigger monitor
'''
def topMain():
    parser = argparse.ArgumentParser(description="SCO GeoTiff Workflow Tool")
    parser.add_argument("--input-bucket", help="The bucket to check for matching objects.", required=True)
    parser.add_argument("--output-bucket", help="The bucket to check for matching objects.", required=True)
    parser.add_argument("-p", "--pattern", help="The glob pattern to match against.", required=True)
    parser.add_argument("-s", "--secret-key", help="The secret key file to use for the S3 connection.")
    parser.add_argument("-a", "--access-key", help="The access key file to use for the S3 connection.")
    parser.add_argument("-e", "--s3-endpoint", help="The hostname of the s3 endpoint to connect to. Defaults to https://s3dev.chtc.wisc.edu.")
    parser.add_argument("-m", "--max-running", help="The maximum number of jobs to run at once.", default=10000)
    args = parser.parse_args()

    s3conf = S3Config(args.access_key, args.secret_key, args.s3_endpoint)
    submit_crondor(args.input_bucket, args.output_bucket, args.pattern, args.max_running, s3conf)
    return 0

'''
postScript is used for post-processing. When Condor reports that the DAG of work is complete,
the DAG's final node invokes the post script to check that any file we expect to be generated
and placed in the output bucket by the workflow is actually there. It works by checking the 
csv of work and checking for files matching column 1 and 2 in the output, as these correspond
to the tif and jpg files we expect to be generated.

For a given line in the CSV, if both files are found, we write the line to a "processed" file. If
either of the files is missing, we instead write the line to a "failed" file. These files are then
copied back to the input bucket, and the original processing file is removed.
'''
def postScript():
    parser = argparse.ArgumentParser(description="SCO GeoTiff Workflow Tool")
    parser.add_argument("command", help="Helper command to run", choices=["post"])
    parser.add_argument("--input-bucket", help="The bucket to check for matching objects.")
    parser.add_argument("--output-bucket", help="The bucket to check for matching objects.")
    # parser.add_argument("-p", "--pattern", help="The glob pattern to match against.")
    parser.add_argument("-s", "--secret-key", help="The secret key file to use for the S3 connection.")
    parser.add_argument("-a", "--access-key", help="The access key file to use for the S3 connection.")
    parser.add_argument("-e", "--s3-endpoint", help="The hostname of the s3 endpoint to connect to. Defaults to https://s3dev.chtc.wisc.edu.")
    args = parser.parse_args()

    # DAGMan doesn't capture stdout/stderr from our post script, so we do that and manually create a file
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()
    sys.stdout = stdout_buf
    sys.stderr = stderr_buf

    try:
        s3conf = S3Config(args.access_key, args.secret_key, args.s3_endpoint)

        # All of the "processing" files are downloaded to the AP. For each match, we check the output bucket.
        # Based on contents of output bucket, we determine which files to write back to the input bucket.
        for file in glob.glob("*processing-*"):
            successful_files = []
            failed_files = []
            with open(file, 'r') as processing_file:
                csv_reader = csv.reader(processing_file)

                for row in csv_reader:
                    # We know there should be an output tif and an output jpg in the first two columns
                    tif_im = row[0]
                    jpg_im = tif_im.replace(".tif", ".jpg")

                    if len(get_matching_objects(args.output_bucket, tif_im, s3conf)) > 0 and len(get_matching_objects(args.output_bucket, jpg_im, s3conf)) > 0:
                        # Objects found!
                        successful_files.append(row)
                    else:
                        # Whoops, something isn't right :(
                        failed_files.append(row)

            # Here we determine which files we're going to delete from the input bucket, along with their path,
            # which we use for storing our post-processing files.
            # NOTE: We've already made the assumption that all processing files have a UNIQUE BASE NAME, so this list
            # MUST have only one element!!!
            remote_file_to_rename = get_matching_objects(args.input_bucket, f"*{file}", s3conf)[0]
            remote_path = "/".join(remote_file_to_rename.split("/")[0:-1])

            copy_map = {}
            if len(successful_files) > 0:
                fname = file.replace("processing", "processed")
                with open(fname, 'w') as processed:
                    csv_writer = csv.writer(processed)
                    csv_writer.writerows(successful_files)
                copy_map[fname] = remote_path+"/"+fname

            if len(failed_files) > 0:
                fname = file.replace("processing", "failed")
                with open(fname, 'w') as failed:
                    csv_writer = csv.writer(failed)
                    csv_writer.writerows(failed_files)
                copy_map[fname] = remote_path+"/"+fname


            # Copy the new files back to the input bucket and delete the "processing" file
            put_object(args.input_bucket, copy_map, s3conf)

            for f in copy_map.keys():
                os.remove(f)

            # Designate all the remote files matching the current file for deletion
            remove_object(args.input_bucket, [remote_file_to_rename], s3conf)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # After post script work is done or in case of an error, retrieve the contents of the buffers
        stdout_content = stdout_buf.getvalue()
        stderr_content = stderr_buf.getvalue()

        # Write the contents of the buffers to files
        with open('post.out', 'w') as f:
            f.write(stdout_content)
        with open('post.err', 'w') as f:
            f.write(stderr_content)

        # Revert stdout and stderr back to their original states. Probably don't need to do this, but can't hurt.
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

def main():
    if len(sys.argv) > 1:
        if sys.argv[1] in ["crondor"]:
            crondorMain()
            return 0
        elif sys.argv[1] in ["post"]:
            postScript()
            return 0

    return topMain()

if __name__ == '__main__':
    sys.exit(main())
