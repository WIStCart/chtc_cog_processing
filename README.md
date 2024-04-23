# SCO Cloud Optimized GeoTiff (COG) Generation Pipeline

## Overview

This tool can be used with the UW-Madison Center for High Throughput Computing infrastructure to generate cloud optimized geotiff images from standard geotiffs.

The tool has three main components:
- A periodic monitor (HTCondor Cron job, or Crondor for short) that watches for triggers indicating
there is work to do
- A Directed Acyclic Graph (DAG) workflow that runs any jobs generated by the trigger
- A post-processing script that manages state tracking for successful/failed jobs run through the DAG

When the script is first run, a Crondor job is submitted on the Access Point. This Crondor is responsible for checking an input S3 bucket every 15 minutes for CSV files that match `*raw.csv`, where the CSV is assumed to contain all of the information necessary for the underlying `generate_cogs.sh` executable to run. Importantly, the appearance of a matching file **MUST** indicate that _all_ files detailed in the CSV are already present in the input bucket. In other words, don't upload the CSV until everything else is finished uploading.

When a matching CSV is found, the Crondor downloads the file to the AP and uses it to generate and submit a
DAG of conversion work. After the DAG is submitted, the script renames the remote `*raw.csv` files to something like `*processing-<TIMESTAMP>.csv` to indicate they are currently being processed. This is needed to avoid re-processing the same inputs.

Each node/job in the DAG is responsible for the conversion of one image. After the image is converted, the
node/job moves the outputs (one .tif and one .jpg) to the output bucket.

When all nodes/jobs in the DAG have completed, the post-processing script checks the output bucket for all of
expected files. If any image pairs are missing, the script writes the corresponding row from the `*raw.csv`
to a file that aggregates conversion failures. Likewise, any image pairs that are found in the output bucket
are written to a file that aggregates success. These two files the replace the `processing-<TIMESTAMP>` file
in the input bucket and can be used for monitoring jobs that might require intervention.

## CSV Structure

The input CSV is used to determine which images in the bucket are ready for processing, along with the number of bands
the image has (which is important for the actual cog-->jpg conversion). Importantly, this CSV assumes two columns:
`bucket/path/to/input_image.tif,<NUM BANDS>`
where `bucket/path/to/input_image.tif` is the name of the image from the input bucket, and `<NUM BANDS>` should be replaced
with the appropriate number of bands for the image. Under the hood, the script uses the image name to create two output images
at the output bucket, one with the same name as the input image, and one with the same name but formatted as a `.jpg`.

**NOTE:** One assumption made by the pipeline script is that *all* images being processed end in `.tif`. Failure to follow
this naming scheme may result in a broken workflow.
**NOTE:** Image paths in the CSV should not contain a preceding `/`. Buckets in S3 don't belong to a root like directories in
filesystems do. The only exception to this rule is if the bucket was explicitly named to contain a preceding `/`.
**WARNING:** One assumption that's made is that any CSV files used to start the workflow have unique base names. That is, if the
input bucket has `path/to/files-raw.csv`, there MUST NOT be another `files-raw.csv` in the bucket. So this is illegal:
`different/path/files-raw.csv`
But this is okay:
`different/path/different-raw.csv`

## Setup

Start by cloning this repository in a user home holder on the Access Point (typically ap2002.chtc.wisc.edu).

```
git clone https://github.com/WIStCart/chtc_cog_processing.git
cd chtc_cog_processing
```

You'll first need to set up several environment pieces. After installing Conda (use the "Create a Miniconda Installation" instructions [here](https://chtc.cs.wisc.edu/uw-research-computing/conda-installation)),
create the required conda environment by running `conda env create -f environment.yml`. After the environment is created, run `conda activate cog_pipeline`.

Finally, you'll need to make sure the actual python script is executable. To do this, run `ls -la` in the repository directory. Locate the line containing the python script
and make sure you see three `x`'s, indicating the script has the correct executable permission. For example, this will cause errors:
```
-rw-r--r-- 1 my_username my_group    20309 Mar 29 15:54 cog_pipeline.py
```
but this is okay:
```
-rwxr-xr-x 1 my_username my_group    20309 Mar 29 15:54 cog_pipeline.py
```

If the script does not have execution permissions, run `chmod +x cog_pipeline.py` and double check that it succeeded by running `ls -la` again.

## Start the Processing Pipeline

This script assumes access to an input and output bucket (which may be the same bucket) that are hosted at some S3 endpoint. By default, we assume the CHTC S3 instance hosted at s3dev.chtc.wisc.edu, although any endpoint can be used as long as you possess a valid access/secret key pair.

To launch the Crondor job, run:
```
python3 cog_pipeline.py  -a <ACCESS KEY FILE PATH> -s <SECRET KEY FILE PATH> -e <S3 ENDPOINT URL> --input-bucket <INPUT BUCKET> --output-bucket <OUTPUT BUCKET> --max-running <num concurrent jobs> -p "*raw.csv"
```

Example:

```
python3 cog_pipeline.py -a ~/rmls3_keyid.txt -s ~/rmls3_accesskey.txt -e web.s3.wisc.edu --input-bucket rml-chtc-inputs --output-bucket rml-chtc-outputs --max-running 500 -p "*raw.csv"
```

The above only needs to be run once.  The Crondor job will run indefinitely, and does not need to be re-run at next time the user logs in.  As a best practice, it would be best to kill the Crondor job with a `condor_rm {username}` if there will no image processing happening for a long period of time.  

When matching files are found in the input bucket, a new directory is created wherever the script was initially run. This directory will be something like `workflow-run-<TIMESTAMP>`. All files Condor creates as a part of the DAG, as well as the CSV files of images, will be located here.

## Troubleshooting

### Diagnosing Held Jobs

> **NOTE:** If any jobs are placed on hold with an error that indicates job credentials aren't available, try running
`condor_submit scitokens_workaround.sub` before launching the DAG. This submits a hold job that forces Condor
to make fresh credentials available to the DAG.

Another potential issue can occur if too many jobs hammer the S3 endpoint at once. Held jobs that indicate this may be happening
will have hold messages similar to:
```
<Job ID>   <your username>       4/10 12:47 Transfer input files failure at execution point slot1_118@e4029.chtc.wisc.edu using protocol https. Details: Error from slot1_118@e4029.chtc.wisc.edu: FILETRANSFER:1:non-zero exit (1) from /usr/libexec/condor/curl_plugin. |Error: Aborted due to lack of progress (with environment: http_proxy='http://squid-cs-2360.chtc.wisc.edu:3128', https_proxy='') ( URL file = <URL of the file that failed to download>?... )|
```

You can see the reason for a held job by running `condor_q -hold <JOB ID>`  or for all of your jobs by running `condor_q -hold <your username>`

If too many held jobs begin accumulating, they can be released with `condor_release <JOB ID>` for individual jobs or with `condor_release -constraint 'JobBatchName=="COG-Pipeline-Crondor-<insert the pattern here>"'`. Another troubleshooting step may be to re-submit the workflow with a lower value for
`--max-running`.

### Other Types of Failure
If you believe some job encountered a different error, there are several files worth examining.

First, the directory from which you ran `python3 cog_pipeline.py` will have 3 files you can check for debugging info related to
the Crondor job that was submitted:

1. `crondor_$(CLUSTER).log`
2. `crondor_$(CLUSTER).out`
3. `crondor_$(CLUSTER).err`

In each of these files, `$(CLUSTER)` will be replaced by the job's ID. When you first run the cog_pipeline script, only the `.log` file
will exist. The `.out` and `.err` files are created the first time the Crondor job wakes up and checks for input files to compute on.

When the Crondor generates other image processing jobs, it creates a directory like:
```
workflow-run-<timestamp>
```

Any log files associated with those jobs will be located in that directory. While there will be many `dag` files in the directory,
only a few will contain useful information in the bulk of cases:

1. `<tif image name>.log/out/err`: These files will contain job specific information for the image being processed
2. `post.out/err`: These files contain the results of the DAG's post script, which is responsible for determining which jobs completed and which failed.

## Workflow Management

### Crondor Jobs

When first running the pipeline script, you submit a single Crondor job. To observe this job, run `condor_q`. This command displays all of the
HTCondor jobs in your current queue. The job you just submitted will have a `BATCH_NAME` like:
```
COG-Pipeline-Crondor-<pattern>
```
where `<pattern>` is the glob pattern you initially passed to the script.

For example, if you run:
```
python3 cog_pipeline.py -a ~/rmls3_keyid.txt -s ~/rmls3_accesskey.txt -e web.s3.wisc.edu --input-bucket rml-chtc-inputs --output-bucket rml-chtc-outputs -p "*raw.csv"
```

Then the job batch name will be:
```
COG-Pipeline-Crondor-*raw.csv
```

If you need to remove this job from the queue, there are two ways to do that. You can either remove it by the job's ID with:
```
condor_rm <the job's ID from JOB_IDS column>
```

Or you can remove it using the job's batch name:
```
condor_rm -c 'JobBatchName=="COG-Pipeline-Crondor-<insert the pattern here>"'
```

### Workflow Jobs

In the event that the Crondor job finds work to do, it will submit a DAG of jobs, where each job processes a single tif.
When this happens, you'll see a row after running `condor_q` with a batch name like
```
COG-Pipeline-DAG-<pattern>
```
One thing you'll notice about these jobs is that each file being processed has its own job id. As such, you can either remove
each individual job with:
```
condor_rm <job id>
```
or you can remove the entire batch of jobs with:
```
condor_rm -c 'JobBatchName=="COG-Pipeline-DAG-<insert the pattern here>"'
```

### Watching the workflow

If you want to monitor the progress of the Crondor and DAG jobs, you can do so by running:
```
condor_watch_q
```
This prints updates to the terminal in near realtime.
