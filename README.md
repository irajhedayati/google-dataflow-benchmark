# Google Dataflow Benchmark

This tool is specialized in benchmarking Dataflow jobs on Google platform.

Use cases:
- find the best machine type for your workers (family and size)
- find the proper window size if your job has any windowing (for batching or aggregation)
- test how your job handle different types of loads

Goal: 

> reduce the cost and improve the performance

Method:

1. Deploy test bed and seed input data
2. Take a snapshot of the Pub/Sub subscriptions
3. Run pipeline with configuration A
4. Wait until all messages are processed
5. Collect stats
6. Restore PubSub subscription snapshot
7. Delete the snapshot
8. Repeat steps 2-7 for N different pipeline configurations

![Benchmark anatomy.png](Benchmark%20anatomy.png)

Support table:

| Feature            | is supported?      |
|:-------------------|:-------------------|
| Streaming          | :heavy_check_mark: |
| Streaming Engine   | :X:                |
| Batch              | :X:                |
| Dataflow Prime     | :X:                |
| Cost estimate      |                    |
| - CPU              | :heavy_check_mark: |
| - memory           | :heavy_check_mark: |
| - GPU              | :heavy_check_mark: |
| - Disk             | :heavy_check_mark: |
| - Streaming Engine | :X:                |
| - Dataflow Prime   | :X:                |

# Preparation

## Prepare local environment

Make sure you have 

- JRE or JDK 11 or higher (run `java -version` to make sure)
- [Install the gcloud CLI](https://cloud.google.com/sdk/docs/install)

Install the `google-dataflow-benchmark` tool locally. 

1. Build (for this, you need to install `sbt` command and have JDK)
    ```bash
    cd $HOME && git clone https://github.com/irajhedayati/google-dataflow-benchmark.git
    cd google-dataflow-benchmark
    sbt package
    ```
2. Download a specific version 

[//]: # (TODO add link to the packages on GitHub) 

Authenticate first:

```bash
gcloud config set project perf-benchmark-project
gcloud auth login --project=perf-benchmark-project
```

The last step is to build your Dataflow job and have an uber JAR ready to deploy.
Make sure that the logging is set properly as the benchmark will try to parse the log of `DataflowRunner` to get
the job ID which is used later to extract metrics of the job.

You can create a file under the `resources` directory called `log4j.properties` with the following content:

```
log4j.rootLogger=INFO,stdout

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%p\t%d{ISO8601}\t%r\t%c\t[%t]\t%m%n
```

## Prepare GCP environment

A Dataflow streaming job is usually pulling messages from a Pub/Sub subscription.
Create a topic for your benchmarks and then mark "Add a default subscription".
It will create a subscription for you, take a note of that.

For each benchmark, it will create a snapshot of the subscription, run the benchmark, and then restore the snapshot.
Although it gives us predictable and repeatable benchmark results, it doesn't simulate the spikes and message bursts.

# Create the test

Create a config file. The config file follows [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md#hocon-human-optimized-config-object-notation) 
format.

The top level should be `google-dataflow-benchmark` i.e.

```
google-dataflow-benchmark {
}
```

see the [reference.conf](./src/main/resources/reference.conf) file.

Here are the configurations:

| Key                        | Description                                                                                                                                                                                       | Default | Example                                      |
|:---------------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------|:---------------------------------------------|
| jar-file                   | Executable Jar file containing Dataflow application                                                                                                                                               | null    | /path/to/dataflow-job.jar                    |
| class-name                 | the entry point to your Dataflow job                                                                                                                                                              | null    | ca.dataedu.dataflow.Main                     |
| gcp-bucket                 | a GCS bucket to use for temp and staging locations. This is used to set `gcpTempLocation` and `stagingLocation` options                                                                           | null    | iraj-test                                    |
| project                    | the GCP project                                                                                                                                                                                   | null    | dataedu-web                                  |
| region                     | the region to run the job. It's used to pass to the `gcloud` command                                                                                                                              | null    | us-central1                                  |
| benchmarks                 | a list of benchmark definitions (see the next table)                                                                                                                                              | []      | -                                            |
| default-worker-log-level   | log level of workers                                                                                                                                                                              | INFO    | INFO                                         |
| input-pub-sub-subscription | the Pub/Sub subscription that is used as input to the job                                                                                                                                         | null    | projects/dataedu-web/subscriptions/input-sub |
| output-transforms          | a list of Apache Beam transform name that generate output. This is used to get throughput metrics. You can get them from your Dataflow job UI and it is in form of "PTrabnsform1/PTransform2/..." | []      | -                                            |
| gcloud-path                | the path to `gcloud` binary on the orchestrator machine to communicate with Google services (output of `which gcloud`)                                                                            | null    | /Users/iraj/opt/google-cloud-sdk/bin/gcloud  |
| number-of-records          | total number of records in your test bed; this is used to estimate the pipeline throughput                                                                                                        | null    | 5000000                                      |
| output-file                | a path to generate the report in CSV                                                                                                                                                              | null    | /Users/iraj/output.csv                       |

Benchmark configuration:

| Key                   | Description                                                          | Type            | Example                            |
|:----------------------|:---------------------------------------------------------------------|:----------------|:-----------------------------------|
| name                  | a unique name of the benchmark that is used in logs and final report | String          | n1-standard-4                      |
| machine-type          | the machine type to use as Dataflow worker                           | String          | n1-standard-4                      |
| number-of-workers     | the number of workers                                                | Integer         | 1                                  |
| max-number-of-workers | the maximum number of workers                                        | Integer         | 1                                  |
| disk-size-gb          | the disk size for worker in GB                                       | Integer         | 100                                |
| application-args      | the command line arguments to pass to the application                | Array of String | ["--arg1=value1", "--arg2=value2"] |


# Run

```bash
java -cp google-dataflow-benchmark_2.13-0.1.0.jar -Dconfig.file=/path/to/my-benchmark.conf
```

Where `/path/to/my-benchmark.conf` is the path to the config file you created in the previous step.

For each benchmark, it will create a snapshot of the provided subscription. The snapshot name prefix is
the name of subscription and the suffix is 6 characters of random lowercase characters and digits.

# Cleanup

The benchmark will take care of the resources it has created. But you need to cleanup the resources
you created to avoid extra charges.

- The Pub/Sub topic and subscription that you created in the preparation phase
- Check the snapshots and make sure that the benchmark actually deleted them
- Check the GCS bucket you provided by `gcp-bucket` config key
