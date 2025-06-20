# Apache Arrow Flight benchmarking client

This application can be used to benchmark Apache Arrow Flight servers. It was tailored for evaluating the server created using [IBM Cloud Pak for Data - Connector SDK fork](https://github.com/mpietr/cp4d-connector-sdk). 

Measurements and metrics:

 * data fetch time from the server,
 * fetch time of only the first batch of data,
 * received data size,
 * server throughput.

The client assumes that the server supports the following data sources: mocked batch, DummyJDBC and PostgreSQL.

## Options

For supported functionalities refer to the command line options:

|Option|Description|
|-|-|
|`-i`, `--ip`|IP address Flight service. Must include port number The default is `127.0.0.1:443`.|
| `-b`, `--batch_size` | Specifies the `batch_size` option in the request JSON that controls the number of rows in each batch returned by Flight. The default values is 100000. |
| `-n`, `--num_runs` | Controls the number of repetitions of the test, meaning the number of repeated requests to the server for which the measurements are taken. The default is 1. |
| `-s`, `--save_csv` | Specifies the data should be fetch once before testing, for the purpose of measuring its size. The size is measured in two ways: the size of the CSV file after saving and Arrow buffer size of the received data. Please note that if the option is not specified, the script will assume that the data size is 0, and as a results the logged throughput will be 0 B/s. |
| `-f`, `--filename` | The filename of the data saved when using the `-s` option. |
| `-a`, `--api_key` | The API KEY used for authentication when connecting to the Flight service. The default value can be specified in an `.env` file as `API_KEY`. |
| `-m`, `--mock` | Specifies whether the mocked batch should be used for sending data by the server. Requires the `-r` option.|
| `-r`, `--rep` | Specifies how many times the mocked batch should be repeatedly sent by the server. Works only when the `-m` option is set. |
| `-t`, `--throughput` | Specifies whether the client should test for throughput - measuring the fetch size of the data. If neither this nor `-l` option is set, the client tests for throughput by default. |
| `-l`, `--latency` | Specifies whether the client should test for latency - measuring the fetch time of the first batch. If neither this nor `-t` option is set, the client tests for throughput by default. |
| `-p`, `--postgres` | If specified, the PostgreSQL datasource is used. The name of the table from which to fetch the data should be provided here. Must be used together with `--postgres_url` option. |
| `--postgres_url` | Specifies the JDBC url for connecting to PostgreSQL. The url should contain the name of the database. By default the credentials for both username and password are `postgres`. |
| `-g`, `--gist` | Specifies whether the results should be saved to Github Gist. Requires Github token with permission to create Gists, which should be added in the `.env` file as `GITHUB_TOKEN`. The measurements are saved to Gist one file per `-l` or `-t` option. The file contains either fetch time or first batch fetch time. The descritpion of the gist contains: `timestamp` - date and time of the test, `batch_size` - specified in the `-b` option, `datasource` - what datasource was used for testing (mocked batch, DummyJDBC or PostgreSQL), `data_size` - the size measured as CSV file, `arrow_data_size` - the size measured as Arrow buffer size, `label` - contents provided when using `--label` option, `clients` - contents provided when using the `--clients` option, `host_name` - name of the host executing the benchmark, `host_ip` - IP of the host executing the benchmark. If not specified, the results will be saved to txt file: `<batch_size>_<data_size>_<data_source>.txt`.|
| `--label` | Can be used for setting a custom label to be logged when saving results to Github Gist. |
| `--clients` | Can be used to specify the number for clients to be logged when saving results to Github Gist. |

## Example

Run 10 iterations of tests for the `DummyJDBC` data source with batch size 10000:

`python benchmark.py -n 10 -b 10000 -a api_key`
