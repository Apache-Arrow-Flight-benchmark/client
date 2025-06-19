import argparse
from dotenv import load_dotenv
import os
import json
import requests
import pandas as pd
from pyarrow import flight
import pyarrow as pa
import time
from gist_util import GistUtil
import socket

load_dotenv()

#Parse arguments
parser = argparse.ArgumentParser(description="Python script for benchmarking Apache Arrow Flight")
parser.add_argument("-i", "--ip", type=str, default="127.0.0.1:443", help="Flight server host IP with port number")
parser.add_argument("-b", "--batch_size", type=int, default=100000, help="Batch size for data transfer")
parser.add_argument("-n", "--num_runs", type=int, default=1, help="Number of runs of one test")
parser.add_argument("-s", "--save_csv", action="store_true", default=False, help="Save results to CSV")
parser.add_argument("-f", "--filename", type=str, default="data", help="CSV file to check for size or save data")
parser.add_argument("-a", "--api_key", type=str, default=os.getenv('API_KEY'), help="API key for authentication")
parser.add_argument("-m", "--mock", action="store_true", default=False, help="Use mocked batch on server side.")
parser.add_argument("-r", "--rep", type=int, default=0, help="Number of repetitions for the mock batch.")
parser.add_argument("-p", "--postgres", type=str, default="", help="Use postgres table instead of mocked batch. Provide table name.")
parser.add_argument("-t", "--throughput", action="store_true", default=False, help="Test throughput of the server.")
parser.add_argument("-l", "--latency", action="store_true", default=False, help="Test latency of the server.")
parser.add_argument("-g", "--gist", action="store_true", default=False, help="Upload results to Gist.")
parser.add_argument("--label", type=str, default="", help="Label of the test for saving to Gist.")
parser.add_argument("--clients", type=int, default=1, help="Number of clients, used when saving to Gist.")
parser.add_argument("--postgres_url", type=str, default="jdbc:postgresql://localhost:5432/testdb", help="Postgres JDBC URL.")

args = parser.parse_args()

APIKEY = args.api_key 
JDBC_HOST = args.ip
BATCH_SIZE = args.batch_size
NUM_RUNS = args.num_runs
SAVE_CSV = args.save_csv
USE_MOCK = args.mock
BATCH_REP = args.rep
THROUGHPUT = args.throughput
LATENCY = args.latency
LABEL = args.label
CLIENTS = args.clients
HOSTNAME = socket.gethostname()
HOST_IP = socket.gethostbyname(HOSTNAME)
GIST = args.gist

if args.postgres == "":
    USE_DUMMY_JDBC = True
else:
    USE_DUMMY_JDBC = False
    POSTGRES_TABLE = args.postgres
    POSTGRES_URL = args.postgres_url

if USE_DUMMY_JDBC:
    DATASOURCE = "dummy"
    if USE_MOCK == True:
        DATASOURCE = "mock"
else:
    DATASOURCE = "postgres"

filename_base = os.path.splitext(args.filename)[0]
CSV_FILE = f"{filename_base}_{DATASOURCE}.csv"

if not (THROUGHPUT or LATENCY):
    THROUGHPUT = True

if USE_DUMMY_JDBC:
    JSON_SPEC = {
        "datasource_type_name": "custom_genericjdbc",
        "connection_properties": {
            "jdbc_url": "jdbc:dummyjdbc://localhost:3306/any"
        },
        "interaction_properties": {
            "schema_name": "mock",
            "table_name": "mock"
        },
        "batch_size": BATCH_SIZE,
        "batch_rep": BATCH_REP,
        "mock": USE_MOCK
    }
else:
    JSON_SPEC = {
        "datasource_type_name": "custom_genericjdbc",
        "connection_properties": {
            "jdbc_url": POSTGRES_URL,
            "username": "postgres",
            "password": "postgres"
        },
        "interaction_properties": {
            "schema_name": "public",
            "table_name": POSTGRES_TABLE
        },
        "batch_size": BATCH_SIZE,
        "mock": False
    }


results = []

class TokenClientAuthHandler(flight.ClientAuthHandler):
    def __init__(self):
        super().__init__()
        self.token = bytes('Bearer ' + os.getenv('ACCESS_TOKEN'), 'utf-8')

    def authenticate(self, outgoing, incoming):
        outgoing.write(self.token)
        self.token = incoming.read()

    def get_token(self):
        return self.token

def get_access_token(api_key):
    url = "https://iam.cloud.ibm.com/identity/token"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = f'grant_type=urn%3Aibm%3Aparams%3Aoauth%3Agrant-type%3Aapikey&apikey={api_key}'

    response = requests.post(url, headers=headers, data=data, verify=False)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise RuntimeError(f"Auth error: {response.status_code}, {response.text}")

def connect():
    access_token = get_access_token(APIKEY)
    os.environ['ACCESS_TOKEN'] = access_token
    location = f'grpc+tls://{JDBC_HOST}'
    
    client = flight.FlightClient(location, disable_server_verification=True)
    client.authenticate(TokenClientAuthHandler())
    return client

def get_endpoint(client, res={}):
    print("Fetching Flight Info...")
    start_time = time.perf_counter()
    descriptor = flight.FlightDescriptor.for_command(json.dumps(JSON_SPEC).encode('utf-8'))
    flight_info = client.get_flight_info(descriptor)
    res["flight_info_time"] = time.perf_counter() - start_time
    return flight_info.endpoints[0]

def benchmark_once(save = False):
    res = {}
    if THROUGHPUT:
        benchmark_throughput(res)
    if LATENCY:
        benchmark_latency(res)
    return res


def benchmark_throughput(res):
    ticket = get_endpoint(client, res).ticket
    print(f"Fetching data using ticket: {ticket}")
    stream = client.do_get(ticket)
    start_time = time.perf_counter()
    stream.read_all()
    res["fetch_time"] = time.perf_counter() - start_time
    res["throughput"] = DATA_SIZE / res["fetch_time"] if res["fetch_time"] > 0 else 0
    print(f"Fetch time: {res['fetch_time']} seconds")
    print(f"Throughput: {res['throughput']} B/s")

def benchmark_latency(res):
    ticket = get_endpoint(client, res).ticket
    print(f"Fetching data using ticket: {ticket}")
    stream = client.do_get(ticket)
    start_time = time.perf_counter()
    first_batch = next(stream)
    res["latency"] = time.perf_counter() - start_time
    for batch in stream:
        pass
    print(f"Latency: {res['latency']} seconds")

def benchmark():
    for i in range(NUM_RUNS):
        print(f"---------Iteration {i+1}/{NUM_RUNS}---------")
        res = benchmark_once()
        results.append(res)


    print("Benchmark results:")
    if THROUGHPUT:
        avg_time = sum([res["fetch_time"] for res in results]) / len(results)
        avg_throughput = sum([res["throughput"] for res in results]) / len(results)
        print(f"Average fetch time: {avg_time} seconds")
        print(f"Average throughput: {avg_throughput} B/s")
    if LATENCY:
        avg_latency = sum([res["latency"] for res in results]) / len(results)
        print(f"Average latency: {avg_latency} seconds")
    
    save_results(results)

def save_csv():
    ticket = get_endpoint(client).ticket
    print(f"Fetching data using ticket: {ticket}")
    stream = client.do_get(ticket)
    table = stream.read_all()
    arrow_data_size = table.get_total_buffer_size()
    print(f"Arrow data size: {arrow_data_size} bytes")
    print(f"Arrow data size: {arrow_data_size / (1024 * 1024):.2f} MiB")
    df = table.to_pandas()
    df.to_csv(CSV_FILE, index=False)
    print(f"Data saved to {CSV_FILE}")
    return arrow_data_size


def save_results(results):
    if GIST:
        upload_to_gist(results)
        return
    
    fetch_path = 'results/fetch_time'
    latency_path = 'results/latency'
    os.makedirs(fetch_path, exist_ok=True)
    os.makedirs(latency_path, exist_ok=True)

    if THROUGHPUT:
        with open(f'{fetch_path}/{BATCH_SIZE}_{DATA_SIZE}_{DATASOURCE}.txt', 'w') as f:
            for res in results:
                f.write(f"{res['fetch_time']}\n")

    if LATENCY:
        with open(f'{latency_path}/{BATCH_SIZE}_{DATA_SIZE}_{DATASOURCE}.txt', 'w') as f:
            for res in results:
                f.write(f"{res['latency']}\n")

def upload_to_gist(results):
    gist_util = GistUtil()
    descriptors = {"batch_size": BATCH_SIZE, "datasource": DATASOURCE, "data_size": DATA_SIZE, "arrow_data_size": ARROW_DATA_SIZE, "label": LABEL, "clients": str(CLIENTS), "host_name": HOSTNAME, "host_ip": HOST_IP}
    gist_util.upload_results(results, descriptors)
    
print("Sending request to the server with the following parameters:")
print(JSON_SPEC)

client = connect()

if not SAVE_CSV:
    if os.path.isfile(CSV_FILE):
        DATA_SIZE = os.path.getsize(CSV_FILE)
        print(f"CSV file size: {DATA_SIZE} bytes")
        print(f"CSV file size: {DATA_SIZE / (1024 * 1024):.2f} MiB")
    else:
        print("Datasize will not be checked")
        DATA_SIZE = 0
        ARROW_DATA_SIZE = 0
else:
    print("Creating new CSV file with data.")
    ARROW_DATA_SIZE = save_csv()
    DATA_SIZE = os.path.getsize(CSV_FILE)
    print(f"CSV file size: {DATA_SIZE} bytes")
    print(f"CSV file size:  {ARROW_DATA_SIZE / (1024 * 1024):.2f} MiB")

benchmark()









