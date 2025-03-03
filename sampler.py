from TraStrainer import tra_strainer, MetricProcessor, TraceProcessor
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--path', type=str, help='set up data dir path')
parser.add_argument('--rate', type=str, help='set up sample rate')
args = parser.parse_args()

# read data
data_file = args.path # demo: './data/test/'
metrics = MetricProcessor.process_metrics(data_file + '/metrics.csv')
traces = TraceProcessor.read_traces(data_file + '/traces.csv')

# run TraStrainer
sampling_rate = float(args.rate) # demo 0.1
tra_strainer_result = tra_strainer(traces, metrics, sampling_rate)
print(f"sampling_rate:{sampling_rate}, sampling trace_ids:{tra_strainer_result}")
