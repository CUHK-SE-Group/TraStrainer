import os
import csv
import copy
import time
import random
import numpy as np
import pandas as pd
from treelib import Tree
from collections import deque
from typing import Dict, List, Tuple, Set, Any, Optional, Union, Deque
from datetime import datetime, timedelta
from LTSF_Linear.run_longExp import Argument, metric_infer


class MetricProcessor:
    """Process and manage system metrics data from CSV files."""
    
    @staticmethod
    def process_metrics(path: str) -> Dict[Tuple[str, str], List[Dict[str, Union[str, float]]]]:
        """
        Extract and process metrics from CSV files in the specified directory.
        
        Args:
            path: Path to the directory containing metric data
            
        Returns:
            Dictionary mapping (service_name, metric_name) to list of timestamp-value pairs
        """
        data_dict = {}
        folder_path = os.path.join(path, 'metric')
        
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                with open(file_path, 'r') as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        # Extract pod/service name and timestamp
                        pod_name = row.get('PodName') if row.get('PodName') else row.get('ServiceName')
                        time_str = row.get('Time')[:19] if row.get('Time') else row.get('time')[:19]
                        
                        # Process each metric in the row
                        for metric, value in row.items():
                            # Skip certain metrics and non-metric fields
                            if ('Byte' in metric or 'P95' in metric or 'P99' in metric or 'Syscall' in metric or
                                metric in ['PodName', 'ServiceName', 'Time', 'time', 'TimeStamp', 'timestamp']):
                                continue
                            
                            # Create key and store data
                            key = (pod_name.split('-')[0], metric)
                            if key not in data_dict:
                                data_dict[key] = []
                            
                            # Handle empty values
                            value = 0.0 if not value else float(value)
                            data_dict[key].append({'date': time_str, 'value': value})
        
        return data_dict


class TimeUtils:
    """Utility class for timestamp and datetime conversions."""
    
    @staticmethod
    def timestamp_to_datetime(timestamp: str) -> str:
        """Convert Unix timestamp to formatted datetime string."""
        dt_object = datetime.fromtimestamp(int(timestamp))
        return dt_object.strftime('%Y-%m-%d %H:%M:%S')
    
    @staticmethod
    def future_datetime(date_string: str, minutes: int) -> str:
        """Calculate a future datetime by adding minutes to a datetime string."""
        date_obj = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        new_date_obj = date_obj + timedelta(minutes=minutes)
        return new_date_obj.strftime('%Y-%m-%d %H:%M:%S')


class TraceProcessor:
    """Process and analyze distributed trace data."""
    
    @staticmethod
    def read_traces(path: str) -> Dict[str, List[Dict]]:
        """
        Read trace data from CSV files in the specified directory.
        
        Args:
            path: Path to directory containing trace data
            
        Returns:
            Dictionary mapping trace IDs to lists of span data
        """
        traces = {}
        folder_path = os.path.join(path, 'trace')
        
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                try:
                    with open(file_path, 'r') as csv_file:
                        csv_reader = csv.DictReader(csv_file)
                        for row in csv_reader:
                            row['status'] = 'success'
                            row['StartTime'] = TimeUtils.timestamp_to_datetime(row['StartTimeUnixNano'][:10])
                            row['EndTime'] = TimeUtils.timestamp_to_datetime(row['EndTimeUnixNano'][:10])
                            
                            if row['TraceID'] in traces:
                                traces[row['TraceID']].append(row)
                            else:
                                traces[row['TraceID']] = [row]
                except FileNotFoundError:
                    pass
        
        return traces
    
    @staticmethod
    def process_trace(spans: List[Dict]) -> Tuple[Dict, Dict, Tree]:
        """
        Process trace spans to extract various metrics and build a trace tree.
        
        Args:
            spans: List of span dictionaries from a trace
            
        Returns:
            Tuple of (service metrics, resource metrics, trace tree)
        """
        resources = ['sql']
        data_dict = {}  # Service-level metrics
        resource_dict = {}  # Resource-specific metrics
        
        # Process each span
        for row in spans:
            status = row['status']
            pod_name = row['PodName'].split('-')[0]
            operation_name = row['OperationName']
            span_id = row['SpanID']
            duration = int(row['Duration'])
            
            # Collect service metrics
            if pod_name not in data_dict:
                data_dict[pod_name] = []
            data_dict[pod_name].append({
                'span_id': span_id,
                'duration': duration,
                'status': status
            })
            
            # Collect resource-specific metrics
            for resource in resources:
                if resource in operation_name:
                    key = (pod_name, resource)
                    if key not in resource_dict:
                        resource_dict[key] = []
                    resource_dict[key].append({
                        'span_id': span_id, 
                        'duration': duration
                    })
        
        # Build the trace tree
        tree = TraceProcessor.build_trace_tree(spans)
        return data_dict, resource_dict, tree
    
    @staticmethod
    def build_trace_tree(spans: List[Dict]) -> Tree:
        """
        Build a tree representation of spans in a trace.
        
        Args:
            spans: List of span dictionaries from a trace
            
        Returns:
            Tree representation of trace spans
        """
        # Sort spans by start time
        spans = sorted(spans, key=lambda x: x['StartTimeUnixNano'])
        
        # Create parent-child relationships
        parent_child = {}
        node_info = {}
        for i, span in enumerate(spans):
            if span['ParentID'] in parent_child:
                parent_child[span['ParentID']].append(i)
            else:
                parent_child[span['ParentID']] = [i]
            node_info[span['SpanID']] = i
        
        # Create the tree
        tree = Tree()
        if spans:
            root_span_id = spans[0]['SpanID']
            tree.create_node(tag=root_span_id, identifier=root_span_id, data=spans[0])
            TraceProcessor._build_tree(root_span_id, parent_child, tree, spans)
        
        return tree
    
    @staticmethod
    def _build_tree(node: str, parent_child: Dict, tree: Tree, spans: List[Dict]) -> None:
        """
        Recursively build the tree structure by adding children to their parents.
        
        Args:
            node: Current node identifier
            parent_child: Dictionary mapping parent span IDs to list of child indices
            tree: Tree being constructed
            spans: List of span dictionaries
        """
        if node not in parent_child:
            return
            
        for event_id, child in enumerate(parent_child[node]):
            child_span_id = spans[child]['SpanID']
            if tree.contains(child_span_id):
                continue
                
            tree.create_node(
                tag=child_span_id,
                identifier=child_span_id,
                parent=node,
                data=spans[child]
            )
            
            TraceProcessor._build_tree(child_span_id, parent_child, tree, spans)
    
    @staticmethod
    def get_seq_span(spans: List[Dict], tree: Tree) -> List[str]:
        """
        Create a sequence representation of spans in a trace.
        
        Args:
            spans: List of span dictionaries
            tree: Trace tree representation
            
        Returns:
            List of span feature strings
        """
        basic_features = []
        
        for span in spans:
            # Create a feature string for each span
            basic_features.append('-'.join([
                str(tree.depth(span['SpanID'])),
                span['PodName'],
                span['OperationName'],
                span['status'],
                str(int(int(span['Duration']) / 1e4))
            ]))
            
        basic_features.sort()
        return basic_features
    
    @staticmethod
    def compute_feature_values(data_dict: Dict, metrics: Dict) -> Dict:
        """
        Compute feature values based on trace data and metrics.
        
        Args:
            data_dict: Dictionary containing service metrics
            metrics: Dictionary of metrics to compute features for
            
        Returns:
            Dictionary of computed feature values
        """
        feature_values = {}
        
        for key in metrics:
            pod = key[0]
            
            # Handle case when pod is not in data_dict
            if pod not in data_dict:
                feature_values[key] = 0
                continue
                
            # Calculate metrics
            span_data = data_dict[pod]
            num_spans = len(span_data)
            
            if num_spans:
                avg_duration = sum(entry['duration'] for entry in span_data) / num_spans
                num_failures = sum(1 for entry in span_data if entry['status'] == 'fail')
                feature_value = num_spans * avg_duration * (1 + num_failures)
            else:
                feature_value = 0
                
            feature_values[key] = feature_value
            
        return feature_values


class SimilarityCalculator:
    """Calculate similarity between trace structures."""
    
    @staticmethod
    def compute_jaccord_similarity(spanline_1: List[str], spanline_2: List[str]) -> float:
        """
        Compute Jaccard similarity between two span sequences.
        
        Args:
            spanline_1: First span sequence
            spanline_2: Second span sequence
            
        Returns:
            Jaccard similarity score [0-1]
        """
        cp_spanline_1 = copy.deepcopy(spanline_1)
        cp_spanline_2 = copy.deepcopy(spanline_2)
        
        # Count intersection elements
        intersection_cnt = 0
        for sl1 in spanline_1:
            if sl1 in cp_spanline_2:
                intersection_cnt += 1
                cp_spanline_2.pop(cp_spanline_2.index(sl1))
                cp_spanline_1.pop(cp_spanline_1.index(sl1))
        
        # Calculate union size and similarity
        union_cnt = intersection_cnt + len(cp_spanline_1) + len(cp_spanline_2)
        span_seq_similarity = intersection_cnt / union_cnt if union_cnt else 0
        
        return span_seq_similarity


class MetricPredictor:
    """Predict and evaluate metrics using ML models."""
    
    @staticmethod
    def compute_metrics_weights(
        metrics: Dict[Tuple[str, str], List[Dict]], 
        start_time: str, 
        end_time: str
    ) -> Dict[Tuple[str, str], float]:
        """
        Compute weights for metrics based on prediction accuracy.
        
        Args:
            metrics: Dictionary of metric data
            start_time: Start time for prediction window
            end_time: End time for prediction window
            
        Returns:
            Dictionary mapping metric keys to weights
        """
        metrics_weights = {}
        
        for key, value in metrics.items():
            # Prepare input data
            input_df = pd.DataFrame(value)
            
            # Find prediction start index and length
            predict_idx = (input_df['date'] >= start_time).idxmax()
            predict_end = TimeUtils.future_datetime(end_time, 1)
            predict_len = len(input_df[(input_df['date'] >= start_time) & (input_df['date'] < predict_end)])
            predict_len = max(predict_len, 1)
            
            # Create model ID from key
            model_id = "_".join(key)
            
            # Use default weight if model not found
            if not os.path.exists(f'./checkpoints/{model_id}.pth'):
                metrics_weights[key] = 1
                continue
                
            # Run inference and calculate weight based on prediction error
            args = Argument(input_df[predict_idx - 96:predict_idx + predict_len], model_id=model_id)
            preds, trues = metric_infer(args, model=None)
            metrics_weight = abs(float(((trues - preds) / preds).mean()))
            metrics_weights[key] = metrics_weight
            
        return metrics_weights


class TraStrainer:
    """Main class for trace-based sampling."""
    
    def __init__(self):
        """Initialize the TraStrainer."""
        pass
        
    @staticmethod
    def tanh(x: float) -> float:
        """Calculate hyperbolic tangent function."""
        return 2 / (1 + np.exp(-2 * x)) - 1
        
    @staticmethod
    def system_biased_filter(
        history_trace_metrics: Dict[Tuple[str, str], Deque], 
        trace_metric: Dict[Tuple[str, str], float], 
        metrics_weights: Dict[Tuple[str, str], float]
    ) -> float:
        """
        Calculate system bias sampling rate based on metric anomalies.
        
        Args:
            history_trace_metrics: Historical metric values
            trace_metric: Current trace metrics
            metrics_weights: Weights for different metrics
            
        Returns:
            Sampling rate based on system metrics
        """
        sampling_rate = 0
        count = 0
        n_sigma_bag = {}
        
        for key, weight in metrics_weights.items():
            value = trace_metric[key]
            
            # Calculate mean and std deviation
            if not history_trace_metrics or not history_trace_metrics[key]:
                mean, std = 0, 0
            else:
                values = list(history_trace_metrics[key])
                mean, std = np.mean(values), np.std(values)
            
            # Calculate normalized deviation (n_sigma)
            n_sigma_bag[key] = abs(value - mean) / (std + 1e-5)
            sampling_rate += weight * n_sigma_bag[key]
            count += weight
        
        # Normalize and transform sampling rate
        sampling_rate = TraStrainer.tanh(sampling_rate / count if count else 0)
        return sampling_rate
        
    @staticmethod
    def diversity_biased_filter(
        history_trace_structures: Deque[List[str]], 
        trace_structure: List[str], 
        diversity_window: Deque[float]
    ) -> float:
        """
        Calculate diversity bias sampling rate based on trace structure similarity.
        
        Args:
            history_trace_structures: Collection of historical trace structures
            trace_structure: Current trace structure
            diversity_window: Window of diversity values for normalization
            
        Returns:
            Sampling rate based on trace diversity
        """
        # Count occurrences of each trace structure
        clustered_history_traces = {}
        for structure in history_trace_structures:
            element = '+'.join(structure)
            clustered_history_traces[element] = clustered_history_traces.get(element, 0) + 1
        
        # Find most similar historical trace
        similarity = 0
        max_trace_structure = ''
        
        for history_structure in history_trace_structures:
            cur_similarity = SimilarityCalculator.compute_jaccord_similarity(
                history_structure, trace_structure)
            
            if cur_similarity > similarity:
                similarity = cur_similarity
                max_trace_structure = '+'.join(history_structure)
        
        # Calculate similarity-based rate
        count = clustered_history_traces.get(max_trace_structure, 0)
        similarity_rate = round(similarity * count, 2)
        
        if similarity_rate:
            similarity_rate = 1 / similarity_rate
            diversity_window.append(similarity_rate)
            return similarity_rate / sum(diversity_window)
        else:
            return 1.0
            
    @staticmethod
    def judge(system_rate: float, diversity_rate: float, strict: bool) -> Tuple[bool, float, float]:
        """
        Decide whether to sample a trace based on system and diversity rates.
        
        Args:
            system_rate: Sampling rate based on system metrics
            diversity_rate: Sampling rate based on trace diversity
            strict: Whether to use AND logic (True) or OR logic (False)
            
        Returns:
            Tuple of (sample decision, system random value, diversity random value)
        """
        system_random = random.random()
        diversity_random = random.random()
        
        is_system_sample = system_random <= system_rate
        is_diversity_sample = diversity_random <= diversity_rate
        
        if strict:
            return is_system_sample and is_diversity_sample, system_random, diversity_random
        else:
            return is_system_sample or is_diversity_sample, system_random, diversity_random
    
    @staticmethod
    def init_history_trace_metrics(
        history_trace_metrics: Dict[Tuple[str, str], Deque], 
        metrics: Dict, 
        window_size: int
    ) -> None:
        """
        Initialize the history trace metrics storage.
        
        Args:
            history_trace_metrics: Dictionary to initialize
            metrics: Metrics dictionary defining keys to initialize
            window_size: Maximum size of the deque for each metric
        """
        for key in metrics:
            history_trace_metrics[key] = deque(maxlen=window_size)
    
    @staticmethod
    def print_num(num: float) -> str:
        """Format a number to two decimal places."""
        return f"{round(num, 2):.2f}"
    
    def run(self, traces: Dict[str, List[Dict]], metrics: Dict, budget_sample_rate: float) -> List[str]:
        """
        Run the TraStrainer algorithm to sample traces.
        
        Args:
            traces: Dictionary of traces
            metrics: Dictionary of metrics
            budget_sample_rate: Target sampling rate
            
        Returns:
            List of sampled trace IDs
        """
        # Initialize parameters
        window_size = int(1 / budget_sample_rate)
        warm_up_size = 10
        trace_vectors = []
        history_trace_metrics = {}
        self.init_history_trace_metrics(history_trace_metrics, metrics, window_size)
        history_trace_structures = deque(maxlen=window_size)
        diversity_window = deque(maxlen=window_size)
        
        # Tracking variables
        cnt = 0
        cur_sample_cnt = 0
        sample_trace_ids = []
        time_used = []
        
        # Process each trace
        for trace_id, trace in traces.items():
            start_time = time.time()
            
            # Process trace data
            data_dict, resource_dict, tree = TraceProcessor.process_trace(trace)
            trace_structure = TraceProcessor.get_seq_span(trace, tree)
            trace_metric = TraceProcessor.compute_feature_values(data_dict, metrics)
            metrics_weights = MetricPredictor.compute_metrics_weights(
                metrics, trace[0]['StartTime'], trace[0]['EndTime'])
            
            # Make sampling decision after warm-up
            if cnt >= warm_up_size:
                system_rate = self.system_biased_filter(
                    history_trace_metrics, trace_metric, metrics_weights)
                diversity_rate = self.diversity_biased_filter(
                    list(history_trace_structures), trace_structure, diversity_window)
                
                # Determine if strict sampling is needed based on current sample rate
                strict = True if (cur_sample_cnt / cnt > budget_sample_rate) else False
                is_sample, system_random, diversity_random = self.judge(
                    system_rate, diversity_rate, strict)
                
                # Add to sample if selected
                if is_sample:
                    cur_sample_cnt += 1
                    sample_trace_ids.append(trace_id)
                    
                # Log sampling decision
                print(
                    f"TraceID:{trace_id}\t "
                    f"SystemRate:{self.print_num(system_rate)}/{self.print_num(system_random)}\t "
                    f"DiversityRate:{self.print_num(diversity_rate)}/{self.print_num(diversity_random)}\t "
                    f"IsAnd:{1 if strict else 0}\t "
                    f"Sample:{is_sample}\t "
                    f"CurSampleRate:{self.print_num(cur_sample_cnt / cnt)}"
                )
            
            # Update history data
            history_trace_structures.append(trace_structure)
            trace_vectors.append(self._output_dict(trace_metric))
            
            for key in metrics:
                history_trace_metrics[key].append(trace_metric[key])
                
            # Update counters and timing
            cnt += 1
            end_time = time.time()
            time_used.append(end_time - start_time)
            
            # Break if we've processed enough traces
            if cnt >= int(round(budget_sample_rate * len(traces))):
                break
                
        return sample_trace_ids
    
    @staticmethod
    def _output_dict(d: Dict) -> Dict[str, str]:
        """Convert a dictionary to string key-value pairs."""
        return {str(k): str(v) for k, v in d.items()}
    
    @staticmethod
    def _output_metrics(metrics: Dict) -> Dict[str, int]:
        """Convert metrics dictionary to string keys with value counts."""
        return {str(k): len(v) for k, v in metrics.items()}


def tra_strainer(traces: Dict[str, List[Dict]], metrics: Dict, budget_sample_rate: float) -> List[str]:
    """
    Legacy function to maintain compatibility with existing code.
    
    Args:
        traces: Dictionary of traces
        metrics: Dictionary of metrics
        budget_sample_rate: Target sampling rate
        
    Returns:
        List of sampled trace IDs
    """
    trainer = TraStrainer()
    return trainer.run(traces, metrics, budget_sample_rate)