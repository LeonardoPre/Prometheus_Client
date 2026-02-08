import pandas as pd
import numpy as np
from pathlib import Path
from dataclasses import dataclass
from scipy.stats import zscore

BASE_DIR = "data"

@dataclass
class Iteration:
    experiment_name: str
    workload: str = "shaped"
    variant: str = "baseline"
    iteration_index: int = 0

    def get_path(self, base_dir: Path) -> Path:
        """Generates the full path for this specific iteration."""
        return base_dir / self.experiment_name / self.workload / self.variant / str(self.iteration_index)

class ExperimentLoader:
    def __init__(self):
        self.base_dir = BASE_DIR
        self.data = Path(self.base_dir)
        self.experiments = [x for x in self.data.iterdir() if x.is_dir()]
    
    @staticmethod
    def get_experiment_length(df):
        times = df["observation_time"].unique()
        data = np.array(times)
        times = data.astype('datetime64[s]')

        time_span = times.max() - times.min()
        # Convert to integer (seconds)
        span_int = time_span.astype(int)

        return span_int

    def get_experiment_names(self):
        return [str(exp_dir.stem) for exp_dir in self.experiments]

    def get_iteration(self, experiment_name: str, workload: str ="shaped", variant: str="baseline", iteration_index: int ="0"):
        return Iteration(experiment_name, workload, variant, iteration_index)
    
    @staticmethod
    def _transform_observation_time(df):
        # Convert to numpy datetime64 in seconds
        times = pd.to_datetime(df["observation_time"]).values.astype('datetime64[s]')
        
        # Calculate offset from the start
        min_time = times.min()
        relative_seconds = (times - min_time).astype(int) 
        
        df["relative_seconds"] = relative_seconds
        return df
    
    def load_pod_measurements(self, iteration: Iteration, clean: bool = True, drop_outliers: bool = False) -> pd.DataFrame:
    
        folder_path = iteration.get_path(self.data)
        measurement_csvs = list(folder_path.glob('**/measurements_pod*.csv'))
        
        if len(measurement_csvs) > 1:
            raise ValueError(f"Ambiguous data: Found {len(measurement_csvs)} pod measurements in {folder_path}")
        elif len(measurement_csvs) < 1:
            raise FileNotFoundError(f"No pod measurements found in {folder_path}")
        
        target_file = measurement_csvs[0]
        df = pd.read_csv(target_file)
        if clean:
            df = df[df["name"] != "loadgenerator" ]
        if drop_outliers:
            df = self._drop_outliers(df)
        
        df = self._transform_observation_time(df)

        return df
    
    def load_node_measurements(self, iteration: Iteration, drop_outliers: bool = False) -> pd.DataFrame:
    
        folder_path = iteration.get_path(self.data)
        measurement_csvs = list(folder_path.glob('**/measurements_node*.csv'))
        
        if len(measurement_csvs) > 1:
            raise ValueError(f"Ambiguous data: Found {len(measurement_csvs)} node measurements in {folder_path}")
        elif len(measurement_csvs) < 1:
            raise FileNotFoundError(f"No node measurements found in {folder_path}")
        
        target_file = measurement_csvs[0]
        df = pd.read_csv(target_file)
        if drop_outliers:
            df = self._drop_outliers(df)
        
        df = self._transform_observation_time(df)

        return df
    
    
    def _drop_outliers(self, df, z_score_threshold=3):
        data_errors = 0
        data_points = len(df)

        common_keys = [
            "wattage_kepler",
            "wattage_scaph",
            "wattage_kepler_new",
            "cpu_usage",
            "memory_usage",
            "network_usage",
        ]

        for key in common_keys:
            if key in df:
                df[f"{key}_zscore"] = zscore(df[key])

        for key in common_keys:
            outliers = df[df[f"{key}_zscore"].abs() > z_score_threshold].index
            data_errors += len(outliers)
            df = df.drop(outliers)

        if data_errors:
             print(
                 f"dropped {data_errors} outliers ({100*data_errors/data_points:.0f}%)"
             )

        return df

    
    
    
    