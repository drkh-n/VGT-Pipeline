import os
from abc import ABC, abstractmethod
from typing import Tuple, Dict


class PipelineBase(ABC):
    """
    Definition of the data processing pipeline interface.

    This class documents the contract for pipeline implementations, including
    method purposes, inputs, and outputs.
    """

    CONFIG_DIR = "config"
    DATA_DIR = "data"

    def __init__(self, julia_notebook: str, python_notebook: str) -> None:
        """
        Initialize pipeline with notebook entry points.

        Inputs
        - julia_notebook: Path to the Julia notebook used for PSI computation.
        - python_notebook: Path to the Python notebook used for visualization.

        Output
        - None
        """
        self.julia_notebook = julia_notebook
        self.python_notebook = python_notebook

    @abstractmethod
    def run_julia_notebook(self, config_path: str, noisemap_path: str, output_path: str) -> None:
        """
        Execute the Julia notebook to compute PSI (or related products).

        Inputs
        - config_path: Absolute or relative path to a JSON config describing the target dataset.
        - noisemap_path: Path to JSON or SDF describing noise statistics for the cube.
        - output_path: Desired output path for the PSI product (e.g., SDF/FITS).

        Output
        - None (side effects: notebook execution generates artifacts on disk)
        """
        raise NotImplementedError

    @abstractmethod
    def run_starlink_collapse(self, fits_path: str, frequency_range: Tuple[float, float], output_path: str) -> None:
        """
        Run STARLINK/KAPPA collapse to compute moment 0 within a range.

        Inputs
        - fits_path: Path to the input spectral cube.
        - frequency_range: (vmin, vmax) limits for integration in native axis units.
        - output_path: Output path for the integrated map (e.g., FITS/SDF).

        Output
        - None (side effects: writes the integrated map to disk)
        """
        raise NotImplementedError

    @abstractmethod
    def run_python_visualization(self, config_path: str) -> None:
        """
        Execute a Python notebook to render diagnostic plots.

        Inputs
        - config_path: Path to the dataset configuration JSON.

        Output
        - None (side effects: generates visualization files, e.g., PNG)
        """
        raise NotImplementedError

    @abstractmethod
    def to_velocity(self, fits_path: str) -> None:
        """
        Convert a spectral cube WCS to velocity (vrad, LSRK) using STARLINK.

        Inputs
        - fits_path: Path to the input spectral cube (modified in place or via headers).

        Output
        - None (side effects: updates WCS in the input cube)
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def parse_stats_output(stats_output: str) -> Dict[str, float]:
        """
        Parse STARLINK 'stats' textual output into a dictionary of metrics.

        Inputs
        - stats_output: Raw text emitted by the 'stats' command.

        Output
        - Dictionary mapping metric names to numeric values.
        """
        raise NotImplementedError

    @abstractmethod
    def calculate_mean_rms(self, fits_path: str, output_path: str, vlow: float, vhigh: float) -> None:
        """
        Compute noise statistics in a velocity window and persist results as JSON.

        Inputs
        - fits_path: Path to the input spectral cube.
        - output_path: Directory where intermediate and JSON files are written.
        - vlow: Lower velocity bound.
        - vhigh: Upper velocity bound.

        Output
        - None (side effects: writes SDF intermediates and JSON summaries)
        """
        raise NotImplementedError

    @abstractmethod
    def run(self, config_glob: str) -> None:
        """
        Orchestrate the end-to-end pipeline for all configs matching a glob.

        Inputs
        - config_glob: Glob pattern for JSON configs inside CONFIG_DIR.

        Output
        - None (side effects: generates products and visualizations per config)
        """
        raise NotImplementedError


