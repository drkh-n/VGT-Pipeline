import os
import re
import json
import glob
import subprocess
from nbclient import NotebookClient
from nbformat import read as nb_read
from pipeline_base import PipelineBase


class Pipeline(PipelineBase):
    CONFIG_DIR = "configs"
    DATA_DIR = "data"

    def __init__(self, julia_notebook: str = "LMC_range1_psi.ipynb", python_notebook: str = "LMC_VGT_visual.ipynb") -> None:
        super().__init__(julia_notebook=julia_notebook, python_notebook=python_notebook)

    def run_julia_notebook(self, config_path: str, noisemap_path: str, output_path: str) -> None:
        print(f"📄 Running Julia notebook with config: {config_path}")
        with open(self.julia_notebook) as f:
            nb = nb_read(f, as_version=4)
        nb.cells[0].source = f'config_path = "{config_path}"\n' + nb.cells[0].source
        nb.cells[0].source = f'noise_path = "{noisemap_path}"\n' + nb.cells[0].source
        client = NotebookClient(nb, timeout=6000, kernel_name="julia-1.11")
        client.execute()

    def run_starlink_collapse(self, fits_path: str, frequency_range, output_path: str) -> None:
        if os.path.exists(output_path):
            print(f"✔️  Moment 0 already exists: {output_path}")
            return
        vmin, vmax = frequency_range
        print(f"🌀 Running STARLINK collapse from {vmin} to {vmax}")
        shell_command = f'''
        source $STARLINK_DIR/etc/profile
        convert
        kappa
        collapse {fits_path} {output_path} 3 low={vmin} high={vmax} estimator="Integ"
        '''
        subprocess.run(["bash", "-c", shell_command], check=True)

    def run_python_visualization(self, config_path: str) -> None:
        print("🎨 Running Python visualization")
        with open(self.python_notebook) as f:
            nb = nb_read(f, as_version=4)
        nb.cells[0].source = f'config_path = "{config_path}"\n' + nb.cells[0].source
        client = NotebookClient(nb, timeout=300, kernel_name="python3")
        client.execute()

    def to_velocity(self, fits_path: str) -> None:
        shell_command = f'''
        source $STARLINK_DIR/etc/profile
        convert
        kappa
        ndftrace {fits_path}
        wcsattrib ndf={fits_path} mode=set name=system newval=vrad
        wcsattrib ndf={fits_path} mode=set name=StdofRest newval=LSRK
        ndftrace {fits_path}
        '''
        subprocess.run(["bash", "-c", shell_command], check=True)

    @staticmethod
    def parse_stats_output(stats_output: str) -> dict:
        stats_dict = {}
        patterns = {
            "Pixel sum": r"Pixel sum\s+:\s+([-\d\.eE\+]+)",
            "Pixel mean": r"Pixel mean\s+:\s+([-\d\.eE\+]+)",
            "Standard deviation": r"Standard deviation\s+:\s+([-\d\.eE\+]+)",
            "Skewness": r"Skewness\s+:\s+([-\d\.eE\+]+)",
            "Kurtosis": r"Kurtosis\s+:\s+([-\d\.eE\+]+)",
            "Minimum pixel value": r"Minimum pixel value\s+:\s+([-\d\.eE\+]+)",
            "Maximum pixel value": r"Maximum pixel value\s+:\s+([-\d\.eE\+]+)",
            "Total number of pixels": r"Total number of pixels\s+:\s+(\d+)",
            "Number of pixels used": r"Number of pixels used\s+:\s+(\d+)",
            "No. of pixels excluded": r"No\. of pixels excluded\s+:\s+(\d+)"
        }
        for key, pattern in patterns.items():
            match = re.search(pattern, stats_output)
            if match:
                value = match.group(1).strip()
                if '.' in value or 'e' in value.lower():
                    stats_dict[key] = float(value)
                else:
                    stats_dict[key] = int(value)
        return stats_dict

    def calculate_mean_rms(self, fits_path: str, output_path: str, vlow, vhigh) -> None:
        shell_command = f'''
        source $STARLINK_DIR/etc/profile
        convert
        kappa
        collapse in={fits_path} axis=vrad low={vlow} high={vhigh} estimator=sigma out={output_path}/noisemap.sdf
        stats {output_path}/noisemap.sdf
        sqorst in={fits_path} axis=3 mode=pixelscale pixscale=1.0 out={output_path}/1kms.sdf method=auto
        collapse in={output_path}/1kms.sdf axis=vrad low={vlow} high={vhigh} estimator=sigma out={output_path}/1kms_noisemap.sdf
        stats {output_path}/1kms.sdf comp=ERROR
        stats {output_path}/1kms_noisemap.sdf
        '''
        result = subprocess.run(
            ["bash", "-c", shell_command],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        full_output = result.stdout
        print(full_output)
        stats_blocks = full_output.split("Pixel statistics for the NDF structure")
        if len(stats_blocks) < 4:
            raise ValueError("Unexpected output format: not enough stats blocks found.")
        stats_outputs = stats_blocks[1:4]
        stats_files = [
            os.path.join(output_path, "noisemap_stats.json"),
            os.path.join(output_path, "1kms_error_stats.json"),
            os.path.join(output_path, "1kms_noisemap_stats.json")
        ]
        for stats_text, json_file in zip(stats_outputs, stats_files):
            stats_dict = self.parse_stats_output(stats_text)
            with open(json_file, 'w') as f:
                json.dump(stats_dict, f, indent=2)
            print(f"Saved stats to {json_file}")

    def run(self, config_glob: str = "M09BC12_CO.json") -> None:
        configs = glob.glob(os.path.join(self.CONFIG_DIR, config_glob))
        for config_path in configs:
            with open(config_path) as f:
                cfg = json.load(f)
            print(f"=====🤖 Running pipeline for {cfg['programID']}=====")
            programID = cfg["programID"]
            data_folder = os.path.join(self.DATA_DIR, programID)
            fits_file_pattern = cfg["fits_file"]
            velocity_range = cfg["velocity_range"]
            noise_range = cfg["noise_range"]
            input_fits = glob.glob(os.path.join(data_folder, fits_file_pattern))[0]
            mom0_path = os.path.join(data_folder, "mom0.fits")
            psi_path = os.path.join(data_folder, "psi.fits")
            noisemap_path = os.path.join(data_folder, "noisemap_stats.json")
            # self.to_velocity(input_fits)
            # self.calculate_mean_rms(input_fits, data_folder, noise_range[0], noise_range[1])
            self.run_julia_notebook(config_path, noisemap_path, psi_path)
            self.run_starlink_collapse(input_fits, velocity_range, mom0_path)
            self.run_python_visualization(config_path)
            print(f"===== finished {cfg['programID']}=====")
        print("Pipeline has finished execution")


