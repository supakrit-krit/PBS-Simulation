# models/job.py
from dataclasses import dataclass

@dataclass
class Job:
    job_id: str
    arrival_time: int
    ncpus: int
    ngpus: int
    walltime: int  # execution duration (seconds)

    start_time: int = None
    end_time: int = None

    @property
    def waiting_time(self):
        return None if self.start_time is None else self.start_time - self.arrival_time

    @property
    def turnaround_time(self):
        return None if self.end_time is None else self.end_time - self.arrival_time