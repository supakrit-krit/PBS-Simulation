# models/workload.py
import pandas as pd
from models.job import Job

class Workload:

    @staticmethod
    def from_csv(path):
        df = pd.read_csv(path)

        jobs = []

        for _, row in df.iterrows():
            job = Job(
                job_id=row["job_id"],
                arrival_time=int(row["qtime"]),
                ncpus=int(row["Resource_List.ncpus"]),
                ngpus=int(row.get("Resource_List.ngpus", 0)),
                walltime=Workload._parse_walltime(row["resources_used.walltime"])
            )
            jobs.append(job)

        jobs.sort(key=lambda j: j.arrival_time)
        return jobs

    @staticmethod
    def _parse_walltime(walltime_str):
        # format: HH:MM:SS
        h, m, s = map(int, walltime_str.split(":"))
        return h*3600 + m*60 + s