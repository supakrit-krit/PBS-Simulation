# models/system_spec.py
class SystemSpec:
    def __init__(self, total_ncpus, total_ngpus):
        self.total_ncpus = total_ncpus
        self.total_ngpus = total_ngpus

        self.available_ncpus = total_ncpus
        self.available_ngpus = total_ngpus

    def can_allocate(self, job):
        return (
            job.ncpus <= self.available_ncpus and
            job.ngpus <= self.available_ngpus
        )

    def allocate(self, job):
        self.available_ncpus -= job.ncpus
        self.available_ngpus -= job.ngpus

    def release(self, job):
        self.available_ncpus += job.ncpus
        self.available_ngpus += job.ngpus