# models/scheduler.py


class FIFOScheduler:
    """
    Simple FIFO scheduler.
    """

    def __init__(self):
        self.queue = []

    def submit(self, job):
        self.queue.append(job)

    def get_next_job(self, system):
        for job in self.queue:
            if system.can_allocate(job):
                self.queue.remove(job)
                return job
        return None


# ----------------------------------------------------------
# EASY BACKFILL SCHEDULER
# ----------------------------------------------------------

class EasyBackfillScheduler:
    """
    Implements EASY Backfilling.

    Rules:
    1. Preserve FIFO order for head job.
    2. Reserve earliest start time for head job.
    3. Allow smaller jobs to backfill if they finish
       before head reservation time.
    """

    def __init__(self):
        self.queue = []

    def submit(self, job):
        self.queue.append(job)

    # -------------------------------------------------
    # Main scheduling entry
    # -------------------------------------------------
    def schedule(self, system, current_time, running_jobs):
        """
        Return list of jobs that can start at current_time.
        """

        started_jobs = []

        if not self.queue:
            return started_jobs

        # -------------------------------------------------
        # 1️⃣ Try head job first (strict FIFO)
        # -------------------------------------------------
        head_job = self.queue[0]

        if system.can_allocate(head_job):
            self.queue.pop(0)
            started_jobs.append(head_job)
            return started_jobs

        # -------------------------------------------------
        # 2️⃣ Compute reservation time for head job
        # -------------------------------------------------
        reservation_time = self._compute_reservation_time(
            head_job,
            system,
            running_jobs,
            current_time
        )

        # -------------------------------------------------
        # 3️⃣ Try backfilling other jobs
        # -------------------------------------------------
        for job in list(self.queue[1:]):

            if not system.can_allocate(job):
                continue

            finish_time = current_time + job.walltime

            # Must not delay head job
            if finish_time <= reservation_time:
                self.queue.remove(job)
                started_jobs.append(job)

        return started_jobs

    # -------------------------------------------------
    # Compute head reservation time
    # -------------------------------------------------
    def _compute_reservation_time(self, head_job, system, running_jobs, current_time):
        """
        Compute earliest time the head job can start.
        """

        # Sort running jobs by end_time ONLY
        future_jobs = sorted(
            running_jobs,
            key=lambda j: j.end_time
        )

        temp_available_cpu = system.available_ncpus
        temp_available_gpu = system.available_ngpus

        # Check if already possible
        if (head_job.ncpus <= temp_available_cpu and
            head_job.ngpus <= temp_available_gpu):
            return current_time

        # Simulate future releases
        for job in future_jobs:
            temp_available_cpu += job.ncpus
            temp_available_gpu += job.ngpus

            if (head_job.ncpus <= temp_available_cpu and
                head_job.ngpus <= temp_available_gpu):
                return job.end_time

        return float("inf")