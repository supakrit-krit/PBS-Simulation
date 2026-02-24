# models/simulation_engine.py

import heapq
import itertools


class SimulationEngine:
    """
    Discrete Event Simulation Engine for PBS-style scheduling.
    Supports FIFO and EASY Backfilling.
    """

    def __init__(self, jobs, system, scheduler):
        self.jobs = jobs
        self.system = system
        self.scheduler = scheduler

        self.time = 0
        self.event_queue = []

        # Prevent heap comparison error
        self._counter = itertools.count()

    # -------------------------------------------------
    # Initialization (used before step-based execution)
    # -------------------------------------------------
    def initialize(self):
        """
        Schedule all arrival events.
        """
        for job in self.jobs:
            heapq.heappush(
                self.event_queue,
                (job.arrival_time, next(self._counter), "arrival", job)
            )

    # -------------------------------------------------
    # Check if simulation is finished
    # -------------------------------------------------
    def is_finished(self):
        return len(self.event_queue) == 0

    # -------------------------------------------------
    # Process one event
    # -------------------------------------------------
    def step(self):
        """
        Process a single event from the event queue.
        """
        if not self.event_queue:
            return

        self.time, _, event_type, job = heapq.heappop(self.event_queue)

        if event_type == "arrival":
            self.scheduler.submit(job)
            self._try_schedule()

        elif event_type == "completion":
            self.system.release(job)
            self._try_schedule()

    # -------------------------------------------------
    # Run full simulation (non-step mode)
    # -------------------------------------------------
    def run(self):
        """
        Run full simulation without step control.
        """
        self.initialize()

        while not self.is_finished():
            self.step()

        return self.jobs

    # -------------------------------------------------
    # Try scheduling jobs after event
    # -------------------------------------------------
    def _try_schedule(self):
        """
        Attempt to schedule as many jobs as possible
        based on scheduler policy.
        """

        while True:

            # Gather currently running jobs
            running_jobs = [
                event[3]  # job object
                for event in self.event_queue
                if event[2] == "completion"
            ]

            # FIFO compatibility
            if hasattr(self.scheduler, "get_next_job"):
                job = self.scheduler.get_next_job(self.system)
                if job is None:
                    break

                self._start_job(job)

            # Backfill compatibility
            elif hasattr(self.scheduler, "schedule"):
                jobs_to_start = self.scheduler.schedule(
                    self.system,
                    self.time,
                    running_jobs
                )

                if not jobs_to_start:
                    break

                for job in jobs_to_start:
                    self._start_job(job)

            else:
                raise ValueError("Unsupported scheduler type.")

    # -------------------------------------------------
    # Start a job
    # -------------------------------------------------
    def _start_job(self, job):
        """
        Allocate resources and schedule completion event.
        """

        job.start_time = self.time
        job.end_time = self.time + job.walltime

        self.system.allocate(job)

        heapq.heappush(
            self.event_queue,
            (job.end_time, next(self._counter), "completion", job)
        )