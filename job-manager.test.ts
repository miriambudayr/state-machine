import { JobManager, JobStates, Priority } from ".";
import { log } from "console";

describe("JobManager", () => {
  describe(".createJob", () => {
    it("should create a job", () => {
      const manager = new JobManager();
      const job = manager.createJob("test-job", () => {}, Priority.high);
      expect(job.name).toBe("test-job");
    });
  });

  describe(".runJobs", () => {
    it("should run all jobs", async () => {
      const manager = new JobManager();
      let job1Ran = false;
      let job2Ran = false;
      const job1 = manager.createJob(
        "test-job-1",
        () => {
          job1Ran = true;
        },
        Priority.high
      );
      const job2 = manager.createJob(
        "test-job-2",
        () => {
          job2Ran = true;
        },
        Priority.high
      );

      expect(job1.getState()).toEqual(JobStates.Created);
      expect(job2.getState()).toEqual(JobStates.Created);

      const maxFailures = 2;
      await manager.runJobs(maxFailures);

      expect(job1.getState()).toEqual(JobStates.Completed);
      expect(job1Ran).toBe(true);
      expect(job2.getState()).toEqual(JobStates.Completed);
      expect(job2Ran).toBe(true);
    });

    it("should run all jobs in priority order", async () => {
      const manager = new JobManager();
      let hiPriorityJobRanAt: number | null = null;
      let lowPriorityJobRanAt: number | null = null;
      const hiPriorityJob = manager.createJob(
        "test-job-1",
        () => {
          hiPriorityJobRanAt = Date.now();
        },
        Priority.high
      );
      const lowPriorityJob = manager.createJob(
        "test-job-2",
        () => {
          lowPriorityJobRanAt = Date.now();
        },
        Priority.low
      );

      expect(hiPriorityJob.getState()).toEqual(JobStates.Created);
      expect(lowPriorityJob.getState()).toEqual(JobStates.Created);

      const maxFailures = 2;
      await manager.runJobs(maxFailures);

      expect(hiPriorityJob.getState()).toEqual(JobStates.Completed);
      expect(lowPriorityJob.getState()).toEqual(JobStates.Completed);
      expect(hiPriorityJobRanAt === null).toBe(false);
      expect(lowPriorityJobRanAt === null).toBe(false);
      expect(hiPriorityJobRanAt).toBeLessThan(lowPriorityJobRanAt);
    });

    it("should retry failed jobs", async () => {
      const manager = new JobManager();
      let count = 0;
      const job1 = manager.createJob(
        "test-job-1",
        () => {
          count++;
          throw new Error("TestError");
        },
        Priority.high
      );

      expect(job1.getState()).toEqual(JobStates.Created);

      const maxFailures = 2;
      await manager.runJobs(maxFailures);

      expect(job1.getState()).toEqual(JobStates.Failed);
      expect(count).toBe(2);

      count = 0;

      const job2 = manager.createJob(
        "test-job-2",
        () => {
          count++;
          if (count >= maxFailures) {
            return;
          }

          throw new Error("TestError");
        },
        Priority.high
      );

      await manager.runJobs(maxFailures);

      expect(job2.getState()).toEqual(JobStates.Completed);
    });

    it("should retry failed jobs with backoff", async () => {
      const manager = new JobManager();
      let count = 0;

      const attemptTimestamps = [];

      const job1 = manager.createJob(
        "test-job-1",
        () => {
          attemptTimestamps.push(Date.now());
          count++;
          throw new Error("TestError");
        },
        Priority.high
      );

      expect(job1.getState()).toEqual(JobStates.Created);

      const maxFailures = 4;
      await manager.runJobs(maxFailures);

      const waitMsTimes = attemptTimestamps.map((current, i) => {
        if (i === 0) {
          return 0;
        }

        return current - attemptTimestamps[i - 1];
      });

      waitMsTimes.forEach((value, i, array) => {
        if (i === 0) {
          return;
        }
        expect(value).toBeGreaterThan(array[i - 1]);
      });

      expect(job1.getState()).toEqual(JobStates.Failed);
      expect(count).toBe(maxFailures);
    }, 10_000);
  });

  describe(".cancelJob", () => {
    it("can cancel a job", async () => {
      const manager = new JobManager();
      let threwAbortError = false;
      const job = manager.createJob(
        "test-job-1",
        (signal: AbortSignal) => {
          if (signal.aborted) {
            threwAbortError = true;
            throw new Error("AbortError");
          }
        },
        Priority.high
      );

      expect(job.getState()).toEqual(JobStates.Created);
      expect(threwAbortError).toEqual(false);

      manager.cancel(job.id);
      const maxFailures = 0;
      await manager.runJobs(maxFailures);

      expect(job.getState()).toEqual(JobStates.Cancelled);
      expect(threwAbortError).toEqual(false);
    });

    it("can cancel a job while it is running", async () => {
      const manager = new JobManager();

      let threwAbortError = false;

      const job = manager.createJob(
        "test-job-1",
        (signal: AbortSignal) => {
          // Simulate a long-running job
          return new Promise((resolve) => setTimeout(resolve, 5000)).then(
            () => {
              if (signal.aborted) {
                threwAbortError = true;
                throw new Error("AbortError");
              }
            }
          );
        },
        Priority.high
      );

      expect(job.getState()).toEqual(JobStates.Created);
      expect(threwAbortError).toEqual(false);

      const maxFailures = 0;
      const jobsPromise = manager.runJobs(maxFailures);

      // Allow some time for the job to start
      await new Promise((resolve) => setTimeout(resolve, 500));

      manager.cancel(job.id);

      await jobsPromise;

      expect(job.getState()).toEqual(JobStates.Cancelled);
      expect(threwAbortError).toEqual(true);
    }, 10_000);

    it("can cancel a job while it is running even if the task does not respond to the abort signal", async () => {
      const manager = new JobManager();
      let abortedWhileExecuting = false;
      let executionStarted = false;
      const job = manager.createJob(
        "test-job-1",
        (signal: AbortSignal) => {
          executionStarted = true;

          // Simulate a long-running job
          return new Promise((resolve) => setTimeout(resolve, 5000)).then(
            () => {
              if (signal.aborted) {
                // Check the abort signal but do not throw an error in response.
                abortedWhileExecuting = true;
              }
            }
          );
        },
        Priority.high
      );

      expect(job.getState()).toEqual(JobStates.Created);
      expect(executionStarted).toEqual(false);

      const maxFailures = 0;
      const runJobsPromise = manager.runJobs(maxFailures);

      // Allow some time for the job to start
      await new Promise((resolve) => setTimeout(resolve, 500));
      expect(executionStarted).toEqual(true);
      expect(abortedWhileExecuting).toEqual(false);
      manager.cancel(job.id);

      await runJobsPromise;

      expect(job.getState()).toEqual(JobStates.Cancelled);
      expect(abortedWhileExecuting).toEqual(true);
    }, 10_000);

    it("cannot cancel a completed job", async () => {
      const manager = new JobManager();
      let jobRan = false;
      const job = manager.createJob(
        "test-job-1",
        () => {
          jobRan = true;
        },
        Priority.high
      );

      expect(job.getState()).toEqual(JobStates.Created);

      const maxFailures = 2;
      await manager.runJobs(maxFailures);

      expect(job.getState()).toEqual(JobStates.Completed);
      expect(jobRan).toBe(true);

      expect(() => manager.cancel(job.id)).toThrow();
    });
  });
});
