import { JobManager, JobStates, Priority } from ".";

describe("JobManager", () => {
  describe(".createJob", () => {
    it("should create a job", () => {
      const manager = new JobManager();
      const job = manager.createJob("test-job", () => {}, Priority.high);
      expect(job.name).toBe("test-job");
    });
  });

  describe(".runJobs", () => {
    it("should run all jobs", () => {
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

      manager.runJobs();

      expect(job1.getState()).toEqual(JobStates.Completed);
      expect(job1Ran).toBe(true);
      expect(job2.getState()).toEqual(JobStates.Completed);
      expect(job2Ran).toBe(true);
    });

    it("should run all jobs in priority order", () => {
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

      manager.runJobs();

      expect(hiPriorityJob.getState()).toEqual(JobStates.Completed);
      expect(lowPriorityJob.getState()).toEqual(JobStates.Completed);
      expect(hiPriorityJobRanAt === null).toBe(false);
      expect(lowPriorityJobRanAt === null).toBe(false);
      expect(hiPriorityJobRanAt).toBeLessThan(lowPriorityJobRanAt);
    });
  });
});
