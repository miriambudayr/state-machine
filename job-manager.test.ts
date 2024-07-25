import { JobManager, JobStates } from ".";

describe("JobManager", () => {
  describe(".createJob", () => {
    it("should create a job", () => {
      const manager = new JobManager();
      const job = manager.createJob("test-job", () => {});
      expect(job.name).toBe("test-job");
    });
  });

  describe(".runJobs", () => {
    it("should run all jobs", () => {
      const manager = new JobManager();
      let job1Ran = false;
      let job2Ran = false;
      const job1 = manager.createJob("test-job-1", () => {
        job1Ran = true;
      });
      const job2 = manager.createJob("test-job-2", () => {
        job2Ran = true;
      });

      expect(job1.getState()).toEqual(JobStates.Created);
      expect(job2.getState()).toEqual(JobStates.Created);

      manager.runJobs();

      expect(job1.getState()).toEqual(JobStates.Completed);
      expect(job1Ran).toBe(true);
      expect(job2.getState()).toEqual(JobStates.Completed);
      expect(job2Ran).toBe(true);
    });
  });
});
