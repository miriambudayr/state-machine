import { assert } from "./libs/errors";
import { uuid } from "uuidv4";

export enum JobStates {
  Created = "Created",
  Processing = "Processing",
  Completed = "Completed",
  Cancelled = "Cancelled",
  Failed = "Failed",
}

enum JobActions {
  Start = "Start",
  Complete = "Complete",
  Cancel = "Cancel",
  Fail = "Fail",
}

export enum Priority {
  low = "low",
  medium = "medium",
  high = "high",
}

type StateMachine = {
  [state in JobStates]: {
    [action in JobActions]?: JobState;
  };
};

type JobState = keyof typeof JobStates;
type JobAction = keyof typeof JobActions;

const stateMachine: StateMachine = {
  [JobStates.Created]: {
    [JobActions.Start]: JobStates.Processing,
    [JobActions.Cancel]: JobStates.Cancelled,
  },
  [JobStates.Processing]: {
    [JobActions.Complete]: JobStates.Completed,
    [JobActions.Cancel]: JobStates.Cancelled,
    [JobActions.Fail]: JobStates.Failed,
  },
  [JobStates.Completed]: {},
  [JobStates.Cancelled]: {},
  [JobStates.Failed]: {},
};

class Job {
  readonly id: string = uuid();
  readonly name: string;
  readonly created_at = new Date();
  private fn: (signal: AbortSignal) => void;
  private priority: Priority;
  private state: JobState = JobStates.Created;
  private abortController: AbortController;

  constructor(
    name: string,
    fn: (signal: AbortSignal) => void,
    priority: Priority
  ) {
    this.name = name;
    this.fn = fn;
    this.priority = priority;
    this.abortController = new AbortController();
  }

  getState(): JobState {
    return this.state;
  }

  setState(state: JobState) {
    this.state = state;
  }

  getPriority(): Priority {
    return this.priority;
  }

  getSignal(): AbortSignal {
    return this.abortController.signal;
  }

  getCallback() {
    return this.fn;
  }

  abort() {
    this.abortController.abort();
  }
}

export class JobManager {
  private jobs: { [priority in Priority]: Map<string, Job> } = {
    [Priority.high]: new Map(),
    [Priority.medium]: new Map(),
    [Priority.low]: new Map(),
  };

  // invariant: each job is added to both the `JobManager.jobs` data structure
  // and the `JobManager.jobsById` map.
  private jobsById: Map<string, Job> = new Map();

  createJob(
    name: string,
    fn: (abortSignal: AbortSignal) => void,
    priority: Priority
  ): Job {
    const job = new Job(name, fn, priority);
    this.jobs[priority].set(job.id, job);
    this.jobsById.set(job.id, job);
    return job;
  }

  async runJobs(maxFailures: number) {
    for (const priority of [Priority.high, Priority.medium, Priority.low]) {
      await Promise.all(
        Array.from(this.jobs[priority].values()).map(async (job) => {
          try {
            if (job.getState() === JobStates.Cancelled) {
              // Jobs should be removed from state by the `.cancel` method.
              // This check is defensive.
              assert(
                this.jobsById.get(job.id) === undefined,
                "cancelled job should have been removed from jobsById map",
                { jobId: job.id, jobState: job.getState() }
              );

              assert(
                this.jobs[job.getPriority()].get(job.id) === undefined,
                "cancelled job should have been removed from the priority jobs map",
                { jobId: job.id, jobState: job.getState() }
              );
              return;
            }

            assert(
              job.getState() === JobStates.Created,
              "expected job to be in the Created state",
              { actual: job.getState() }
            );

            this.transitionState(job, JobActions.Start);

            await withExponentialBackoff(
              job.getCallback(),
              job.getSignal(),
              maxFailures,
              1000
            );
            this.transitionState(job, JobActions.Complete);
          } catch (e) {
            if (e.name === "AbortError") {
              assert(
                job.getState() === JobStates.Cancelled,
                "abort error should only be thrown for a cancelled job",
                { jobId: job.id, jobState: job.getState() }
              );
              console.error("AbortError", { id: job.id, name: job.name });
              return;
            }
            console.error("JobFailed", { id: job.id, name: job.name });
            this.transitionState(job, JobActions.Fail);
          } finally {
            assert(
              job.getState() === JobStates.Cancelled ||
                job.getState() === JobStates.Completed ||
                job.getState() === JobStates.Failed,
              "job run resulted in unexpected terminal state",
              { jobState: job.getState() }
            );
            this.removeJob(job);
          }
        })
      );
    }
  }

  cancel(jobId: string) {
    const job = this.jobsById.get(jobId);
    assert(!!job, "unknown job id", { jobId });
    this.removeJob(job);
    this.transitionState(job, JobActions.Cancel);
    job.abort();
  }

  private removeJob(job: Job) {
    assert(
      job.getState() === JobStates.Cancelled ||
        job.getState() === JobStates.Completed ||
        job.getState() === JobStates.Failed,
      "cannot remove job in unexpected terminal state",
      { jobState: job.getState() }
    );
    this.jobs[job.getPriority()].delete(job.id);
    this.jobsById.delete(job.id);
  }

  private transitionState(job: Job, action: JobAction) {
    const next = stateMachine[job.getState()][action];
    assert(next !== undefined, "Invalid state transition", {
      from: job.getState(),
      action,
    });

    console.log(
      `Transitioning from ${job.getState()} to ${next} via ${action} action`
    );

    job.setState(next);
  }
}

async function waitForTime(time_ms: number) {
  return new Promise((res) => {
    setTimeout(res, time_ms);
  });
}

async function withExponentialBackoff(
  fn: (signal: AbortSignal) => void,
  signal: AbortSignal,
  maxFailures: number,
  baseBackoffMillis: number
) {
  let attempt = 0;

  while (true) {
    try {
      if (signal.aborted) {
        throw new Error("AbortError");
      }
      return fn(signal);
    } catch (e) {
      if (e.name === "AbortError") {
        // Rethrow the error so the calling function can handle it properly.
        throw e;
      }

      attempt++;

      if (attempt >= maxFailures) {
        throw e;
      }

      const delay = 2 ** attempt * (baseBackoffMillis / 2);
      const jitter = Math.random() * (baseBackoffMillis / 2);
      await waitForTime(delay + jitter);
    }
  }
}
