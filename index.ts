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
  // The easiest way to manage asynchronous cancellation is to generously define all possible actions
  // that can be taken on the `JobStates.Cancelled` state rather than adding brittle job state checks
  // throughout the code (I tried this and it wasn't good; then remembered the problems state machines solve).
  [JobStates.Cancelled]: {
    [JobActions.Start]: JobStates.Cancelled,
    [JobActions.Complete]: JobStates.Cancelled,
    [JobActions.Cancel]: JobStates.Cancelled,
    [JobActions.Fail]: JobStates.Cancelled,
  },
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

  getSummary() {
    return {
      name: this.name,
      priority: this.getPriority(),
      id: this.id,
      state: this.getState(),
    };
  }
}

export class JobManager {
  // invariant: each job is added to both the `JobManager.jobs` data structure
  // and the `JobManager.jobsById` map.
  private jobs: { [priority in Priority]: Map<string, Job> } = {
    [Priority.high]: new Map(),
    [Priority.medium]: new Map(),
    [Priority.low]: new Map(),
  };

  // invariant: each job is added to both the `JobManager.jobs` data structure
  // and the `JobManager.jobsById` map.
  private jobsById: Map<string, Job> = new Map();

  private isCancelled(job: Job) {
    if (job.getState() === JobStates.Cancelled) {
      // Jobs should be removed from state by the `.cancel` method.
      // This check is defensive.
      this.assertJobRemoved(job);
      return true;
    }

    return false;
  }

  private assertTerminalState(job: Job) {
    const state = job.getState();
    const isTerminalState =
      Object.keys(stateMachine[state]).length === 0 ||
      job.getState() === JobStates.Cancelled;
    assert(isTerminalState, "unexpected terminal state", { jobState: state });
  }

  private assertJobRemoved(job: Job) {
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
  }

  private removeJob(job: Job) {
    this.assertTerminalState(job);
    this.jobs[job.getPriority()].delete(job.id);
    this.jobsById.delete(job.id);
    this.assertJobRemoved(job);
  }

  private transitionState(job: Job, action: JobAction) {
    const next = stateMachine[job.getState()][action];
    assert(next !== undefined, "Invalid state transition", {
      from: job.getState(),
      action,
    });

    console.log("TransitioningState", {
      from: job.getState(),
      to: next,
      action,
      id: job.id,
      name: job.name,
    });

    job.setState(next);
  }

  createJob(
    name: string,
    fn: (abortSignal: AbortSignal) => void,
    priority: Priority
  ): Job {
    const job = new Job(name, fn, priority);
    this.jobs[priority].set(job.id, job);
    this.jobsById.set(job.id, job);

    console.log("JobCreated", job.getSummary());
    return job;
  }

  async runJobs(maxFailures: number) {
    console.log("WillRunJobs");
    for (const priority of [Priority.high, Priority.medium, Priority.low]) {
      await Promise.all(
        Array.from(this.jobs[priority].values()).map(async (job) => {
          console.log("WillRunJob", job.getSummary());

          try {
            // Jobs handles can be cancelled while `runJobs` is running, which requires an additional check.
            if (this.isCancelled(job)) {
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

            // Job handles can be cancelled while the execution of the job is running.
            // The state machine will ensure the job remains in the `Cancelled` state.
            if (this.isCancelled(job)) {
              console.log(
                "ExecutionCompletedWithJobCancellation",
                job.getSummary()
              );
            }

            this.transitionState(job, JobActions.Complete);
            console.log("JobCompletedSuccessfully", job.getSummary());
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
            console.log("WillRemoveJob", job.getSummary());
            this.assertTerminalState(job);
            this.removeJob(job);
          }
        })
      );
    }
  }

  // Idempotent.
  // This provides no guarantee that the execution of the job function will be immediately terminated because Node.js does not provide
  // concurrency primitives that allow for that.
  // This simply guarantees that an in-progress or created job will be cancelled. Upon job creation, user can pass in an abort signal and manage
  // the job execution control flow via that signal.
  cancel(jobId: string) {
    const job = this.jobsById.get(jobId);
    assert(!!job, "unknown job id", { jobId });

    if (this.isCancelled(job)) {
      return;
    }

    console.log("WillCancelJob", job.getSummary());
    this.transitionState(job, JobActions.Cancel);
    this.removeJob(job);
    job.abort();
    console.log("JobCanceled", job.getSummary());
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
