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
  id: string = uuid();
  name: string;
  created_at = new Date();
  priority: Priority;
  fn: () => void;
  state: JobState = JobStates.Created;

  constructor(name: string, fn: () => void, priority: Priority) {
    this.name = name;
    this.fn = fn;
    this.priority = priority;
  }

  performAction(action: JobAction) {
    const next = stateMachine[this.state][action];
    assert(next !== undefined, "Invalid state transition", {
      from: this.state,
      action,
    });

    console.log(
      `Transitioning from ${this.state} to ${next} via ${action} action`
    );

    this.state = next;
  }

  getState(): JobState {
    return this.state;
  }
}

export class JobManager {
  private jobs: { [priority in Priority]: Map<string, Job> } = {
    [Priority.high]: new Map(),
    [Priority.medium]: new Map(),
    [Priority.low]: new Map(),
  };

  createJob(name: string, fn: () => void, priority: Priority): Job {
    const job = new Job(name, fn, priority);
    this.jobs[priority].set(job.id, job);
    return job;
  }

  async runJobs(maxFailures: number) {
    for (const priority of [Priority.high, Priority.medium, Priority.low]) {
      await Promise.all(
        Array.from(this.jobs[priority].values()).map(async (job) => {
          assert(
            job.state === JobStates.Created,
            "expected job to be in the Created state",
            { actual: job.state }
          );

          job.performAction(JobActions.Start);

          try {
            await withExponentialBackoff(job.fn, maxFailures, 1000);
            job.performAction(JobActions.Complete);
          } catch (e) {
            console.error("JobFailed", { id: job.id, name: job.name });
            job.performAction(JobActions.Fail);
          }

          this.jobs[priority].delete(job.id);
        })
      );
    }
  }
}

async function waitForTime(time_ms: number) {
  return new Promise((res) => {
    setTimeout(res, time_ms);
  });
}

async function withExponentialBackoff(
  fn: () => void,
  maxFailures: number,
  baseBackoffMillis: number
) {
  let attempt = 0;

  while (true) {
    try {
      return fn();
    } catch (e) {
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
