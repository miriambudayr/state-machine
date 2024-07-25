import { assert } from "./libs/errors";
import { uuid } from "uuidv4";

export enum JobStates {
  Created = "Created",
  Processing = "Processing",
  Paused = "Paused",
  Completed = "Completed",
  Cancelled = "Cancelled",
  Failed = "Failed",
}

enum JobActions {
  Start = "Start",
  Pause = "Pause",
  Resume = "Resume",
  Complete = "Complete",
  Cancel = "Cancel",
  Fail = "Fail",
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
    [JobActions.Pause]: JobStates.Paused,
    [JobActions.Complete]: JobStates.Completed,
    [JobActions.Cancel]: JobStates.Cancelled,
    [JobActions.Fail]: JobStates.Failed,
  },
  [JobStates.Paused]: {
    [JobActions.Resume]: JobStates.Processing,
    [JobActions.Cancel]: JobStates.Cancelled,
  },
  [JobStates.Completed]: {},
  [JobStates.Cancelled]: {},
  [JobStates.Failed]: {},
};

class Job {
  id: string = uuid();
  name: string;
  created_at = new Date();
  fn: () => void;
  state: JobState = JobStates.Created;

  constructor(name: string, fn: () => void) {
    this.name = name;
    this.fn = fn;
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
  private jobs: Map<string, Job> = new Map();

  createJob(name: string, fn: () => void): Job {
    const job = new Job(name, fn);
    this.jobs.set(job.id, job);
    return job;
  }

  runJobs() {
    for (const job of this.jobs.values()) {
      assert(
        job.state === JobStates.Created,
        "expected job to be in the Created state",
        { actual: job.state }
      );

      job.performAction(JobActions.Start);

      try {
        job.fn();
        job.performAction(JobActions.Complete);
      } catch (e) {
        console.error("JobFailed", { id: job.id, name: job.name });
        job.performAction(JobActions.Fail);
      }
    }
  }
}
