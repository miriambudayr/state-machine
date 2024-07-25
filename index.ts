enum JobStates {
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
