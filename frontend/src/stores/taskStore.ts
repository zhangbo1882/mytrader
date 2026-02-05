import { create } from 'zustand';
import type { Task } from '@/types';

interface TaskState {
  tasks: Task[];
  runningTask: Task | null;

  setTasks: (tasks: Task[]) => void;
  updateTask: (task: Task) => void;
  addTask: (task: Task) => void;
  removeTask: (id: string) => void;
  setRunningTask: (task: Task | null) => void;
}

export const useTaskStore = create<TaskState>((set, get) => ({
  tasks: [],
  runningTask: null,

  setTasks: (tasks) => {
    set({ tasks });
    // Update running task
    const running = tasks.find((t) => t.status === 'running') || null;
    set({ runningTask: running });
  },

  updateTask: (task) =>
    set((state) => ({
      tasks: state.tasks.map((t) => (t.id === task.id ? task : t)),
    })),

  addTask: (task) =>
    set((state) => ({
      tasks: [task, ...state.tasks],
    })),

  removeTask: (id) =>
    set((state) => ({
      tasks: state.tasks.filter((t) => t.id !== id),
    })),

  setRunningTask: (task) => set({ runningTask: task }),
}));
