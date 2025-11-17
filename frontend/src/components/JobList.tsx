import { useState } from 'react';
import { BulkImportJobStatus } from '../services/api';
import ProgressTracker from './ProgressTracker';

interface JobListProps {
  jobs: BulkImportJobStatus[];
  onCancel: (jobId: string) => void;
  onRefresh?: () => void;
  loading?: boolean;
}

export default function JobList({ jobs, onCancel }: JobListProps) {
  const [collapsedStatuses, setCollapsedStatuses] = useState<Set<string>>(
    new Set(['completed', 'failed', 'cancelled'])
  );

  if (jobs.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500">
        <p className="text-sm">No recent import jobs</p>
        <p className="text-xs mt-1">Start an import to see progress here</p>
      </div>
    );
  }

  // Group jobs by status
  const jobsByStatus: Record<string, BulkImportJobStatus[]> = {
    running: [],
    pending: [],
    completed: [],
    failed: [],
    cancelled: [],
  };

  jobs.forEach((job) => {
    const status = job.status || 'unknown';
    if (status in jobsByStatus) {
      jobsByStatus[status].push(job);
    }
  });

  // Sort each group by started_at descending
  Object.keys(jobsByStatus).forEach((status) => {
    jobsByStatus[status].sort((a, b) => {
      const aTime = a.started_at ? new Date(a.started_at).getTime() : 0;
      const bTime = b.started_at ? new Date(b.started_at).getTime() : 0;
      return bTime - aTime;
    });
  });

  const statusLabels: Record<string, { label: string; icon: string; color: string }> = {
    running: { label: 'Running', icon: '▶️', color: 'text-blue-600' },
    pending: { label: 'Pending', icon: '⏳', color: 'text-yellow-600' },
    completed: { label: 'Completed', icon: '✅', color: 'text-green-600' },
    failed: { label: 'Failed', icon: '❌', color: 'text-red-600' },
    cancelled: { label: 'Cancelled', icon: '🚫', color: 'text-gray-600' },
  };

  const statusOrder = ['running', 'pending', 'completed', 'failed', 'cancelled'];

  const toggleCollapse = (status: string) => {
    setCollapsedStatuses((prev) => {
      const updated = new Set(prev);
      if (updated.has(status)) {
        updated.delete(status);
      } else {
        updated.add(status);
      }
      return updated;
    });
  };

  return (
    <div className="space-y-3">
      {statusOrder.map((status) => {
        const statusJobs = jobsByStatus[status];
        if (statusJobs.length === 0) return null;

        const isCollapsed = collapsedStatuses.has(status);
        const statusInfo = statusLabels[status] || { label: status, icon: '📋', color: 'text-gray-600' };

        return (
          <div key={status} className="border border-gray-200 rounded-lg overflow-hidden bg-white">
            <button
              onClick={() => toggleCollapse(status)}
              className="w-full px-4 py-3 bg-gray-50 hover:bg-gray-100 flex items-center justify-between text-left transition-colors"
            >
              <div className="flex items-center gap-3">
                <span className="text-lg">{statusInfo.icon}</span>
                <span className={`font-semibold ${statusInfo.color}`}>
                  {statusInfo.label} ({statusJobs.length})
                </span>
              </div>
              <span className="text-gray-500 text-sm">{isCollapsed ? '▶' : '▼'}</span>
            </button>

            {!isCollapsed && (
              <div className="p-4 space-y-4 bg-white border-t border-gray-100">
                {statusJobs.map((job) => (
                  <ProgressTracker
                    key={job.job_id}
                    jobStatus={job}
                    onCancel={job.status === 'running' ? () => onCancel(job.job_id) : undefined}
                  />
                ))}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

