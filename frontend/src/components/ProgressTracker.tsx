import { useState } from 'react';
import { BulkImportJobStatus } from '../services/api';

interface ProgressTrackerProps {
  jobStatus: BulkImportJobStatus;
  onCancel?: () => void;
}

export default function ProgressTracker({ jobStatus, onCancel }: ProgressTrackerProps) {
  const [expanded, setExpanded] = useState(false);

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-green-100 text-green-800';
      case 'failed':
        return 'bg-red-100 text-red-800';
      case 'cancelled':
        return 'bg-yellow-100 text-yellow-800';
      case 'running':
        return 'bg-blue-100 text-blue-800';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const formatTime = (isoString?: string) => {
    if (!isoString) return 'N/A';
    return new Date(isoString).toLocaleString();
  };

  return (
    <div className="bg-white border rounded-lg p-4 mb-4">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-3">
          <span className={`px-3 py-1 rounded-full text-xs font-medium ${getStatusColor(jobStatus.status)}`}>
            {jobStatus.status.toUpperCase()}
          </span>
          <span className="text-sm text-gray-600">
            {jobStatus.job_type.replace(/_/g, ' ')}
            {jobStatus.cycle && ` - Cycle ${jobStatus.cycle}`}
            {jobStatus.progress_data?.data_type && ` - ${jobStatus.progress_data.data_type.replace(/_/g, ' ')}`}
          </span>
        </div>
        <div className="flex items-center gap-2">
          {jobStatus.status === 'running' && onCancel && (
            <button
              onClick={onCancel}
              className="px-3 py-1 text-sm bg-red-600 text-white rounded hover:bg-red-700"
            >
              Cancel
            </button>
          )}
          <button
            onClick={() => setExpanded(!expanded)}
            className="px-3 py-1 text-sm text-gray-600 hover:text-gray-900"
          >
            {expanded ? '▼' : '▶'} Details
          </button>
        </div>
      </div>

      {/* Progress Bar */}
      {jobStatus.status === 'running' && (
        <div className="mb-2">
          <div className="w-full bg-gray-200 rounded-full h-2.5">
            <div
              className="bg-blue-600 h-2.5 rounded-full transition-all duration-300"
              style={{ width: `${Math.min(100, jobStatus.overall_progress)}%` }}
            ></div>
          </div>
          <div className="text-xs text-gray-500 mt-1">
            {jobStatus.overall_progress.toFixed(1)}% complete
          </div>
        </div>
      )}

      {/* Summary Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm mb-2">
        {jobStatus.total_cycles > 0 && (
          <>
            <div>
              <span className="text-gray-500">Cycles:</span>{' '}
              <span className="font-medium">{jobStatus.completed_cycles}/{jobStatus.total_cycles}</span>
            </div>
            {jobStatus.current_cycle && (
              <div>
                <span className="text-gray-500">Current:</span>{' '}
                <span className="font-medium">{jobStatus.current_cycle}</span>
              </div>
            )}
          </>
        )}
        {jobStatus.total_chunks > 0 && (
          <div>
            <span className="text-gray-500">Chunks:</span>{' '}
            <span className="font-medium">{jobStatus.current_chunk}/{jobStatus.total_chunks}</span>
          </div>
        )}
        <div>
          <span className="text-gray-500">Records:</span>{' '}
          <span className="font-medium">{jobStatus.imported_records.toLocaleString()}</span>
        </div>
      </div>

      {/* Expanded Details */}
      {expanded && (
        <div className="mt-4 pt-4 border-t space-y-2 text-sm">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <span className="text-gray-500">Job ID:</span>{' '}
              <span className="font-mono text-xs">{jobStatus.job_id}</span>
            </div>
            <div>
              <span className="text-gray-500">Started:</span>{' '}
              {formatTime(jobStatus.started_at)}
            </div>
            {jobStatus.completed_at && (
              <div>
                <span className="text-gray-500">Completed:</span>{' '}
                {formatTime(jobStatus.completed_at)}
              </div>
            )}
            {jobStatus.skipped_records > 0 && (
              <div>
                <span className="text-gray-500">Skipped:</span>{' '}
                <span className="font-medium">{jobStatus.skipped_records.toLocaleString()}</span>
              </div>
            )}
          </div>

          {jobStatus.error_message && (
            <div className="bg-red-50 border border-red-200 text-red-800 px-3 py-2 rounded text-xs">
              <strong>Error:</strong> {jobStatus.error_message}
            </div>
          )}

          {jobStatus.progress_data && Object.keys(jobStatus.progress_data).length > 0 && (
            <div className="bg-gray-50 p-3 rounded">
              <div className="text-xs font-medium text-gray-700 mb-2">Progress Details:</div>
              <pre className="text-xs overflow-auto">
                {JSON.stringify(jobStatus.progress_data, null, 2)}
              </pre>
            </div>
          )}

          {jobStatus.cycles && jobStatus.cycles.length > 0 && (
            <div>
              <span className="text-gray-500">Cycles to import:</span>{' '}
              <span className="text-xs">{jobStatus.cycles.join(', ')}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

