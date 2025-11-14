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

  // Get current stage from progress_data
  const getCurrentStage = () => {
    const progressData = jobStatus.progress_data || {};
    const status = progressData.status || 'starting';
    
    const stageMap: Record<string, { label: string; icon: string; color: string }> = {
      'downloading': { label: 'Downloading', icon: '⬇️', color: 'text-blue-600' },
      'extracting': { label: 'Extracting', icon: '📦', color: 'text-purple-600' },
      'parsing': { label: 'Parsing', icon: '📄', color: 'text-indigo-600' },
      'importing': { label: 'Importing', icon: '💾', color: 'text-green-600' },
      'clearing': { label: 'Clearing', icon: '🗑️', color: 'text-red-600' },
      'starting': { label: 'Starting', icon: '⏳', color: 'text-gray-600' },
      'completed': { label: 'Completed', icon: '✅', color: 'text-green-600' },
      'failed': { label: 'Failed', icon: '❌', color: 'text-red-600' },
    };
    
    return stageMap[status] || { label: status, icon: '⏳', color: 'text-gray-600' };
  };

  const getStageProgress = () => {
    const progressData = jobStatus.progress_data || {};
    const status = progressData.status || 'starting';
    
    // Calculate stage-specific progress
    if (status === 'downloading' && progressData.cycle_progress) {
      const cycleProgress = progressData.cycle_progress[jobStatus.current_cycle || jobStatus.cycle];
      if (cycleProgress && cycleProgress.total_mb) {
        const pct = (cycleProgress.downloaded_mb / cycleProgress.total_mb) * 100;
        return {
          percentage: Math.min(100, pct),
          details: `${(cycleProgress.downloaded_mb || 0).toFixed(1)} MB / ${cycleProgress.total_mb.toFixed(1)} MB`
        };
      }
    }
    
    if (status === 'importing' || status === 'parsing') {
      if (jobStatus.total_chunks > 0) {
        const pct = (jobStatus.current_chunk / jobStatus.total_chunks) * 100;
        return {
          percentage: Math.min(100, pct),
          details: `Chunk ${jobStatus.current_chunk} / ${jobStatus.total_chunks}`
        };
      }
    }
    
    // Fallback to overall progress
    return {
      percentage: jobStatus.overall_progress || 0,
      details: null
    };
  };

  const currentStage = getCurrentStage();
  const stageProgress = getStageProgress();

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

      {/* Current Stage Indicator */}
      {jobStatus.status === 'running' && (
        <div className="mb-3">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-lg">{currentStage.icon}</span>
            <span className={`text-sm font-medium ${currentStage.color}`}>
              {currentStage.label}
              {jobStatus.progress_data?.data_type && (
                <span className="text-gray-500 ml-2">
                  ({jobStatus.progress_data.data_type.replace(/_/g, ' ')})
                </span>
              )}
            </span>
          </div>
          
          {/* Stage Progress Bar */}
          <div className="w-full bg-gray-200 rounded-full h-3 mb-1">
            <div
              className="h-3 rounded-full transition-all duration-300"
              style={{ 
                width: `${Math.min(100, stageProgress.percentage)}%`,
                backgroundColor: currentStage.color === 'text-blue-600' ? '#2563eb' :
                                currentStage.color === 'text-purple-600' ? '#9333ea' :
                                currentStage.color === 'text-indigo-600' ? '#4f46e5' :
                                currentStage.color === 'text-green-600' ? '#16a34a' :
                                currentStage.color === 'text-red-600' ? '#dc2626' :
                                '#6b7280'
              }}
            ></div>
          </div>
          
          {/* Progress Details */}
          <div className="flex justify-between items-center text-xs text-gray-600">
            <span>
              {stageProgress.details || `${stageProgress.percentage.toFixed(1)}% complete`}
            </span>
            {jobStatus.progress_data?.resuming && (
              <span className="text-orange-600 font-medium">Resuming from checkpoint...</span>
            )}
          </div>
          
          {/* Overall Progress (if different from stage progress) */}
          {jobStatus.overall_progress > 0 && Math.abs(jobStatus.overall_progress - stageProgress.percentage) > 5 && (
            <div className="mt-2">
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span>Overall Progress</span>
                <span>{jobStatus.overall_progress.toFixed(1)}%</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div
                  className="bg-gray-400 h-2 rounded-full transition-all duration-300"
                  style={{ width: `${Math.min(100, jobStatus.overall_progress)}%` }}
                ></div>
              </div>
            </div>
          )}
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
        {jobStatus.progress_data?.file_position && jobStatus.progress_data.file_position > 0 && (
          <div>
            <span className="text-gray-500">File Position:</span>{' '}
            <span className="font-medium">{(jobStatus.progress_data.file_position / 1000000).toFixed(1)}M rows</span>
          </div>
        )}
      </div>
      
      {/* Download Progress (if downloading) */}
      {jobStatus.progress_data?.status === 'downloading' && jobStatus.progress_data.cycle_progress && (
        <div className="mb-2 p-2 bg-blue-50 rounded text-xs">
          {Object.entries(jobStatus.progress_data.cycle_progress).map(([cycle, progress]: [string, any]) => (
            <div key={cycle} className="mb-1">
              <div className="flex justify-between mb-1">
                <span className="text-gray-700">Cycle {cycle}:</span>
                <span className="font-medium text-blue-700">
                  {progress.downloaded_mb?.toFixed(1) || 0} MB
                  {progress.total_mb && ` / ${progress.total_mb.toFixed(1)} MB`}
                  {progress.progress_pct && ` (${progress.progress_pct.toFixed(1)}%)`}
                </span>
              </div>
              {progress.total_mb && (
                <div className="w-full bg-blue-200 rounded-full h-1.5">
                  <div
                    className="bg-blue-600 h-1.5 rounded-full transition-all duration-300"
                    style={{ width: `${Math.min(100, (progress.downloaded_mb / progress.total_mb) * 100)}%` }}
                  ></div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

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

