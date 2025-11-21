import { useState, useEffect, useCallback } from 'react';
import { bulkDataApi, BulkDataStatus, BulkDataCycle, CycleStatus } from '../services/api';
import DataTypeGrid from '../components/DataTypeGrid';
import JobList from '../components/JobList';
import BulkDataStatusSummary from '../components/BulkDataStatusSummary';
import BulkDataOperations from '../components/BulkDataOperations';
import CycleSelector from '../components/CycleSelector';
import QuickStats from '../components/QuickStats';
import { useMultipleJobTracking } from '../hooks/useMultipleJobTracking';
import { useBulkDataOperations } from '../hooks/useBulkDataOperations';

export default function BulkDataManagement() {
  const [status, setStatus] = useState<BulkDataStatus | null>(null);
  const [cycles, setCycles] = useState<BulkDataCycle[]>([]);
  const [cycleStatus, setCycleStatus] = useState<CycleStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [selectedCycle, setSelectedCycle] = useState<number>(2024);
  const [selectedDataTypes, setSelectedDataTypes] = useState<Set<string>>(new Set());
  const [forceDownload, setForceDownload] = useState<boolean>(false);
  const [initialLoading, setInitialLoading] = useState(true);

  // Load initial data
  const loadStatus = useCallback(async () => {
    try {
      const [statusData, cyclesData] = await Promise.all([
        bulkDataApi.getStatus(),
        bulkDataApi.getCycles(),
      ]);
      setStatus(statusData);
      setCycles(cyclesData.cycles || []);

      // Set default cycle to most recent available or current year
      if (cyclesData.cycles && cyclesData.cycles.length > 0) {
        const firstCycle = cyclesData.cycles[0];
        const defaultCycle = firstCycle.cycle || 2024;
        if (!selectedCycle || selectedCycle === 2024) {
          setSelectedCycle(defaultCycle);
        }
      } else {
        const currentYear = new Date().getFullYear();
        const defaultCycle = Math.floor(currentYear / 2) * 2; // Nearest even year
        if (!selectedCycle || selectedCycle === 2024) {
          setSelectedCycle(defaultCycle);
        }
      }
    } catch (err: any) {
      setError(err.message || 'Failed to load bulk data status');
    }
  }, []);

  const loadCycleStatus = useCallback(async () => {
    try {
      const status = await bulkDataApi.getDataTypeStatus(selectedCycle);
      // Validate response structure
      if (status && status.data_types && Array.isArray(status.data_types)) {
        setCycleStatus(status);
      } else {
        setCycleStatus(null);
        setError('Invalid response from server. Please try again.');
      }
    } catch (err: any) {
      setCycleStatus(null);
      // Don't set error here as it might be a temporary issue
    }
  }, [selectedCycle]);

  // Job tracking with callback to refresh data when jobs complete
  const handleJobComplete = useCallback(() => {
    loadStatus();
    loadCycleStatus();
    // Note: loadRecentJobs is called separately when needed (e.g., on manual refresh)
  }, [loadStatus, loadCycleStatus]);

  const {
    jobs: activeJobs,
    startTracking,
    cancelJob: handleCancelJobInternal,
    loadJobs,
  } = useMultipleJobTracking(handleJobComplete);

  const loadRecentJobs = useCallback(async () => {
    try {
      const result = await bulkDataApi.getRecentJobs(20); // Get last 20 jobs
      // Load all jobs into state - this will also start tracking active ones
      loadJobs(result.jobs);
    } catch (err) {
      // Don't show error to user - this is just for display
    }
  }, [loadJobs]);

  // Operations hook
  const {
    loading,
    handleDownload,
    handleImportSelected,
    handleImportAllTypes,
    handleClearContributions,
    handleCleanupAndReimport,
    handleImportAll,
    handleRefreshCycles,
    handleComputeAnalysis,
  } = useBulkDataOperations({
    selectedCycle,
    selectedDataTypes,
    forceDownload,
    onSuccess: (message) => {
      setSuccess(message);
      setError(null);
      // Auto-dismiss success after 5 seconds
      setTimeout(() => setSuccess(null), 5000);
    },
    onError: (message) => {
      setError(message);
      setSuccess(null);
    },
    onJobStarted: (jobId) => {
      startTracking(jobId);
      // Also refresh recent jobs to ensure it appears immediately
      setTimeout(() => loadRecentJobs(), 500);
    },
    onStatusRefresh: loadStatus,
    onCycleStatusRefresh: loadCycleStatus,
  });

  // Initial load
  useEffect(() => {
    const initialize = async () => {
      await loadStatus();
      await loadRecentJobs();
      setInitialLoading(false);
    };
    initialize();
  }, []); // Only run on mount

  useEffect(() => {
    // Load cycle status when selected cycle changes
    loadCycleStatus();
  }, [selectedCycle, loadCycleStatus]);

  // Data type selection handlers
  const handleDataTypeSelection = (dataType: string, selected: boolean) => {
    setSelectedDataTypes((prev) => {
      const updated = new Set(prev);
      if (selected) {
        updated.add(dataType);
      } else {
        updated.delete(dataType);
      }
      return updated;
    });
  };

  const handleSelectAll = () => {
    if (!cycleStatus) return;
    const implemented = cycleStatus.data_types
      .filter((dt) => dt.is_implemented)
      .map((dt) => dt.data_type);
    setSelectedDataTypes(new Set(implemented));
  };

  const handleDeselectAll = () => {
    setSelectedDataTypes(new Set());
  };

  const handleCancelJob = async (jobId: string) => {
    if (!window.confirm('Are you sure you want to cancel this job?')) {
      return;
    }

    try {
      await handleCancelJobInternal(jobId);
    } catch (err: any) {
      setError(err.message || 'Failed to cancel job');
    }
  };

  // Calculate stats for components
  const runningJobs = Array.from(activeJobs.values()).filter((j) => j.status === 'running').length;
  const completedToday = Array.from(activeJobs.values()).filter((j) => {
    if (j.status === 'completed' && j.completed_at) {
      const completedDate = new Date(j.completed_at);
      const today = new Date();
      today.setHours(0, 0, 0, 0);
      return completedDate >= today;
    }
    return false;
  }).length;

  if (initialLoading && !status) {
    return (
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="animate-pulse">
          <div className="h-8 bg-gray-200 rounded w-1/2 mb-4"></div>
          <div className="h-4 bg-gray-200 rounded w-1/3"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">Bulk Data Management</h1>
        <p className="text-gray-600">
          Manage FEC bulk CSV data downloads and imports. Use this to clean up incorrectly imported
          data and reimport with correct mappings.
        </p>
      </div>

      {/* Error/Success Messages */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 text-red-800 px-4 py-3 rounded">
          <strong>Error:</strong> {error}
        </div>
      )}

      {success && (
        <div className="mb-6 bg-green-50 border border-green-200 text-green-800 px-4 py-3 rounded">
          <strong>Success:</strong> {success}
        </div>
      )}

      {/* Quick Stats */}
      <div className="mb-6">
        <QuickStats status={status} cycles={cycles} runningJobs={runningJobs} />
      </div>

      {/* Jobs Section - Always Visible */}
      <div className="mb-6 bg-gradient-to-r from-blue-50 to-indigo-50 border-2 border-blue-200 rounded-lg p-6 shadow-lg">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-3">
            {runningJobs > 0 && (
              <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
            )}
            <h2 className="text-2xl font-bold text-gray-900">
              Import Jobs ({activeJobs.size})
            </h2>
          </div>
          <div className="flex items-center gap-3">
            {runningJobs > 0 && (
              <span className="px-3 py-1 bg-blue-100 text-blue-800 text-sm font-medium rounded-full">
                {runningJobs} running
              </span>
            )}
            <button
              onClick={loadRecentJobs}
              disabled={loading}
              className="px-3 py-1.5 text-sm bg-white border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
              title="Refresh job status"
            >
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
                />
              </svg>
              Refresh
            </button>
          </div>
        </div>

        <JobList
          jobs={Array.from(activeJobs.values())}
          onCancel={handleCancelJob}
          onRefresh={loadRecentJobs}
          loading={loading}
        />
      </div>

      {/* Status Summary */}
      <div className="mb-6">
        <BulkDataStatusSummary
          status={status}
          cycles={cycles}
          totalJobs={activeJobs.size}
          runningJobs={runningJobs}
          completedToday={completedToday}
        />
      </div>

      {/* Cycle Selector */}
      <div className="mb-6">
        <CycleSelector
          cycles={cycles}
          selectedCycle={selectedCycle}
          onCycleChange={(cycle) => {
            setSelectedCycle(cycle);
            setSelectedDataTypes(new Set()); // Clear selections when cycle changes
          }}
          onRefresh={loadCycleStatus}
          onSearchCycles={handleRefreshCycles}
          loading={loading}
        />
      </div>

      {/* Data Type Grid */}
      <div className="bg-white shadow rounded-lg p-6 mb-6">
        <DataTypeGrid
          cycleStatus={cycleStatus}
          selectedTypes={selectedDataTypes}
          onSelectionChange={handleDataTypeSelection}
          onSelectAll={handleSelectAll}
          onDeselectAll={handleDeselectAll}
          loading={loading}
        />
      </div>

      {/* Operations Panel */}
      <div className="mb-6">
        <BulkDataOperations
          selectedDataTypesCount={selectedDataTypes.size}
          loading={loading}
          forceDownload={forceDownload}
          onForceDownloadChange={setForceDownload}
          onDownload={handleDownload}
          onImportSelected={handleImportSelected}
          onImportAllTypes={handleImportAllTypes}
          onClearContributions={handleClearContributions}
          onCleanupAndReimport={handleCleanupAndReimport}
          onImportAll={handleImportAll}
          onComputeAnalysis={handleComputeAnalysis}
          selectedCycle={selectedCycle}
        />
      </div>
    </div>
  );
}
