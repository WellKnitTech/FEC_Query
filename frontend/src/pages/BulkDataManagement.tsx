import { useState, useEffect, useRef } from 'react';
import { bulkDataApi, BulkDataStatus, BulkDataCycle, BulkImportJobStatus, CycleStatus } from '../services/api';
import ProgressTracker from '../components/ProgressTracker';
import DataTypeGrid from '../components/DataTypeGrid';

export default function BulkDataManagement() {
  const [status, setStatus] = useState<BulkDataStatus | null>(null);
  const [cycles, setCycles] = useState<BulkDataCycle[]>([]);
  const [cycleStatus, setCycleStatus] = useState<CycleStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [selectedCycle, setSelectedCycle] = useState<number>(2024);
  const [selectedDataTypes, setSelectedDataTypes] = useState<Set<string>>(new Set());
  const [operationInProgress, setOperationInProgress] = useState<string | null>(null);
  const [activeJobs, setActiveJobs] = useState<Map<string, BulkImportJobStatus>>(new Map());
  const wsRefs = useRef<Map<string, WebSocket>>(new Map());
  const pollingIntervals = useRef<Map<string, NodeJS.Timeout>>(new Map());

  useEffect(() => {
    loadStatus();
    
    // Cleanup on unmount
    return () => {
      wsRefs.current.forEach((ws) => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.close();
        }
      });
      pollingIntervals.current.forEach((interval) => clearInterval(interval));
    };
  }, []);

  useEffect(() => {
    // Load cycle status when selected cycle changes
    loadCycleStatus();
  }, [selectedCycle]);

  const startJobTracking = (jobId: string) => {
    // Try WebSocket first
    const ws = bulkDataApi.createWebSocket(
      jobId,
      (message) => {
        if (message.type === 'progress' || message.type === 'completed' || message.type === 'failed' || message.type === 'cancelled') {
          // The backend sends { type, job_id, data: { ...jobStatus } }
          const jobStatus = message.data as BulkImportJobStatus;
          setActiveJobs((prev) => {
            const updated = new Map(prev);
            updated.set(jobId, jobStatus);
            return updated;
          });
          
          // Stop tracking if job is done
          if (message.type !== 'progress') {
            stopJobTracking(jobId);
            if (message.type === 'completed' || message.type === 'failed') {
              loadStatus(); // Refresh status
              loadCycleStatus(); // Refresh cycle status
            }
          }
        }
      },
      () => {
        // WebSocket failed, fallback to polling
        console.log(`WebSocket failed for job ${jobId}, falling back to polling`);
        startPolling(jobId);
      }
    );

    if (ws) {
      wsRefs.current.set(jobId, ws);
      
      // Fallback to polling if WebSocket closes unexpectedly
      ws.onclose = () => {
        if (!pollingIntervals.current.has(jobId)) {
          startPolling(jobId);
        }
      };
    } else {
      // WebSocket creation failed, use polling
      startPolling(jobId);
    }
  };

  const startPolling = (jobId: string) => {
    // Stop any existing polling for this job
    if (pollingIntervals.current.has(jobId)) {
      clearInterval(pollingIntervals.current.get(jobId)!);
    }

    const poll = async () => {
      try {
        const jobStatus = await bulkDataApi.getJobStatus(jobId);
        setActiveJobs((prev) => {
          const updated = new Map(prev);
          updated.set(jobId, jobStatus);
          return updated;
        });

        // Stop polling if job is done
        if (jobStatus.status !== 'running' && jobStatus.status !== 'pending') {
          stopJobTracking(jobId);
          if (jobStatus.status === 'completed' || jobStatus.status === 'failed') {
            loadStatus(); // Refresh status
            loadCycleStatus(); // Refresh cycle status
          }
        }
      } catch (err) {
        console.error(`Error polling job ${jobId}:`, err);
      }
    };

    // Poll immediately, then every 2 seconds
    poll();
    const interval = setInterval(poll, 2000);
    pollingIntervals.current.set(jobId, interval);
  };

  const stopJobTracking = (jobId: string) => {
    // Close WebSocket if open
    const ws = wsRefs.current.get(jobId);
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.close();
    }
    wsRefs.current.delete(jobId);

    // Clear polling interval
    const interval = pollingIntervals.current.get(jobId);
    if (interval) {
      clearInterval(interval);
      pollingIntervals.current.delete(jobId);
    }
  };

  const handleCancelJob = async (jobId: string) => {
    if (!window.confirm('Are you sure you want to cancel this job?')) {
      return;
    }

    try {
      await bulkDataApi.cancelJob(jobId);
      // Job status will be updated via WebSocket/polling
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to cancel job');
      console.error(err);
    }
  };

  const loadStatus = async () => {
    try {
      setLoading(true);
      setError(null);
      const [statusData, cyclesData] = await Promise.all([
        bulkDataApi.getStatus(),
        bulkDataApi.getCycles(),
      ]);
      setStatus(statusData);
      setCycles(cyclesData.cycles || []);
      
      // Set default cycle to most recent available or current year
      if (cyclesData.cycles && cyclesData.cycles.length > 0) {
        // First cycle should be most recent
        const firstCycle = cyclesData.cycles[0];
        const defaultCycle = firstCycle.cycle || 2024;
        if (!selectedCycle || selectedCycle === 2024) {
          setSelectedCycle(defaultCycle);
        }
      } else {
        const currentYear = new Date().getFullYear();
        const defaultCycle = (Math.floor(currentYear / 2) * 2); // Nearest even year
        if (!selectedCycle || selectedCycle === 2024) {
          setSelectedCycle(defaultCycle);
        }
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load bulk data status');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const loadCycleStatus = async () => {
    try {
      const status = await bulkDataApi.getDataTypeStatus(selectedCycle);
      setCycleStatus(status);
    } catch (err: any) {
      console.error('Failed to load cycle status:', err);
      setCycleStatus(null);
    }
  };

  const handleClearContributions = async () => {
    if (!window.confirm('Are you sure you want to clear all contributions? This action cannot be undone.')) {
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      setOperationInProgress('Clearing contributions...');
      
      const result = await bulkDataApi.clearContributions();
      setSuccess(`Successfully cleared ${result.deleted_count} contributions`);
      await loadStatus(); // Refresh status
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to clear contributions');
      console.error(err);
    } finally {
      setLoading(false);
      setOperationInProgress(null);
    }
  };

  const handleDownload = async () => {
    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      
      const result = await bulkDataApi.download(selectedCycle);
      setSuccess(result.message || `Download started for cycle ${selectedCycle}`);
      
      // Start tracking the job
      if (result.job_id) {
        startJobTracking(result.job_id);
      }
      
      await loadStatus(); // Refresh status
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to download bulk data');
      console.error(err);
    } finally {
      setLoading(false);
      setOperationInProgress(null);
    }
  };

  const handleCleanupAndReimport = async () => {
    if (!window.confirm(
      `Are you sure you want to clear all contributions and reimport cycle ${selectedCycle}? ` +
      'This will delete all existing contribution data and cannot be undone.'
    )) {
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      
      const result = await bulkDataApi.cleanupAndReimport(selectedCycle);
      setSuccess(result.message || `Cleanup and reimport started for cycle ${selectedCycle}`);
      
      // Start tracking the job
      if (result.job_id) {
        startJobTracking(result.job_id);
      }
      
      await loadStatus(); // Refresh status
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to cleanup and reimport');
      console.error(err);
    } finally {
      setLoading(false);
      setOperationInProgress(null);
    }
  };

  const handleImportSelected = async () => {
    if (selectedDataTypes.size === 0) {
      setError('Please select at least one data type to import');
      return;
    }

    const dataTypesArray = Array.from(selectedDataTypes);
    if (!window.confirm(
      `Import ${dataTypesArray.length} selected data type(s) for cycle ${selectedCycle}?`
    )) {
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      
      const result = await bulkDataApi.importMultipleDataTypes(selectedCycle, dataTypesArray);
      setSuccess(result.message || `Import started for ${dataTypesArray.length} data types`);
      
      // Start tracking the job
      if (result.job_id) {
        startJobTracking(result.job_id);
      }
      
      await loadCycleStatus(); // Refresh cycle status
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to import selected data types');
      console.error(err);
    } finally {
      setLoading(false);
      setOperationInProgress(null);
    }
  };

  const handleImportAllTypes = async () => {
    if (!window.confirm(
      `Import all implemented data types for cycle ${selectedCycle}?`
    )) {
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      
      const result = await bulkDataApi.importAllDataTypes(selectedCycle);
      setSuccess(result.message || `Import started for all data types`);
      
      // Start tracking the job
      if (result.job_id) {
        startJobTracking(result.job_id);
      }
      
      await loadCycleStatus(); // Refresh cycle status
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to import all data types');
      console.error(err);
    } finally {
      setLoading(false);
      setOperationInProgress(null);
    }
  };

  const handleImportAll = async () => {
    const cycleCount = cycles.length;
    const endYear = new Date().getFullYear() + 6;
    if (!window.confirm(
      `Are you sure you want to import all ${cycleCount} cycles from 2000 to ${endYear}? ` +
      'This will download and import data for all election cycles (including future cycles). This may take a very long time.'
    )) {
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setSuccess(null);
      
      const result = await bulkDataApi.importAll();
      setSuccess(result.message || `Import started for ${result.count} cycles`);
      
      // Start tracking the job
      if (result.job_id) {
        startJobTracking(result.job_id);
      }
      
      await loadStatus(); // Refresh status
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to import all cycles');
      console.error(err);
    } finally {
      setLoading(false);
      setOperationInProgress(null);
    }
  };

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

  if (loading && !status) {
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
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          Bulk Data Management
        </h1>
        <p className="text-gray-600">
          Manage FEC bulk CSV data downloads and imports. Use this to clean up incorrectly imported data and reimport with correct mappings.
        </p>
      </div>

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

          {/* Active Jobs */}
          {Array.from(activeJobs.values()).length > 0 && (
            <div className="mb-6">
              <h2 className="text-xl font-semibold text-gray-900 mb-4">Active Jobs</h2>
              {Array.from(activeJobs.values()).map((job) => (
                <ProgressTracker
                  key={job.job_id}
                  jobStatus={job}
                  onCancel={() => handleCancelJob(job.job_id)}
                />
              ))}
            </div>
          )}

          {operationInProgress && activeJobs.size === 0 && (
            <div className="mb-6 bg-blue-50 border border-blue-200 text-blue-800 px-4 py-3 rounded">
              <strong>In Progress:</strong> {operationInProgress}
              <div className="mt-2">
                <div className="w-full bg-blue-200 rounded-full h-2.5">
                  <div className="bg-blue-600 h-2.5 rounded-full animate-pulse" style={{ width: '100%' }}></div>
                </div>
              </div>
            </div>
          )}

      {/* Status Card */}
      <div className="bg-white shadow rounded-lg p-6 mb-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Current Status</h2>
        {status ? (
          <dl className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <div>
              <dt className="text-sm font-medium text-gray-500">Bulk Data Enabled</dt>
              <dd className="mt-1 text-sm text-gray-900">
                {status.enabled ? (
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                    Enabled
                  </span>
                ) : (
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                    Disabled
                  </span>
                )}
              </dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Total Records</dt>
              <dd className="mt-1 text-sm text-gray-900">
                {status.total_records?.toLocaleString() || 0}
              </dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Available Cycles</dt>
              <dd className="mt-1 text-sm text-gray-900">
                {status.available_cycles?.length || 0}
              </dd>
            </div>
            <div>
              <dt className="text-sm font-medium text-gray-500">Update Interval</dt>
              <dd className="mt-1 text-sm text-gray-900">
                {status.update_interval_hours || 24} hours
              </dd>
            </div>
          </dl>
        ) : (
          <p className="text-gray-500">No status available</p>
        )}
      </div>

      {/* Year Selection and Data Type Grid */}
      <div className="bg-white shadow rounded-lg p-6 mb-6">
        <div className="flex justify-between items-center mb-4">
          <div className="flex-1">
            <label htmlFor="cycle-select" className="block text-sm font-medium text-gray-700 mb-2">
              Election Cycle
            </label>
            <div className="flex gap-2">
              <select
                id="cycle-select"
                value={selectedCycle}
                onChange={(e) => {
                  setSelectedCycle(parseInt(e.target.value));
                  setSelectedDataTypes(new Set()); // Clear selections when cycle changes
                }}
                className="block w-48 rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm"
                disabled={loading}
              >
                {cycles.length > 0 ? (
                  cycles.map((cycleObj) => (
                    <option key={cycleObj.cycle} value={cycleObj.cycle}>
                      {cycleObj.cycle}
                    </option>
                  ))
                ) : (
                  <option value={2024}>2024</option>
                )}
              </select>
              <button
                onClick={loadCycleStatus}
                disabled={loading}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                Refresh Status
              </button>
            </div>
          </div>
          <div className="flex gap-2">
            <button
              onClick={handleImportSelected}
              disabled={loading || selectedDataTypes.size === 0}
              className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Import Selected ({selectedDataTypes.size})
            </button>
            <button
              onClick={handleImportAllTypes}
              disabled={loading}
              className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Import All Types
            </button>
          </div>
        </div>

        {/* Data Type Grid */}
        <DataTypeGrid
          cycleStatus={cycleStatus}
          selectedTypes={selectedDataTypes}
          onSelectionChange={handleDataTypeSelection}
          onSelectAll={handleSelectAll}
          onDeselectAll={handleDeselectAll}
          loading={loading}
        />
      </div>

      {/* Legacy Operations Card - Keep for backward compatibility */}
      <div className="bg-white shadow rounded-lg p-6 mb-6">
        <h2 className="text-xl font-semibold text-gray-900 mb-4">Legacy Operations</h2>
        
        <div className="space-y-4">
          {/* Action Buttons */}
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <button
              onClick={handleDownload}
              disabled={loading}
              className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Download & Import (Individual Contributions)
            </button>

            <button
              onClick={handleClearContributions}
              disabled={loading}
              className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-yellow-600 hover:bg-yellow-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-yellow-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Clear All Contributions
            </button>

            <button
              onClick={handleCleanupAndReimport}
              disabled={loading}
              className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Cleanup & Reimport
            </button>

            <button
              onClick={handleImportAll}
              disabled={loading}
              className="inline-flex justify-center items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md text-white bg-purple-600 hover:bg-purple-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-purple-500 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Import All Cycles
            </button>
          </div>
        </div>
      </div>

      {/* Information Card */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-blue-900 mb-2">About Cleanup & Reimport</h3>
        <div className="text-sm text-blue-800 space-y-2">
          <p>
            <strong>Clear All Contributions:</strong> Removes all contribution records from the database. 
            This is useful if data was imported with incorrect column mappings.
          </p>
          <p>
            <strong>Download & Import:</strong> Downloads and imports FEC bulk CSV data for the selected cycle 
            without clearing existing data. Duplicates will be skipped.
          </p>
          <p>
            <strong>Cleanup & Reimport:</strong> First clears all contributions, then downloads and imports 
            fresh data for the selected cycle. This ensures a clean import with correct column mappings.
          </p>
          <p>
            <strong>Import All Cycles:</strong> Downloads and imports all election cycles from 2000 to {new Date().getFullYear() + 6} (includes future cycles). 
            This will import {cycles.length} cycles and may take several hours to complete. Future cycles may not have data available yet. Duplicates will be skipped.
          </p>
          <p className="mt-4 font-semibold">
            ⚠️ Warning: Cleanup operations are irreversible. Make sure you want to delete all contribution data before proceeding.
          </p>
        </div>
      </div>
    </div>
  );
}

